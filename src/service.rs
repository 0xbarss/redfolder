use crate::calendar::CalendarClient;
use crate::config::RedFolderConfig;
use crate::engine::BlackoutEngine;
use crate::error::Result;
use crate::events::{EventListener, RedFolderEvent};
use crate::types::{BlackoutNotification, BlackoutWindow};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Lifecycle state of the background synchronization and evaluation service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceState {
    /// Service is stopped and no background tasks are running.
    Stopped,
    /// Service is currently performing initial calendar refresh and starting up.
    Starting,
    /// Background synchronization and evaluation loops are active.
    Running,
    /// Service is shutting down and awaiting background tasks to terminate.
    Stopping,
}

/// Internal state tracking for an individual registered worker or strategy.
struct WorkerState {
    config: RedFolderConfig,
    legacy_sender: Option<mpsc::UnboundedSender<BlackoutNotification>>,
    event_sender: Option<mpsc::UnboundedSender<RedFolderEvent>>,
    in_blackout: bool,
    active_window: Option<BlackoutWindow>,
    last_warned_window_start: Option<DateTime<Utc>>,
}

/// Internal mutable state protected by an async mutex.
struct ServiceInner {
    workers: HashMap<String, WorkerState>,
    engine: BlackoutEngine,
    last_fetch_date: Option<NaiveDate>,
    client: CalendarClient,
    check_interval: std::time::Duration,
    broadcast_tx: broadcast::Sender<RedFolderEvent>,
    listeners: Vec<Arc<dyn EventListener>>,
    state: ServiceState,
    notify: Arc<Notify>,
    last_sync_time: Option<DateTime<Utc>>,
    last_sync_error: Option<String>,
}

impl ServiceInner {
    fn new(
        client: CalendarClient,
        check_interval: std::time::Duration,
        broadcast_tx: broadcast::Sender<RedFolderEvent>,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            workers: HashMap::new(),
            engine: BlackoutEngine::new(),
            last_fetch_date: None,
            client,
            check_interval,
            broadcast_tx,
            listeners: Vec::new(),
            state: ServiceState::Stopped,
            notify,
            last_sync_time: None,
            last_sync_error: None,
        }
    }

    fn check_calendar_staleness(&mut self) -> bool {
        let is_stale = if let Some(last_sync) = self.last_sync_time {
            if let Some(max_age) = self.client.max_stale_cache_age() {
                if let Ok(chrono_dur) = Duration::from_std(max_age) {
                    Utc::now() - last_sync > chrono_dur
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            self.engine.is_empty()
        };
        self.engine.set_stale(is_stale);
        is_stale
    }

    /// Evaluates blackout status and warnings for all registered workers,
    /// broadcasting domain events on transitions.
    /// Returns the earliest timestamp of the next expected state transition across all workers.
    fn check_and_notify_workers(&mut self) -> Option<DateTime<Utc>> {
        self.check_calendar_staleness();

        if self.workers.is_empty() {
            return None;
        }

        let now = Utc::now();
        let mut events_to_dispatch: Vec<RedFolderEvent> = Vec::new();
        let mut next_transition: Option<DateTime<Utc>> = None;

        let update_next = |current: &mut Option<DateTime<Utc>>, candidate: DateTime<Utc>| {
            if candidate > now {
                match current {
                    Some(ref mut t) if candidate < *t => *t = candidate,
                    None => *current = Some(candidate),
                    _ => {}
                }
            }
        };

        // 1. Check blackout active transitions and upcoming warnings per worker
        for (worker_id, ws) in self.workers.iter_mut() {
            if !ws.config.enabled {
                continue;
            }

            let is_active = self.engine.is_blackout(&ws.config);

            // State transition: Enter or Exit
            if is_active != ws.in_blackout {
                ws.in_blackout = is_active;
                let window = if is_active {
                    self.engine.current_window(&ws.config)
                } else {
                    None
                };

                // Legacy notification
                if let Some(ref sender) = ws.legacy_sender {
                    sender
                        .send(BlackoutNotification {
                            active: is_active,
                            window: window.clone(),
                        })
                        .ok();
                }

                if is_active {
                    ws.active_window = window.clone();
                    if let Some(w) = window {
                        let end_str = w.end.format("%H:%M UTC").to_string();
                        warn!(worker=%worker_id, until=%end_str, "ENTERING news blackout window");
                        let ev = RedFolderEvent::BlackoutStarted {
                            window: w,
                            worker_id: Some(worker_id.clone()),
                        };
                        if let Some(ref sender) = ws.event_sender {
                            sender.send(ev.clone()).ok();
                        }
                        events_to_dispatch.push(ev);
                    }
                } else {
                    info!(worker=%worker_id, "EXITING news blackout window");
                    // Send the actual window that concluded (or fallback dummy if none tracked)
                    let ended_window = ws.active_window.take().unwrap_or_else(|| BlackoutWindow {
                        start: now,
                        end: now,
                        events: vec![],
                    });
                    let ev = RedFolderEvent::BlackoutEnded {
                        window: ended_window,
                        worker_id: Some(worker_id.clone()),
                    };
                    if let Some(ref sender) = ws.event_sender {
                        sender.send(ev.clone()).ok();
                    }
                    events_to_dispatch.push(ev);
                }
            }

            // Track next transition for this worker
            if ws.in_blackout {
                if let Some(ref w) = ws.active_window {
                    update_next(&mut next_transition, w.end);
                }
            } else {
                let upcoming = self.engine.upcoming_blackouts(&ws.config, 24);
                if let Some(next_window) = upcoming.first() {
                    update_next(&mut next_transition, next_window.start);

                    if let Some(warn_min) = ws.config.warning_before_min {
                        let warn_time = next_window.start - Duration::minutes(warn_min);
                        update_next(&mut next_transition, warn_time);

                        let mins_until_start = (next_window.start - now).num_minutes();
                        if mins_until_start > 0 && mins_until_start <= warn_min {
                            let already_warned = ws
                                .last_warned_window_start
                                .map(|t| t == next_window.start)
                                .unwrap_or(false);

                            if !already_warned {
                                ws.last_warned_window_start = Some(next_window.start);
                                let ev = RedFolderEvent::BlackoutWarning {
                                    window: next_window.clone(),
                                    minutes_until_start: mins_until_start,
                                    worker_id: Some(worker_id.clone()),
                                };
                                if let Some(ref sender) = ws.event_sender {
                                    sender.send(ev.clone()).ok();
                                }
                                events_to_dispatch.push(ev);
                            }
                        }
                    }
                }
            }
        }

        // 2. Dispatch events to global broadcast bus and registered event listeners (with timeout guard)
        for ev in events_to_dispatch {
            self.dispatch_event(ev);
        }

        next_transition
    }

    /// Dispatches an event to the global broadcast bus and registered event listeners.
    fn dispatch_event(&self, event: RedFolderEvent) {
        self.broadcast_tx.send(event.clone()).ok();
        for listener in &self.listeners {
            let listener_clone = listener.clone();
            let ev_clone = event.clone();
            tokio::spawn(async move {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    listener_clone.on_event(&ev_clone),
                )
                .await;
            });
        }
    }
}

/// Async service that orchestrates daily economic calendar synchronization and event-driven blackout alerts.
///
/// Features:
/// - Event-driven architecture with broadcast channels (`subscribe()`) and listener callbacks.
/// - Early warning notifications before blackout periods commence.
/// - Background daily refresh at midnight UTC with automatic disk fallback.
/// - Transition-driven background evaluation loop dispatching alerts at exact start and end timestamps.
/// - Multi-worker independent registration with dedicated typed streams.
/// - Deterministic lifecycle management with `CancellationToken` and tracked join handles.
pub struct RedFolderService {
    inner: Arc<Mutex<ServiceInner>>,
    refresh_lock: Arc<Mutex<()>>,
    broadcast_tx: broadcast::Sender<RedFolderEvent>,
    cancel_token: Arc<Mutex<Option<CancellationToken>>>,
    task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    notify: Arc<Notify>,
}

/// Minimum allowable check interval for periodic evaluation to prevent CPU busy loops.
pub const MIN_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

impl RedFolderService {
    /// Create a new `RedFolderService` with an optional cache directory.
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        Self::with_client(CalendarClient::new(cache_dir))
    }

    /// Create with an existing `CalendarClient`.
    pub fn with_client(client: CalendarClient) -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        let notify = Arc::new(Notify::new());
        Self {
            inner: Arc::new(Mutex::new(ServiceInner::new(
                client,
                std::time::Duration::from_secs(15),
                broadcast_tx.clone(),
                notify.clone(),
            ))),
            refresh_lock: Arc::new(Mutex::new(())),
            broadcast_tx,
            cancel_token: Arc::new(Mutex::new(None)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
            notify,
        }
    }

    /// Current lifecycle state of the service.
    pub async fn state(&self) -> ServiceState {
        self.inner.lock().await.state
    }

    /// Subscribe to the global broadcast event bus.
    ///
    /// Any system component (e.g. risk manager, Telegram bot, MT5 bridge)
    /// can receive a cloned stream of all `RedFolderEvent`s.
    pub fn subscribe(&self) -> broadcast::Receiver<RedFolderEvent> {
        self.broadcast_tx.subscribe()
    }

    /// Add an asynchronous event listener implementing `EventListener`.
    pub async fn add_listener(&self, listener: Arc<dyn EventListener>) {
        self.inner.lock().await.listeners.push(listener);
    }

    /// Configure the periodic evaluation loop frequency or watchdog interval.
    ///
    /// Returns an error if the requested interval is zero or less than [`MIN_CHECK_INTERVAL`] (100ms)
    /// to prevent busy evaluation loops and excessive CPU consumption.
    pub async fn set_check_interval(&self, interval: std::time::Duration) -> Result<()> {
        if interval < MIN_CHECK_INTERVAL {
            return Err(crate::error::RedFolderError::Config(format!(
                "check interval must be at least {:?} (got {:?})",
                MIN_CHECK_INTERVAL, interval
            )));
        }
        self.inner.lock().await.check_interval = interval;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Manually trigger blackout evaluation and worker notifications immediately.
    pub async fn evaluate_and_notify(&self) {
        self.inner.lock().await.check_and_notify_workers();
    }

    /// Set an explicit `BlackoutEngine` instance for testing or simulation.
    pub async fn set_engine(&self, engine: BlackoutEngine) {
        self.inner.lock().await.engine = engine;
        self.notify.notify_waiters();
    }

    /// Register a worker or trading strategy, returning a legacy `BlackoutNotification` receiver.
    /// Immediately notifies if worker is currently in blackout.
    ///
    /// Validates `config` before registering. If a worker with `worker_id` is already registered,
    /// returns an error to prevent silent disconnection of existing receivers.
    /// To deliberately update or replace a worker, use [`reregister_worker`](Self::reregister_worker).
    pub async fn register_worker(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<BlackoutNotification>> {
        config.validate()?;
        let worker_id = worker_id.into();

        let mut inner = self.inner.lock().await;
        if inner.workers.contains_key(&worker_id) {
            return Err(crate::error::RedFolderError::Service(format!(
                "worker '{worker_id}' is already registered; use reregister_worker to update or unregister first"
            )));
        }

        let (legacy_tx, legacy_rx) = mpsc::unbounded_channel();
        let is_active = inner.engine.is_blackout(&config);
        let active_window = if is_active {
            inner.engine.current_window(&config)
        } else {
            None
        };

        if is_active {
            legacy_tx
                .send(BlackoutNotification {
                    active: true,
                    window: active_window.clone(),
                })
                .ok();
        }

        inner.workers.insert(
            worker_id.clone(),
            WorkerState {
                config,
                legacy_sender: Some(legacy_tx),
                event_sender: None,
                in_blackout: is_active,
                active_window,
                last_warned_window_start: None,
            },
        );

        drop(inner);
        self.notify.notify_waiters();

        debug!(worker=%worker_id, "registered worker in RedFolderService");
        Ok(legacy_rx)
    }

    /// Register a worker or trading strategy, returning an event-driven `RedFolderEvent` receiver.
    /// Immediately notifies if worker is currently in blackout.
    ///
    /// Validates `config` before registering. If a worker with `worker_id` is already registered,
    /// returns an error to prevent silent disconnection of existing receivers.
    /// To deliberately update or replace a worker, use [`reregister_worker_events`](Self::reregister_worker_events).
    pub async fn register_worker_events(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<RedFolderEvent>> {
        config.validate()?;
        let worker_id = worker_id.into();

        let mut inner = self.inner.lock().await;
        if inner.workers.contains_key(&worker_id) {
            return Err(crate::error::RedFolderError::Service(format!(
                "worker '{worker_id}' is already registered; use reregister_worker_events to update or unregister first"
            )));
        }

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let is_active = inner.engine.is_blackout(&config);
        let active_window = if is_active {
            inner.engine.current_window(&config)
        } else {
            None
        };

        if is_active {
            if let Some(ref w) = active_window {
                event_tx
                    .send(RedFolderEvent::BlackoutStarted {
                        window: w.clone(),
                        worker_id: Some(worker_id.clone()),
                    })
                    .ok();
            }
        }

        inner.workers.insert(
            worker_id.clone(),
            WorkerState {
                config,
                legacy_sender: None,
                event_sender: Some(event_tx),
                in_blackout: is_active,
                active_window,
                last_warned_window_start: None,
            },
        );

        drop(inner);
        self.notify.notify_waiters();

        debug!(worker=%worker_id, "registered event worker in RedFolderService");
        Ok(event_rx)
    }

    /// Re-registers an existing worker or registers a new worker, updating its configuration
    /// and replacing its event channels. Emits a warning if an existing worker is overwritten.
    pub async fn reregister_worker(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<BlackoutNotification>> {
        config.validate()?;
        let worker_id = worker_id.into();

        let mut inner = self.inner.lock().await;
        if inner.workers.contains_key(&worker_id) {
            warn!(worker=%worker_id, "re-registering worker: overwriting previous worker state and channels");
        }

        let (legacy_tx, legacy_rx) = mpsc::unbounded_channel();
        let is_active = inner.engine.is_blackout(&config);
        let active_window = if is_active {
            inner.engine.current_window(&config)
        } else {
            None
        };

        if is_active {
            legacy_tx
                .send(BlackoutNotification {
                    active: true,
                    window: active_window.clone(),
                })
                .ok();
        }

        inner.workers.insert(
            worker_id.clone(),
            WorkerState {
                config,
                legacy_sender: Some(legacy_tx),
                event_sender: None,
                in_blackout: is_active,
                active_window,
                last_warned_window_start: None,
            },
        );

        drop(inner);
        self.notify.notify_waiters();

        debug!(worker=%worker_id, "re-registered worker in RedFolderService");
        Ok(legacy_rx)
    }

    /// Re-registers an existing event worker or registers a new worker, updating its configuration
    /// and replacing its event channels. Emits a warning if an existing worker is overwritten.
    pub async fn reregister_worker_events(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<RedFolderEvent>> {
        config.validate()?;
        let worker_id = worker_id.into();

        let mut inner = self.inner.lock().await;
        if inner.workers.contains_key(&worker_id) {
            warn!(worker=%worker_id, "re-registering worker: overwriting previous worker state and channels");
        }

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let is_active = inner.engine.is_blackout(&config);
        let active_window = if is_active {
            inner.engine.current_window(&config)
        } else {
            None
        };

        if is_active {
            if let Some(ref w) = active_window {
                event_tx
                    .send(RedFolderEvent::BlackoutStarted {
                        window: w.clone(),
                        worker_id: Some(worker_id.clone()),
                    })
                    .ok();
            }
        }

        inner.workers.insert(
            worker_id.clone(),
            WorkerState {
                config,
                legacy_sender: None,
                event_sender: Some(event_tx),
                in_blackout: is_active,
                active_window,
                last_warned_window_start: None,
            },
        );

        drop(inner);
        self.notify.notify_waiters();

        debug!(worker=%worker_id, "re-registered event worker in RedFolderService");
        Ok(event_rx)
    }

    /// Unregister a worker by ID.
    pub async fn unregister_worker(&self, worker_id: &str) {
        self.inner.lock().await.workers.remove(worker_id);
        self.notify.notify_waiters();
    }

    /// Returns the active and upcoming blackout windows derived specifically for the given worker.
    pub async fn windows_for_worker(&self, worker_id: &str) -> Vec<BlackoutWindow> {
        let inner = self.inner.lock().await;
        if let Some(w) = inner.workers.get(worker_id) {
            inner.engine.windows_for_config(&w.config, Utc::now())
        } else {
            Vec::new()
        }
    }

    /// Whether the background worker tasks are currently running.
    pub async fn is_running(&self) -> bool {
        self.inner.lock().await.state == ServiceState::Running
    }

    /// Start the background synchronization and evaluation loops.
    pub async fn start(&self) -> Result<()> {
        {
            let mut inner = self.inner.lock().await;
            if inner.state == ServiceState::Running || inner.state == ServiceState::Starting {
                return Err(crate::error::RedFolderError::Service(
                    "service is already running".to_string(),
                ));
            }
            if inner.workers.is_empty() {
                warn!("no workers registered — RedFolderService not starting");
                return Ok(());
            }
            if !inner.workers.values().any(|w| w.config.enabled) {
                info!("all registered worker blackout configs are disabled");
                return Ok(());
            }
            inner.state = ServiceState::Starting;
        }

        // Perform initial calendar synchronization fallibly
        if let Err(e) = Self::fetch_and_compile(&self.inner, &self.refresh_lock, false, false).await
        {
            let mut inner = self.inner.lock().await;
            inner.state = ServiceState::Stopped;
            return Err(e);
        }

        let cancel_token = CancellationToken::new();
        *self.cancel_token.lock().await = Some(cancel_token.clone());

        let mut handles = Vec::new();

        // 1. Daily midnight fetch loop (with short-interval retry on failure)
        {
            let inner_arc = self.inner.clone();
            let refresh_lock_arc = self.refresh_lock.clone();
            let token = cancel_token.child_token();
            let handle = tokio::spawn(async move {
                loop {
                    let now = Utc::now();
                    let next_midnight = (now + Duration::days(1))
                        .date_naive()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_utc();
                    let sleep_secs = (next_midnight - now).num_seconds().max(1) as u64;

                    tokio::select! {
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(sleep_secs)) => {
                            let res = tokio::select! {
                                res = Self::fetch_and_compile(&inner_arc, &refresh_lock_arc, false, true) => res,
                                _ = token.cancelled() => {
                                    break;
                                }
                            };

                            if let Err(ref e) = res {
                                warn!(err = %e, "daily midnight calendar sync failed; will retry every 15 minutes");
                                loop {
                                    tokio::select! {
                                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(900)) => {
                                            let retry_res = Self::fetch_and_compile(&inner_arc, &refresh_lock_arc, false, true).await;
                                            if retry_res.is_ok() {
                                                info!("calendar sync successfully recovered after retry");
                                                break;
                                            } else if let Err(err) = retry_res {
                                                warn!(err = %err, "calendar sync retry failed; will retry in 15 minutes");
                                            }
                                        }
                                        _ = token.cancelled() => {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        _ = token.cancelled() => {
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // 2. Transition-driven blackout evaluation loop (with watchdog and interruptible notify)
        {
            let inner_arc = self.inner.clone();
            let token = cancel_token.child_token();
            let notify = self.notify.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let (next_transition, check_interval) = {
                        let mut inner = inner_arc.lock().await;
                        (inner.check_and_notify_workers(), inner.check_interval)
                    };

                    let now = Utc::now();
                    let sleep_duration = if let Some(target) = next_transition {
                        let dur = (target - now).to_std().unwrap_or(std::time::Duration::ZERO);
                        dur.min(check_interval)
                    } else {
                        check_interval
                    };

                    tokio::select! {
                        _ = tokio::time::sleep(sleep_duration) => {}
                        _ = notify.notified() => {}
                        _ = token.cancelled() => {
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }

        *self.task_handles.lock().await = handles;

        {
            let mut inner = self.inner.lock().await;
            inner.state = ServiceState::Running;
        }

        info!("RedFolderService background tasks started successfully");
        Ok(())
    }

    /// Stop all background tasks gracefully and await their termination.
    pub async fn stop(&self) {
        {
            let mut inner = self.inner.lock().await;
            if inner.state == ServiceState::Stopped || inner.state == ServiceState::Stopping {
                return;
            }
            inner.state = ServiceState::Stopping;
        }

        info!("stopping RedFolderService");

        if let Some(token) = self.cancel_token.lock().await.take() {
            token.cancel();
        }
        self.notify.notify_waiters();

        let mut handles: Vec<_> = self.task_handles.lock().await.drain(..).collect();
        for handle in &mut handles {
            let res = tokio::time::timeout(std::time::Duration::from_secs(3), &mut *handle).await;
            if res.is_err() {
                handle.abort();
                let _ = handle.await;
            }
        }

        {
            let mut inner = self.inner.lock().await;
            inner.state = ServiceState::Stopped;
        }
    }

    /// Manually trigger a calendar download and recompile blackout windows.
    pub async fn refresh(&self) -> Result<()> {
        Self::fetch_and_compile(&self.inner, &self.refresh_lock, false, false).await
    }

    /// Force a fresh calendar fetch from the remote API, bypassing cache.
    pub async fn force_refresh(&self) -> Result<()> {
        Self::fetch_and_compile(&self.inner, &self.refresh_lock, true, false).await
    }

    /// Internal helper that fetches events and compiles windows without holding mutex across network I/O.
    async fn fetch_and_compile(
        inner: &Arc<Mutex<ServiceInner>>,
        refresh_lock: &Arc<Mutex<()>>,
        force_remote: bool,
        skip_if_already_fetched_today: bool,
    ) -> Result<()> {
        let _refresh_guard = refresh_lock.lock().await;
        let today = Utc::now().date_naive();

        // 1. Check if already fetched today under lock (if requested by scheduled loop)
        if skip_if_already_fetched_today {
            let state = inner.lock().await;
            if state.last_fetch_date == Some(today) && !state.engine.is_empty() {
                debug!("calendar already fetched and compiled for today");
                return Ok(());
            }
        }

        // 2. Perform HTTP request outside lock
        let (client, client_tz) = {
            let state = inner.lock().await;
            (state.client.clone(), state.client.calendar_timezone())
        };
        let fetch_res = if force_remote {
            client.force_fetch().await
        } else {
            client.fetch_or_cached().await
        };

        let raw = match fetch_res {
            Ok(raw) => raw,
            Err(e) => {
                let err_str = e.to_string();
                let mut state = inner.lock().await;
                state.last_sync_error = Some(err_str.clone());
                state.check_calendar_staleness();
                state.check_and_notify_workers();
                state.notify.notify_waiters();
                state.dispatch_event(RedFolderEvent::CalendarSyncFailed { error: err_str });
                return Err(e);
            }
        };

        // 3. Re-acquire lock to compile windows into engine
        let mut state = inner.lock().await;
        let configs: Vec<RedFolderConfig> = state
            .workers
            .values()
            .filter(|w| w.config.enabled)
            .map(|w| w.config.clone())
            .collect();
        let config_refs: Vec<&RedFolderConfig> = configs.iter().collect();

        state.engine = BlackoutEngine::compile_with_tz(&raw, &config_refs, Utc::now(), client_tz);
        state.last_fetch_date = Some(today);
        state.last_sync_time = Some(Utc::now());
        state.last_sync_error = None;

        // Immediately reconcile worker states with newly compiled engine
        state.check_and_notify_workers();
        state.notify.notify_waiters();

        let total_windows = state.engine.windows().len();
        info!(windows=%total_windows, "refreshed economic calendar windows");

        // Centralized dispatch to both broadcast channel and EventListeners
        state.dispatch_event(RedFolderEvent::CalendarUpdated {
            total_events: raw.len(),
            total_windows,
        });

        Ok(())
    }

    /// Returns the timestamp of the last successful calendar synchronization, if any.
    pub async fn last_sync_time(&self) -> Option<DateTime<Utc>> {
        self.inner.lock().await.last_sync_time
    }

    /// Returns the error message from the most recent failed calendar synchronization, if any.
    pub async fn last_sync_error(&self) -> Option<String> {
        self.inner.lock().await.last_sync_error.clone()
    }

    /// Checks if calendar data is considered stale based on configured maximum stale age.
    pub async fn is_calendar_stale(&self) -> bool {
        let mut inner = self.inner.lock().await;
        inner.check_calendar_staleness()
    }

    /// Check if a specific worker is currently in blackout.
    pub async fn is_blackout(&self, worker_id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        inner.check_calendar_staleness();
        inner
            .workers
            .get(worker_id)
            .map(|w| inner.engine.is_blackout(&w.config))
            .unwrap_or(false)
    }

    /// Get current active window for a worker, if any.
    pub async fn current_window(&self, worker_id: &str) -> Option<BlackoutWindow> {
        let mut inner = self.inner.lock().await;
        inner.check_calendar_staleness();
        let worker = inner.workers.get(worker_id)?;
        inner.engine.current_window(&worker.config)
    }

    /// Return upcoming blackout windows for a specific worker within `hours` hours.
    pub async fn get_upcoming_blackouts(&self, worker_id: &str, hours: u32) -> Vec<BlackoutWindow> {
        let inner = self.inner.lock().await;
        let Some(worker) = inner.workers.get(worker_id) else {
            return Vec::new();
        };
        inner.engine.upcoming_blackouts(&worker.config, hours)
    }
}

/// Backwards compatibility alias for `RedFolderService`.
pub type NewsBlackoutService = RedFolderService;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_bus_and_warning_dispatch() {
        let service = RedFolderService::new(None);
        let mut broadcast_rx = service.subscribe();

        let config = RedFolderConfig::builder()
            .currencies(vec!["USD"])
            .impacts(vec!["High"])
            .buffer_minutes(0, 15)
            .warning_minutes(10)
            .build();

        let mut worker_events = service
            .register_worker_events("worker_usd", config)
            .await
            .unwrap();

        let now = Utc::now();
        // Event starts in 5 minutes (triggering the 10-minute warning)
        let raw = vec![crate::calendar::RawCalendarEvent {
            title: "US CPI Release".into(),
            country: "USD".into(),
            date: (now + Duration::minutes(5)).to_rfc3339(),
            time: "".into(),
            impact: "High".into(),
        }];

        {
            let mut inner = service.inner.lock().await;
            let cfg = RedFolderConfig {
                weekend_enabled: false,
                before_min: 0,
                after_min: 15,
                ..Default::default()
            };
            inner.engine = BlackoutEngine::compile(&raw, &[&cfg], now);
            inner.check_and_notify_workers();
        }

        // Verify worker event channel received BlackoutWarning
        let ev = worker_events
            .try_recv()
            .expect("should receive warning event");
        assert!(ev.is_warning());
        assert_eq!(ev.worker_id(), Some("worker_usd"));

        // Verify global broadcast bus received warning
        let bus_ev = broadcast_rx
            .try_recv()
            .expect("broadcast should receive warning");
        assert!(bus_ev.is_warning());
    }

    #[tokio::test]
    async fn test_set_check_interval_validation() {
        let service = RedFolderService::new(None);

        // Zero duration must be rejected
        let res_zero = service.set_check_interval(std::time::Duration::ZERO).await;
        assert!(res_zero.is_err(), "zero interval must be rejected");
        assert!(res_zero
            .unwrap_err()
            .to_string()
            .contains("must be at least"));

        // Sub-100ms interval must be rejected
        let res_small = service
            .set_check_interval(std::time::Duration::from_millis(50))
            .await;
        assert!(res_small.is_err(), "sub-100ms interval must be rejected");

        // Sensible duration must be accepted
        let res_valid = service
            .set_check_interval(std::time::Duration::from_secs(5))
            .await;
        assert!(res_valid.is_ok(), "valid interval must be accepted");
    }

    #[tokio::test]
    async fn test_register_worker_rejects_unvalidated_config() {
        let service = RedFolderService::new(None);

        // Directly construct invalid config with negative buffers (bypassing builder)
        let invalid_cfg = RedFolderConfig {
            before_min: -10,
            ..Default::default()
        };

        let res_legacy = service
            .register_worker("bad_worker_legacy", invalid_cfg.clone())
            .await;
        assert!(
            res_legacy.is_err(),
            "register_worker must reject unvalidated config with negative buffer"
        );

        let res_events = service
            .register_worker_events("bad_worker_events", invalid_cfg)
            .await;
        assert!(
            res_events.is_err(),
            "register_worker_events must reject unvalidated config with negative buffer"
        );
    }

    #[tokio::test]
    async fn test_duplicate_worker_registration_rejected() {
        let service = RedFolderService::new(None);
        let cfg = RedFolderConfig::default();

        let rx1 = service.register_worker("dup_worker", cfg.clone()).await;
        assert!(rx1.is_ok());

        // Duplicate registration must fail to protect existing channels
        let rx2 = service.register_worker("dup_worker", cfg.clone()).await;
        assert!(rx2.is_err());
        assert!(rx2.unwrap_err().to_string().contains("already registered"));

        // Explicit reregistration must succeed
        let rereg_rx = service.reregister_worker("dup_worker", cfg).await;
        assert!(rereg_rx.is_ok());
    }

    #[tokio::test]
    async fn test_calendar_sync_failed_event_dispatch() {
        // Construct client pointing to unreachable endpoint
        let unreachable_client = CalendarClient::with_options(
            reqwest::Client::new(),
            "http://127.0.0.1:9/unreachable",
            None,
            std::time::Duration::from_millis(50),
        );
        let service = RedFolderService::with_client(unreachable_client);
        let mut sub = service.subscribe();

        let cfg = RedFolderConfig::default();
        let _rx = service.register_worker("test_bot", cfg).await.unwrap();

        // Refresh will fail due to unreachable host
        let refresh_res = service.refresh().await;
        assert!(refresh_res.is_err());

        // Subscriber must receive CalendarSyncFailed event
        let ev = sub
            .try_recv()
            .expect("must receive CalendarSyncFailed event");
        assert!(ev.is_sync_failed());

        // Service health check should report error
        let err = service.last_sync_error().await;
        assert!(err.is_some());
    }

    #[tokio::test]
    async fn test_fail_closed_worker_in_service() {
        let service = RedFolderService::new(None);

        let open_cfg = RedFolderConfig::builder()
            .weekend_curfew(false, "20:00", "21:00", "short")
            .fail_safe_mode(crate::types::FailSafeMode::FailOpen)
            .build();

        let closed_cfg = RedFolderConfig::builder()
            .weekend_curfew(false, "20:00", "21:00", "short")
            .fail_safe_mode(crate::types::FailSafeMode::FailClosed)
            .build();

        let _open_rx = service
            .register_worker("open_worker", open_cfg)
            .await
            .unwrap();
        let mut closed_rx = service
            .register_worker("closed_worker", closed_cfg)
            .await
            .unwrap();

        // 1. Without calendar data, open worker is NOT in blackout, closed worker IS in blackout
        assert!(!service.is_blackout("open_worker").await);
        assert!(service.is_blackout("closed_worker").await);

        // 2. Closed worker gets active window
        let win = service
            .current_window("closed_worker")
            .await
            .expect("fail-closed worker must have active window");
        assert!(win.summary_title().contains("Fail-Closed Safety Blackout"));

        // 3. Closed worker channel received immediate active notification
        let notification = closed_rx
            .try_recv()
            .expect("must receive initial notification");
        assert!(notification.active);
        assert!(notification.window.is_some());
    }
}
