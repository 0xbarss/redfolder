use crate::calendar::{CalendarClient, IngestStats, SnapshotSource};
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
use tokio::sync::{broadcast, mpsc, Mutex, Notify, RwLock};
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

/// The outcome of an economic calendar refresh attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshOutcome {
    /// Fresh data (remote or valid TTL cache). Next attempt: next midnight.
    Fresh,
    /// Usable but degraded (fallback / empty-feed cache). Keep retrying.
    Degraded,
}

impl RefreshOutcome {
    /// Whether this outcome represents a freshly synchronized calendar.
    #[must_use]
    pub fn is_fresh(self) -> bool {
        matches!(self, Self::Fresh)
    }

    /// Whether this outcome represents a degraded calendar served from fallback cache.
    #[must_use]
    pub fn is_degraded(self) -> bool {
        matches!(self, Self::Degraded)
    }
}

/// Default maximum acceptable age for calendar data (36 hours).
pub const DEFAULT_MAX_DATA_AGE: std::time::Duration = std::time::Duration::from_secs(36 * 3600);

/// Default maximum ratio of unparseable events allowed before snapshot rejection (5%).
pub const DEFAULT_MAX_UNPARSEABLE_RATIO: f64 = 0.05;

/// Internal state tracking for an individual registered worker or strategy.
struct WorkerState {
    config: RedFolderConfig,
    legacy_sender: Option<mpsc::UnboundedSender<BlackoutNotification>>,
    event_sender: Option<mpsc::UnboundedSender<RedFolderEvent>>,
    in_blackout: bool,
    active_window: Option<BlackoutWindow>,
    last_warned_window_start: Option<DateTime<Utc>>,
}

/// Internal synchronization and client metadata protected by an async Mutex.
struct ServiceSyncMeta {
    client: CalendarClient,
    check_interval: std::time::Duration,
    state: ServiceState,
    last_fetch_date: Option<NaiveDate>,
    last_sync_time: Option<DateTime<Utc>>,
    last_remote_success: Option<DateTime<Utc>>,
    data_source: Option<SnapshotSource>,
    ingest: Option<IngestStats>,
    last_sync_error: Option<String>,
    max_data_age: std::time::Duration,
    max_unparseable_ratio: f64,
}

/// Async service that orchestrates daily economic calendar synchronization and event-driven blackout alerts.
///
/// Features:
/// - Granular split locking model (`RwLock` for `engine`, `workers`, and `listeners`; `Mutex` for sync metadata)
///   to ensure high-throughput concurrent evaluation and status queries without lock contention across workers.
/// - Event-driven architecture with broadcast channels (`subscribe()`) and concurrent listener callbacks.
/// - Early warning notifications before blackout periods commence.
/// - Background daily refresh at midnight UTC with automatic disk fallback.
/// - Transition-driven background evaluation loop dispatching alerts at exact start and end timestamps.
/// - Multi-worker independent registration with dedicated typed streams.
/// - Deterministic lifecycle management with `CancellationToken` and tracked join handles.
#[derive(Clone)]
pub struct RedFolderService {
    engine: Arc<RwLock<BlackoutEngine>>,
    workers: Arc<RwLock<HashMap<String, WorkerState>>>,
    listeners: Arc<RwLock<Vec<Arc<dyn EventListener>>>>,
    sync_meta: Arc<Mutex<ServiceSyncMeta>>,
    refresh_lock: Arc<Mutex<()>>,
    broadcast_tx: broadcast::Sender<RedFolderEvent>,
    cancel_token: Arc<Mutex<Option<CancellationToken>>>,
    task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    notify: Arc<Notify>,
}

/// Minimum allowable check interval for periodic evaluation to prevent CPU busy loops.
pub const MIN_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Single retry ladder delays in seconds for degraded or failed refreshes (1m -> 2m -> 5m -> 15m).
const RETRY_LADDER: [u64; 4] = [60, 120, 300, 900];

/// Calculates duration until next UTC midnight.
fn until_next_midnight() -> std::time::Duration {
    let now = Utc::now();
    let next = (now + Duration::days(1))
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    (next - now)
        .to_std()
        .unwrap_or(std::time::Duration::from_secs(1))
        .max(std::time::Duration::from_secs(1))
}

impl RedFolderService {
    /// Create a new `RedFolderService` with an optional cache directory.
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        Self::with_client(CalendarClient::new(cache_dir))
    }

    /// Create with an existing `CalendarClient`.
    pub fn with_client(client: CalendarClient) -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        let notify = Arc::new(Notify::new());
        let max_data_age = client.max_stale_cache_age().unwrap_or(DEFAULT_MAX_DATA_AGE);
        Self {
            engine: Arc::new(RwLock::new(BlackoutEngine::new())),
            workers: Arc::new(RwLock::new(HashMap::new())),
            listeners: Arc::new(RwLock::new(Vec::new())),
            sync_meta: Arc::new(Mutex::new(ServiceSyncMeta {
                client,
                check_interval: std::time::Duration::from_secs(15),
                state: ServiceState::Stopped,
                last_fetch_date: None,
                last_sync_time: None,
                last_remote_success: None,
                data_source: None,
                ingest: None,
                last_sync_error: None,
                max_data_age,
                max_unparseable_ratio: DEFAULT_MAX_UNPARSEABLE_RATIO,
            })),
            refresh_lock: Arc::new(Mutex::new(())),
            broadcast_tx,
            cancel_token: Arc::new(Mutex::new(None)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
            notify,
        }
    }

    /// Current lifecycle state of the service.
    pub async fn state(&self) -> ServiceState {
        self.sync_meta.lock().await.state
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
        self.listeners.write().await.push(listener);
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
        self.sync_meta.lock().await.check_interval = interval;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Manually trigger blackout evaluation and worker notifications immediately.
    pub async fn evaluate_and_notify(&self) {
        self.check_and_notify_workers().await;
    }

    /// Set an explicit `BlackoutEngine` instance for testing or simulation.
    pub async fn set_engine(&self, engine: BlackoutEngine) {
        *self.engine.write().await = engine;
        self.notify.notify_waiters();
    }

    /// Checks if calendar data is considered stale based on configured maximum data age.
    pub async fn is_calendar_stale(&self) -> bool {
        self.check_calendar_staleness().await
    }

    /// Pure read-lock staleness check without mutating the engine lock.
    pub async fn check_calendar_staleness(&self) -> bool {
        let engine = self.engine.read().await;
        engine.is_empty() || engine.is_stale_at(Utc::now())
    }

    /// Returns the data source provenance for the currently loaded calendar snapshot, if any.
    pub async fn data_source(&self) -> Option<SnapshotSource> {
        self.sync_meta.lock().await.data_source
    }

    /// Returns the ingestion statistics for the currently loaded calendar snapshot, if any.
    pub async fn ingest_stats(&self) -> Option<IngestStats> {
        self.sync_meta.lock().await.ingest.clone()
    }

    /// Returns the timestamp of the last successful remote download, if any.
    pub async fn last_remote_success(&self) -> Option<DateTime<Utc>> {
        self.sync_meta.lock().await.last_remote_success
    }

    /// Returns the configured maximum acceptable data age.
    pub async fn max_data_age(&self) -> std::time::Duration {
        self.sync_meta.lock().await.max_data_age
    }

    /// Set the maximum acceptable age before calendar data is considered stale.
    ///
    /// Must be at least 60 seconds.
    pub async fn set_max_data_age(&self, age: std::time::Duration) -> Result<()> {
        if age < std::time::Duration::from_secs(60) {
            return Err(crate::error::RedFolderError::Config(
                "max_data_age must be at least 60s".to_string(),
            ));
        }
        self.sync_meta.lock().await.max_data_age = age;
        if let Ok(chrono_age) = Duration::from_std(age) {
            self.engine.write().await.set_max_data_age(chrono_age);
        }
        self.notify.notify_waiters();
        Ok(())
    }

    /// Returns the configured maximum ratio of unparseable events allowed (0.0 ..= 1.0).
    pub async fn max_unparseable_ratio(&self) -> f64 {
        self.sync_meta.lock().await.max_unparseable_ratio
    }

    /// Set the maximum ratio of unparseable events allowed before a calendar snapshot is rejected.
    ///
    /// Must be within 0.0..=1.0.
    pub async fn set_max_unparseable_ratio(&self, ratio: f64) -> Result<()> {
        if !(0.0..=1.0).contains(&ratio) {
            return Err(crate::error::RedFolderError::Config(
                "max_unparseable_ratio must be within 0.0..=1.0".to_string(),
            ));
        }
        self.sync_meta.lock().await.max_unparseable_ratio = ratio;
        Ok(())
    }

    /// Evaluates blackout status and warnings for all registered workers,
    /// broadcasting domain events on transitions.
    /// Returns the earliest timestamp of the next expected state transition across all workers.
    pub async fn check_and_notify_workers(&self) -> Option<DateTime<Utc>> {
        self.check_calendar_staleness().await;

        let mut workers = self.workers.write().await;
        if workers.is_empty() {
            return None;
        }

        let engine = self.engine.read().await;
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
        for (worker_id, ws) in workers.iter_mut() {
            if !ws.config.enabled {
                continue;
            }

            let is_active = engine.is_blackout(&ws.config);

            // State transition: Enter or Exit
            if is_active != ws.in_blackout {
                ws.in_blackout = is_active;
                let window = if is_active {
                    engine.current_window(&ws.config)
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
                let upcoming = engine.upcoming_blackouts(&ws.config, 24);
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

        drop(workers);
        drop(engine);

        // 2. Dispatch events to global broadcast bus and registered event listeners (with timeout guard)
        for ev in events_to_dispatch {
            self.dispatch_event(ev).await;
        }

        next_transition
    }

    /// Dispatches an event to the global broadcast bus and registered event listeners.
    async fn dispatch_event(&self, event: RedFolderEvent) {
        self.broadcast_tx.send(event.clone()).ok();
        let listeners_guard = self.listeners.read().await;
        for listener in listeners_guard.iter() {
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

    /// Internal unified worker registration routine to ensure atomic registration and channel initialization.
    async fn register_worker_internal<R>(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
        allow_overwrite: bool,
        reregister_method_hint: &str,
        setup: impl FnOnce(&str, &RedFolderConfig, bool, Option<BlackoutWindow>) -> (WorkerState, R),
    ) -> Result<R> {
        config.validate()?;
        let worker_id = worker_id.into();

        let mut workers = self.workers.write().await;
        if workers.contains_key(&worker_id) {
            if allow_overwrite {
                warn!(worker=%worker_id, "re-registering worker: overwriting previous worker state and channels");
            } else {
                return Err(crate::error::RedFolderError::Service(format!(
                    "worker '{worker_id}' is already registered; use {reregister_method_hint} to update or unregister first"
                )));
            }
        }

        let engine = self.engine.read().await;
        let is_active = engine.is_blackout(&config);
        let active_window = if is_active {
            engine.current_window(&config)
        } else {
            None
        };
        drop(engine);

        let (state, rx) = setup(&worker_id, &config, is_active, active_window);
        workers.insert(worker_id.clone(), state);
        drop(workers);

        self.notify.notify_waiters();

        if allow_overwrite {
            debug!(worker=%worker_id, "re-registered worker in RedFolderService");
        } else {
            debug!(worker=%worker_id, "registered worker in RedFolderService");
        }
        Ok(rx)
    }

    /// Register a worker or trading strategy, returning a legacy `BlackoutNotification` receiver.
    /// Immediately notifies if worker is currently in blackout.
    pub async fn register_worker(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<BlackoutNotification>> {
        self.register_worker_internal(
            worker_id,
            config,
            false,
            "reregister_worker",
            |_, cfg, is_active, active_window| {
                let (tx, rx) = mpsc::unbounded_channel();
                if is_active {
                    tx.send(BlackoutNotification {
                        active: true,
                        window: active_window.clone(),
                    })
                    .ok();
                }
                let state = WorkerState {
                    config: cfg.clone(),
                    legacy_sender: Some(tx),
                    event_sender: None,
                    in_blackout: is_active,
                    active_window,
                    last_warned_window_start: None,
                };
                (state, rx)
            },
        )
        .await
    }

    /// Register a worker or trading strategy, returning an event-driven `RedFolderEvent` receiver.
    /// Immediately notifies if worker is currently in blackout.
    pub async fn register_worker_events(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<RedFolderEvent>> {
        self.register_worker_internal(
            worker_id,
            config,
            false,
            "reregister_worker_events",
            |wid, cfg, is_active, active_window| {
                let (tx, rx) = mpsc::unbounded_channel();
                if is_active {
                    if let Some(ref w) = active_window {
                        tx.send(RedFolderEvent::BlackoutStarted {
                            window: w.clone(),
                            worker_id: Some(wid.to_string()),
                        })
                        .ok();
                    }
                }
                let state = WorkerState {
                    config: cfg.clone(),
                    legacy_sender: None,
                    event_sender: Some(tx),
                    in_blackout: is_active,
                    active_window,
                    last_warned_window_start: None,
                };
                (state, rx)
            },
        )
        .await
    }

    /// Re-registers an existing worker or registers a new worker, updating its configuration
    /// and replacing its event channels. Emits a warning if an existing worker is overwritten.
    pub async fn reregister_worker(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<BlackoutNotification>> {
        self.register_worker_internal(
            worker_id,
            config,
            true,
            "reregister_worker",
            |_, cfg, is_active, active_window| {
                let (tx, rx) = mpsc::unbounded_channel();
                if is_active {
                    tx.send(BlackoutNotification {
                        active: true,
                        window: active_window.clone(),
                    })
                    .ok();
                }
                let state = WorkerState {
                    config: cfg.clone(),
                    legacy_sender: Some(tx),
                    event_sender: None,
                    in_blackout: is_active,
                    active_window,
                    last_warned_window_start: None,
                };
                (state, rx)
            },
        )
        .await
    }

    /// Re-registers an existing event worker or registers a new worker, updating its configuration
    /// and replacing its event channels. Emits a warning if an existing worker is overwritten.
    pub async fn reregister_worker_events(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> Result<mpsc::UnboundedReceiver<RedFolderEvent>> {
        self.register_worker_internal(
            worker_id,
            config,
            true,
            "reregister_worker_events",
            |wid, cfg, is_active, active_window| {
                let (tx, rx) = mpsc::unbounded_channel();
                if is_active {
                    if let Some(ref w) = active_window {
                        tx.send(RedFolderEvent::BlackoutStarted {
                            window: w.clone(),
                            worker_id: Some(wid.to_string()),
                        })
                        .ok();
                    }
                }
                let state = WorkerState {
                    config: cfg.clone(),
                    legacy_sender: None,
                    event_sender: Some(tx),
                    in_blackout: is_active,
                    active_window,
                    last_warned_window_start: None,
                };
                (state, rx)
            },
        )
        .await
    }

    /// Unregister a worker by ID.
    pub async fn unregister_worker(&self, worker_id: &str) {
        self.workers.write().await.remove(worker_id);
        self.notify.notify_waiters();
    }

    /// Returns the active and upcoming blackout windows derived specifically for the given worker.
    pub async fn windows_for_worker(&self, worker_id: &str) -> Vec<BlackoutWindow> {
        let cfg = {
            let workers = self.workers.read().await;
            workers.get(worker_id).map(|w| w.config.clone())
        };
        if let Some(config) = cfg {
            let engine = self.engine.read().await;
            engine.windows_for_config(&config, Utc::now())
        } else {
            Vec::new()
        }
    }

    /// Whether the background worker tasks are currently running.
    pub async fn is_running(&self) -> bool {
        self.sync_meta.lock().await.state == ServiceState::Running
    }

    /// Start the background synchronization and evaluation loops.
    pub async fn start(&self) -> Result<()> {
        {
            let mut meta = self.sync_meta.lock().await;
            if meta.state == ServiceState::Running || meta.state == ServiceState::Starting {
                return Err(crate::error::RedFolderError::Service(
                    "service is already running".to_string(),
                ));
            }
            let workers = self.workers.read().await;
            if workers.is_empty() {
                warn!("no workers registered — RedFolderService not starting");
                return Ok(());
            }
            if !workers.values().any(|w| w.config.enabled) {
                info!("all registered worker blackout configs are disabled");
                return Ok(());
            }
            meta.state = ServiceState::Starting;
        }

        // Perform initial calendar synchronization fallibly
        let initial = self.refresh_outcome(false, false).await;
        let first_delay = match initial {
            Ok(RefreshOutcome::Fresh) => until_next_midnight(),
            Ok(RefreshOutcome::Degraded) => std::time::Duration::from_secs(RETRY_LADDER[0]),
            Err(e) => {
                let mut meta = self.sync_meta.lock().await;
                meta.state = ServiceState::Stopped;
                return Err(e);
            }
        };

        let cancel_token = CancellationToken::new();
        *self.cancel_token.lock().await = Some(cancel_token.clone());

        let mut handles = Vec::new();

        // 1. Daily midnight fetch loop with single retry cadence ladder
        {
            let this = self.clone();
            let token = cancel_token.child_token();
            let handle = tokio::spawn(async move {
                let mut delay = first_delay;
                let mut failures = 0usize;
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = token.cancelled() => break,
                    }
                    let outcome = tokio::select! {
                        res = this.refresh_outcome(false, true) => res,
                        _ = token.cancelled() => break,
                    };
                    match outcome {
                        Ok(RefreshOutcome::Fresh) => {
                            failures = 0;
                            delay = until_next_midnight();
                        }
                        Ok(RefreshOutcome::Degraded) | Err(_) => {
                            delay = std::time::Duration::from_secs(
                                RETRY_LADDER[failures.min(RETRY_LADDER.len() - 1)],
                            );
                            failures = failures.saturating_add(1);
                            warn!(?delay, "calendar not freshly synchronized; retrying");
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // 2. Transition-driven blackout evaluation loop (with watchdog and interruptible notify)
        {
            let this = self.clone();
            let token = cancel_token.child_token();
            let notify = self.notify.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let next_transition = this.check_and_notify_workers().await;
                    let check_interval = this.sync_meta.lock().await.check_interval;

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
            let mut meta = self.sync_meta.lock().await;
            meta.state = ServiceState::Running;
        }

        info!("RedFolderService background tasks started successfully");
        Ok(())
    }

    /// Stop all background tasks gracefully and await their termination.
    pub async fn stop(&self) {
        {
            let mut meta = self.sync_meta.lock().await;
            if meta.state == ServiceState::Stopped || meta.state == ServiceState::Stopping {
                return;
            }
            meta.state = ServiceState::Stopping;
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
            let mut meta = self.sync_meta.lock().await;
            meta.state = ServiceState::Stopped;
        }
    }

    /// Manually trigger a calendar download and recompile blackout windows.
    pub async fn refresh(&self) -> Result<()> {
        self.refresh_internal(false, false).await
    }

    /// Force a fresh calendar fetch from the remote API, bypassing cache.
    pub async fn force_refresh(&self) -> Result<()> {
        self.refresh_internal(true, false).await
    }

    /// Internal helper that invokes `refresh_outcome` and discards the outcome variant.
    async fn refresh_internal(
        &self,
        force_remote: bool,
        skip_if_already_fetched_today: bool,
    ) -> Result<()> {
        self.refresh_outcome(force_remote, skip_if_already_fetched_today)
            .await
            .map(|_| ())
    }

    /// Perform a calendar refresh attempt returning the detailed `RefreshOutcome`.
    pub async fn refresh_outcome(
        &self,
        force_remote: bool,
        skip_if_already_fetched_today: bool,
    ) -> Result<RefreshOutcome> {
        let _refresh_guard = self.refresh_lock.lock().await;
        let today = Utc::now().date_naive();

        // 1. Check if already fetched today under lock (if requested by scheduled loop)
        if skip_if_already_fetched_today {
            let meta = self.sync_meta.lock().await;
            let engine_empty = self.engine.read().await.is_empty();
            if meta.last_fetch_date == Some(today) && !engine_empty {
                debug!("calendar already fetched and compiled for today");
                return Ok(RefreshOutcome::Fresh);
            }
        }

        // 2. Perform HTTP request outside lock
        let (client, tz, max_age, max_ratio) = {
            let m = self.sync_meta.lock().await;
            (
                m.client.clone(),
                m.client.calendar_timezone(),
                m.max_data_age,
                m.max_unparseable_ratio,
            )
        };

        let fetched = if force_remote {
            client.force_fetch_snapshot().await
        } else {
            client.fetch_snapshot().await
        };

        let mut snapshot = match fetched {
            Ok(s) => s,
            Err(e) => return self.fail_refresh(e).await,
        };

        // 3. Compile new engine from the snapshot (does not touch the live engine yet)
        let configs: Vec<RedFolderConfig> = {
            let workers = self.workers.read().await;
            workers
                .values()
                .filter(|w| w.config.enabled)
                .map(|w| w.config.clone())
                .collect()
        };
        let config_refs: Vec<&RedFolderConfig> = configs.iter().collect();
        let chrono_age = Duration::from_std(max_age).unwrap_or_else(|_| Duration::hours(36));
        let new_engine =
            BlackoutEngine::compile_snapshot(&snapshot, &config_refs, Utc::now(), tz, chrono_age);

        // 4. Reject data we cannot actually interpret
        let raw = snapshot.events.len();
        let parsed = new_engine.parsed_events().len();
        snapshot.stats.unparseable = raw.saturating_sub(parsed);
        let ratio = (raw.saturating_sub(parsed)) as f64 / raw.max(1) as f64;
        if parsed == 0 || ratio > max_ratio {
            let e = crate::error::RedFolderError::Calendar(format!(
                "{}/{} calendar events could not be parsed (limit {:.0}%); keeping previous calendar",
                raw.saturating_sub(parsed),
                raw,
                max_ratio * 100.0
            ));
            return self.fail_refresh(e).await;
        }

        let total_windows = new_engine.windows().len();
        *self.engine.write().await = new_engine;

        let outcome = if snapshot.source.is_fresh() {
            RefreshOutcome::Fresh
        } else {
            RefreshOutcome::Degraded
        };

        {
            let mut m = self.sync_meta.lock().await;
            m.last_sync_time = Some(snapshot.data_fetched_at);
            m.data_source = Some(snapshot.source);
            m.ingest = Some(snapshot.stats.clone());
            if snapshot.source == SnapshotSource::Remote {
                m.last_remote_success = Some(snapshot.data_fetched_at);
            }
            match outcome {
                RefreshOutcome::Fresh => {
                    m.last_fetch_date = Some(today);
                    m.last_sync_error = None;
                }
                RefreshOutcome::Degraded => {
                    // Do not set last_fetch_date: the skip-if-fetched-today guard must not suppress retries
                    m.last_sync_error = Some(format!(
                        "serving {:?} data from {}: {}",
                        snapshot.source,
                        snapshot.data_fetched_at,
                        snapshot
                            .remote_error
                            .as_deref()
                            .unwrap_or("remote unavailable")
                    ));
                }
            }
        }

        // Immediately reconcile worker states with newly compiled engine
        self.check_and_notify_workers().await;
        self.notify.notify_waiters();

        info!(windows=%total_windows, ?outcome, "refreshed economic calendar windows");

        if outcome == RefreshOutcome::Degraded {
            let err = self
                .sync_meta
                .lock()
                .await
                .last_sync_error
                .clone()
                .unwrap_or_default();
            self.dispatch_event(RedFolderEvent::CalendarSyncFailed { error: err })
                .await;
        }
        self.dispatch_event(RedFolderEvent::CalendarUpdated {
            total_events: raw,
            total_windows,
        })
        .await;

        Ok(outcome)
    }

    async fn fail_refresh(&self, e: crate::error::RedFolderError) -> Result<RefreshOutcome> {
        let msg = e.to_string();
        self.sync_meta.lock().await.last_sync_error = Some(msg.clone());
        self.check_and_notify_workers().await;
        self.notify.notify_waiters();
        self.dispatch_event(RedFolderEvent::CalendarSyncFailed { error: msg })
            .await;
        Err(e)
    }

    /// Returns the timestamp of the last successful calendar synchronization, if any.
    pub async fn last_sync_time(&self) -> Option<DateTime<Utc>> {
        self.sync_meta.lock().await.last_sync_time
    }

    /// Returns the error message from the most recent failed calendar synchronization, if any.
    pub async fn last_sync_error(&self) -> Option<String> {
        self.sync_meta.lock().await.last_sync_error.clone()
    }

    /// Check if a specific worker is currently in blackout.
    pub async fn is_blackout(&self, worker_id: &str) -> bool {
        self.check_calendar_staleness().await;
        let cfg = {
            let workers = self.workers.read().await;
            workers.get(worker_id).map(|w| w.config.clone())
        };
        if let Some(config) = cfg {
            let engine = self.engine.read().await;
            engine.is_blackout(&config)
        } else {
            false
        }
    }

    /// Get current active window for a worker, if any.
    pub async fn current_window(&self, worker_id: &str) -> Option<BlackoutWindow> {
        self.check_calendar_staleness().await;
        let cfg = {
            let workers = self.workers.read().await;
            workers.get(worker_id).map(|w| w.config.clone())
        };
        if let Some(config) = cfg {
            let engine = self.engine.read().await;
            engine.current_window(&config)
        } else {
            None
        }
    }

    /// Return upcoming blackout windows for a specific worker within `hours` hours.
    pub async fn get_upcoming_blackouts(&self, worker_id: &str, hours: u32) -> Vec<BlackoutWindow> {
        let cfg = {
            let workers = self.workers.read().await;
            workers.get(worker_id).map(|w| w.config.clone())
        };
        if let Some(config) = cfg {
            let engine = self.engine.read().await;
            engine.upcoming_blackouts(&config, hours)
        } else {
            Vec::new()
        }
    }
}

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

        let cfg = RedFolderConfig {
            weekend_enabled: false,
            before_min: 0,
            after_min: 15,
            ..Default::default()
        };
        service
            .set_engine(BlackoutEngine::compile(&raw, &[&cfg], now))
            .await;
        service.evaluate_and_notify().await;

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
