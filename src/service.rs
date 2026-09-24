use crate::calendar::CalendarClient;
use crate::config::RedFolderConfig;
use crate::engine::BlackoutEngine;
use crate::error::Result;
use crate::events::{EventListener, RedFolderEvent};
use crate::types::{BlackoutNotification, BlackoutWindow};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tracing::{debug, info, warn};

/// Internal state tracking for an individual registered worker or strategy.
struct WorkerState {
    config: RedFolderConfig,
    legacy_sender: mpsc::UnboundedSender<BlackoutNotification>,
    event_sender: mpsc::UnboundedSender<RedFolderEvent>,
    in_blackout: bool,
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
}

impl ServiceInner {
    fn new(
        client: CalendarClient,
        check_interval: std::time::Duration,
        broadcast_tx: broadcast::Sender<RedFolderEvent>,
    ) -> Self {
        Self {
            workers: HashMap::new(),
            engine: BlackoutEngine::new(),
            last_fetch_date: None,
            client,
            check_interval,
            broadcast_tx,
            listeners: Vec::new(),
        }
    }

    /// Evaluates blackout status and warnings for all registered workers,
    /// broadcasting domain events on transitions.
    fn check_and_notify_workers(&mut self) {
        if self.engine.windows().is_empty() {
            return;
        }

        let now = Utc::now();
        let mut events_to_dispatch: Vec<RedFolderEvent> = Vec::new();

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
                ws.legacy_sender
                    .send(BlackoutNotification {
                        active: is_active,
                        window: window.clone(),
                    })
                    .ok();

                if is_active {
                    if let Some(w) = window {
                        let end_str = w.end.format("%H:%M UTC").to_string();
                        warn!(worker=%worker_id, until=%end_str, "ENTERING news blackout window");
                        let ev = RedFolderEvent::BlackoutStarted {
                            window: w,
                            worker_id: Some(worker_id.clone()),
                        };
                        ws.event_sender.send(ev.clone()).ok();
                        events_to_dispatch.push(ev);
                    }
                } else {
                    info!(worker=%worker_id, "EXITING news blackout window");
                    // Send dummy or latest window for clearance
                    let dummy_window = BlackoutWindow {
                        start: now,
                        end: now,
                        events: vec![],
                    };
                    let ev = RedFolderEvent::BlackoutEnded {
                        window: dummy_window,
                        worker_id: Some(worker_id.clone()),
                    };
                    ws.event_sender.send(ev.clone()).ok();
                    events_to_dispatch.push(ev);
                }
            }

            // Warning check (if worker not in blackout and warning_before_min is set)
            if !ws.in_blackout {
                if let Some(warn_min) = ws.config.warning_before_min {
                    let upcoming = self.engine.upcoming_blackouts(&ws.config, 2);
                    if let Some(next_window) = upcoming.first() {
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
                                ws.event_sender.send(ev.clone()).ok();
                                events_to_dispatch.push(ev);
                            }
                        }
                    }
                }
            }
        }

        // 2. Dispatch events to global broadcast bus and registered event listeners
        for ev in events_to_dispatch {
            self.broadcast_tx.send(ev.clone()).ok();
            for listener in &self.listeners {
                let listener_clone = listener.clone();
                let ev_clone = ev.clone();
                tokio::spawn(async move {
                    listener_clone.on_event(&ev_clone).await;
                });
            }
        }
    }
}

/// Async service that orchestrates daily economic calendar synchronization and event-driven blackout alerts.
///
/// Features:
/// - Event-driven architecture with broadcast channels (`subscribe()`) and listener callbacks.
/// - Early warning notifications before blackout periods commence.
/// - Background daily refresh at midnight UTC with automatic disk fallback.
/// - 15-second background evaluation loop dispatching alerts when blackout state changes.
/// - Multi-worker independent registration with dedicated typed streams.
/// - Graceful cancellation and lifecycle management.
pub struct RedFolderService {
    inner: Arc<Mutex<ServiceInner>>,
    broadcast_tx: broadcast::Sender<RedFolderEvent>,
    shutdown_tx: watch::Sender<bool>,
}

impl RedFolderService {
    /// Create a new `RedFolderService` with an optional cache directory.
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        Self::with_client(CalendarClient::new(cache_dir))
    }

    /// Create with an existing `CalendarClient`.
    pub fn with_client(client: CalendarClient) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(Mutex::new(ServiceInner::new(
                client,
                std::time::Duration::from_secs(15),
                broadcast_tx.clone(),
            ))),
            broadcast_tx,
            shutdown_tx,
        }
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

    /// Configure the periodic evaluation loop frequency.
    pub async fn set_check_interval(&self, interval: std::time::Duration) {
        self.inner.lock().await.check_interval = interval;
    }

    /// Register a worker or trading strategy, returning a legacy `BlackoutNotification` receiver.
    pub async fn register_worker(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> mpsc::UnboundedReceiver<BlackoutNotification> {
        let worker_id = worker_id.into();
        let (legacy_tx, legacy_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        self.inner.lock().await.workers.insert(
            worker_id.clone(),
            WorkerState {
                config,
                legacy_sender: legacy_tx,
                event_sender: event_tx,
                in_blackout: false,
                last_warned_window_start: None,
            },
        );

        debug!(worker=%worker_id, "registered worker in RedFolderService");
        legacy_rx
    }

    /// Register a worker or trading strategy, returning an event-driven `RedFolderEvent` receiver.
    pub async fn register_worker_events(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> mpsc::UnboundedReceiver<RedFolderEvent> {
        let worker_id = worker_id.into();
        let (legacy_tx, _legacy_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        self.inner.lock().await.workers.insert(
            worker_id.clone(),
            WorkerState {
                config,
                legacy_sender: legacy_tx,
                event_sender: event_tx,
                in_blackout: false,
                last_warned_window_start: None,
            },
        );

        debug!(worker=%worker_id, "registered event worker in RedFolderService");
        event_rx
    }

    /// Unregister a worker by ID.
    pub async fn unregister_worker(&self, worker_id: &str) {
        self.inner.lock().await.workers.remove(worker_id);
    }

    /// Start the background synchronization and evaluation loops.
    pub async fn start(&self) -> Result<()> {
        {
            let inner = self.inner.lock().await;
            if inner.workers.is_empty() {
                warn!("no workers registered — RedFolderService not starting");
                return Ok(());
            }
            if !inner.workers.values().any(|w| w.config.enabled) {
                info!("all registered worker blackout configs are disabled");
                return Ok(());
            }
        }

        // Perform initial calendar synchronization
        self.refresh().await?;

        // 1. Daily midnight fetch loop
        {
            let inner_arc = self.inner.clone();
            let mut shutdown = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
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
                            let _ = Self::fetch_and_compile(&inner_arc).await;
                        }
                        _ = shutdown.changed() => break,
                    }
                }
            });
        }

        // 2. Periodic blackout evaluation loop
        {
            let inner_arc = self.inner.clone();
            let mut shutdown = self.shutdown_tx.subscribe();
            let interval = self.inner.lock().await.check_interval;
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(interval) => {
                            inner_arc.lock().await.check_and_notify_workers();
                        }
                        _ = shutdown.changed() => break,
                    }
                }
            });
        }

        info!("RedFolderService background tasks started successfully");
        Ok(())
    }

    /// Stop all background tasks gracefully.
    pub async fn stop(&self) {
        info!("stopping RedFolderService");
        self.shutdown_tx.send(true).ok();
    }

    /// Manually trigger a calendar download and recompile blackout windows.
    pub async fn refresh(&self) -> Result<()> {
        Self::fetch_and_compile(&self.inner).await
    }

    /// Internal helper that fetches events and compiles windows without holding mutex across network I/O.
    async fn fetch_and_compile(inner: &Arc<Mutex<ServiceInner>>) -> Result<()> {
        let today = Utc::now().date_naive();

        // 1. Check if already fetched today under lock
        {
            let state = inner.lock().await;
            if state.last_fetch_date == Some(today) && !state.engine.windows().is_empty() {
                debug!("calendar already fetched and compiled for today");
                return Ok(());
            }
        }

        // 2. Perform HTTP request outside lock
        let client = { inner.lock().await.client.clone() };
        let raw = client.fetch_or_cached().await?;

        // 3. Re-acquire lock to compile windows into engine
        let mut state = inner.lock().await;
        let configs: Vec<RedFolderConfig> = state
            .workers
            .values()
            .filter(|w| w.config.enabled)
            .map(|w| w.config.clone())
            .collect();
        let config_refs: Vec<&RedFolderConfig> = configs.iter().collect();

        state.engine = BlackoutEngine::compile(&raw, &config_refs, Utc::now());
        state.last_fetch_date = Some(today);

        let total_windows = state.engine.windows().len();
        info!(windows=%total_windows, "refreshed economic calendar windows");

        // Broadcast CalendarUpdated event
        state
            .broadcast_tx
            .send(RedFolderEvent::CalendarUpdated {
                total_events: raw.len(),
                total_windows,
            })
            .ok();

        Ok(())
    }

    /// Check if a specific worker is currently in blackout.
    pub async fn is_blackout(&self, worker_id: &str) -> bool {
        let inner = self.inner.lock().await;
        inner
            .workers
            .get(worker_id)
            .map(|w| inner.engine.is_blackout(&w.config))
            .unwrap_or(false)
    }

    /// Get current active window for a worker, if any.
    pub async fn current_window(&self, worker_id: &str) -> Option<BlackoutWindow> {
        let inner = self.inner.lock().await;
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

        let mut worker_events = service.register_worker_events("worker_usd", config).await;

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
}
