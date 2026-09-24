use crate::calendar::CalendarClient;
use crate::config::RedFolderConfig;
use crate::engine::BlackoutEngine;
use crate::error::Result;
use crate::types::{BlackoutNotification, BlackoutWindow};
use chrono::{Duration, NaiveDate, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex};
use tracing::{debug, info, warn};

/// Internal state tracking for an individual registered worker or strategy.
struct WorkerState {
    config: RedFolderConfig,
    sender: mpsc::UnboundedSender<BlackoutNotification>,
    in_blackout: bool,
}

/// Internal mutable state protected by an async mutex.
struct ServiceInner {
    workers: HashMap<String, WorkerState>,
    engine: BlackoutEngine,
    last_fetch_date: Option<NaiveDate>,
    client: CalendarClient,
    check_interval: std::time::Duration,
}

impl ServiceInner {
    fn new(client: CalendarClient, check_interval: std::time::Duration) -> Self {
        Self {
            workers: HashMap::new(),
            engine: BlackoutEngine::new(),
            last_fetch_date: None,
            client,
            check_interval,
        }
    }

    /// Evaluates blackout status for all registered workers and broadcasts notifications on state transitions.
    fn check_and_notify_workers(&mut self) {
        if self.engine.windows().is_empty() {
            return;
        }

        // Collect state transitions to avoid holding multiple mutable references
        let transitions: Vec<(String, bool, Option<BlackoutWindow>)> = self
            .workers
            .iter()
            .filter(|(_, ws)| ws.config.enabled)
            .filter_map(|(id, ws)| {
                let is_active = self.engine.is_blackout(&ws.config);
                if is_active != ws.in_blackout {
                    let window = if is_active {
                        self.engine.current_window(&ws.config)
                    } else {
                        None
                    };
                    Some((id.clone(), is_active, window))
                } else {
                    None
                }
            })
            .collect();

        for (worker_id, is_active, window) in transitions {
            if let Some(ws) = self.workers.get_mut(&worker_id) {
                ws.in_blackout = is_active;
                if is_active {
                    let end_str = window
                        .as_ref()
                        .map(|w| w.end.format("%H:%M UTC").to_string())
                        .unwrap_or_else(|| "N/A".to_string());
                    warn!(worker=%worker_id, until=%end_str, "ENTERING news blackout window");
                } else {
                    info!(worker=%worker_id, "EXITING news blackout window");
                }
                ws.sender
                    .send(BlackoutNotification {
                        active: is_active,
                        window,
                    })
                    .ok();
            }
        }
    }
}

/// Async service that orchestrates daily economic calendar synchronization and real-time blackout alerts.
///
/// Features:
/// - Background daily refresh at midnight UTC with automatic disk fallback.
/// - 15-second background evaluation loop dispatching alerts when blackout state changes.
/// - Multi-worker independent registration with dedicated `mpsc::UnboundedReceiver` streams.
/// - Graceful cancellation and lifecycle management.
pub struct RedFolderService {
    inner: Arc<Mutex<ServiceInner>>,
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
        Self {
            inner: Arc::new(Mutex::new(ServiceInner::new(
                client,
                std::time::Duration::from_secs(15),
            ))),
            shutdown_tx,
        }
    }

    /// Configure the check loop frequency.
    pub async fn set_check_interval(&self, interval: std::time::Duration) {
        self.inner.lock().await.check_interval = interval;
    }

    /// Register a worker or trading strategy, returning an unbounded channel receiver
    /// that receives `BlackoutNotification` events whenever blackout status changes.
    pub async fn register_worker(
        &self,
        worker_id: impl Into<String>,
        config: RedFolderConfig,
    ) -> mpsc::UnboundedReceiver<BlackoutNotification> {
        let worker_id = worker_id.into();
        let (tx, rx) = mpsc::unbounded_channel();
        let enabled = config.enabled;
        let currencies = config.currencies.clone();

        self.inner.lock().await.workers.insert(
            worker_id.clone(),
            WorkerState {
                config,
                sender: tx,
                in_blackout: false,
            },
        );

        debug!(
            worker=%worker_id,
            enabled=%enabled,
            currencies=?currencies,
            "registered worker in RedFolderService"
        );
        rx
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
        info!(windows=%state.engine.windows().len(), "refreshed economic calendar windows");
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
    pub async fn get_upcoming_blackouts(
        &self,
        worker_id: &str,
        hours: u32,
    ) -> Vec<BlackoutWindow> {
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

    #[test]
    fn test_service_inner_check_and_notify() {
        let client = CalendarClient::new(None);
        let mut inner = ServiceInner::new(client, std::time::Duration::from_secs(1));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let config = RedFolderConfig::builder()
            .currencies(vec!["USD"])
            .impacts(vec!["High"])
            .build();

        inner.workers.insert(
            "worker_1".into(),
            WorkerState {
                config,
                sender: tx,
                in_blackout: false,
            },
        );

        let now = Utc::now();
        let raw = vec![crate::calendar::RawCalendarEvent {
            title: "US CPI Release".into(),
            country: "USD".into(),
            date: now.to_rfc3339(),
            time: "".into(),
            impact: "High".into(),
        }];

        let cfg = RedFolderConfig {
            weekend_enabled: false,
            ..Default::default()
        };
        inner.engine = BlackoutEngine::compile(&raw, &[&cfg], now);

        inner.check_and_notify_workers();
        assert!(inner.workers.get("worker_1").unwrap().in_blackout);

        let notif = rx.try_recv().expect("should receive enter notification");
        assert!(notif.active);
        assert!(notif.window.is_some());
    }

    #[tokio::test]
    async fn test_service_lifecycle_mocked() {
        let service = RedFolderService::new(None);
        let config = RedFolderConfig::default();
        let _rx = service.register_worker("test_bot", config).await;

        // Manually inject windows into inner engine
        {
            let mut inner = service.inner.lock().await;
            inner.last_fetch_date = Some(Utc::now().date_naive());
            let now = Utc::now();
            let raw = vec![crate::calendar::RawCalendarEvent {
                title: "FOMC Rate Decision".into(),
                country: "USD".into(),
                date: (now + Duration::minutes(60)).to_rfc3339(),
                time: "".into(),
                impact: "High".into(),
            }];
            let cfg = RedFolderConfig {
                weekend_enabled: false,
                ..Default::default()
            };
            inner.engine = BlackoutEngine::compile(&raw, &[&cfg], now);
        }

        let upcoming = service.get_upcoming_blackouts("test_bot", 2).await;
        assert_eq!(upcoming.len(), 1);
        assert_eq!(upcoming[0].events[0].title, "FOMC Rate Decision");

        service.stop().await;
    }
}
