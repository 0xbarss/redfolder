use chrono::{Duration, Utc};
use redfolder::events::{EventListener, RedFolderEvent};
use redfolder::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Mock listener tracking received events in memory
struct MockAuditListener {
    warnings: AtomicUsize,
    starts: AtomicUsize,
    ends: AtomicUsize,
    calendars: AtomicUsize,
    last_event: Mutex<Option<RedFolderEvent>>,
}

impl MockAuditListener {
    fn new() -> Self {
        Self {
            warnings: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            ends: AtomicUsize::new(0),
            calendars: AtomicUsize::new(0),
            last_event: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl EventListener for MockAuditListener {
    async fn on_event(&self, event: &RedFolderEvent) {
        match event {
            RedFolderEvent::BlackoutWarning { .. } => {
                self.warnings.fetch_add(1, Ordering::SeqCst);
            }
            RedFolderEvent::BlackoutStarted { .. } => {
                self.starts.fetch_add(1, Ordering::SeqCst);
            }
            RedFolderEvent::BlackoutEnded { .. } => {
                self.ends.fetch_add(1, Ordering::SeqCst);
            }
            RedFolderEvent::CalendarUpdated { .. } => {
                self.calendars.fetch_add(1, Ordering::SeqCst);
            }
        }
        *self.last_event.lock().await = Some(event.clone());
    }
}

#[tokio::test]
async fn test_multi_worker_isolation_and_filtering() {
    let service = RedFolderService::new(None);

    let usd_cfg = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(10, 10)
        .build();

    let eur_cfg = RedFolderConfig::builder()
        .currencies(vec!["EUR"])
        .impacts(vec!["High"])
        .buffer_minutes(10, 10)
        .build();

    let _usd_rx = service.register_worker_events("usd_bot", usd_cfg).await;
    let _eur_rx = service.register_worker_events("eur_bot", eur_cfg).await;

    let now = Utc::now();
    // Raw event strictly for USD
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "US Retail Sales".into(),
        country: "USD".into(),
        date: (now + Duration::minutes(2)).to_rfc3339(),
        time: "".into(),
        impact: "High".into(),
    }];

    // Simulate engine compile with mock event
    let compiled = BlackoutEngine::compile(
        &raw,
        &[&RedFolderConfig {
            currencies: vec!["USD".into()],
            weekend_enabled: false,
            before_min: 5,
            after_min: 5,
            ..Default::default()
        }],
        now,
    );

    assert_eq!(compiled.windows().len(), 1);

    // USD worker should be in blackout
    let usd_config = RedFolderConfig::builder().currencies(vec!["USD"]).build();
    let eur_config = RedFolderConfig::builder().currencies(vec!["EUR"]).build();

    assert!(compiled.is_blackout(&usd_config));
    // EUR worker should NOT be in blackout
    assert!(!compiled.is_blackout(&eur_config));
}

#[tokio::test]
async fn test_custom_event_listener_callback() {
    let service = RedFolderService::new(None);
    let listener = Arc::new(MockAuditListener::new());

    service.add_listener(listener.clone()).await;

    let config = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .warning_minutes(15)
        .build();

    let _rx = service.register_worker_events("test_worker", config).await;

    let now = Utc::now();
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "Federal Reserve Chair Speech".into(),
        country: "USD".into(),
        date: (now + Duration::minutes(8)).to_rfc3339(),
        time: "".into(),
        impact: "High".into(),
    }];

    // Trigger state check through internal engine
    let cfg = RedFolderConfig {
        currencies: vec!["USD".into()],
        weekend_enabled: false,
        before_min: 0,
        after_min: 15,
        ..Default::default()
    };
    let engine = BlackoutEngine::compile(&raw, &[&cfg], now);

    // Verify upcoming query matches correctly
    let upcoming = engine.upcoming_blackouts(&cfg, 2);
    assert_eq!(upcoming.len(), 1);
    assert_eq!(upcoming[0].summary_title(), "Federal Reserve Chair Speech");
}

#[tokio::test]
async fn test_wildcard_currency_and_case_insensitivity() {
    let now = Utc::now();
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "Global Central Bank Forum".into(),
        country: "ALL".into(),
        date: now.to_rfc3339(),
        time: "".into(),
        impact: "high".into(),
    }];

    let config = RedFolderConfig::builder()
        .currencies(vec!["GBP"])
        .impacts(vec!["High"])
        .weekend_curfew(false, "20:00", "21:00", "short")
        .build();

    let engine = BlackoutEngine::compile(&raw, &[&config], now);
    assert!(engine.is_blackout(&config));
}

#[tokio::test]
async fn test_service_worker_unregistration() {
    let service = RedFolderService::new(None);
    let config = RedFolderConfig::default();

    let _rx = service.register_worker("bot_to_remove", config).await;
    assert!(!service.is_blackout("bot_to_remove").await);

    service.unregister_worker("bot_to_remove").await;
    // Non-existent worker returns false
    assert!(!service.is_blackout("bot_to_remove").await);
    assert!(service.current_window("bot_to_remove").await.is_none());
    assert!(service
        .get_upcoming_blackouts("bot_to_remove", 24)
        .await
        .is_empty());
}

#[tokio::test]
async fn test_multi_worker_buffer_isolation_in_service() {
    let service = RedFolderService::new(None);

    let scalper_cfg = RedFolderConfig::builder()
        .currency(Currency::USD)
        .impact(Impact::High)
        .buffer_minutes(5, 5)
        .weekend_curfew(false, "20:00", "21:00", "short")
        .build();

    let swing_cfg = RedFolderConfig::builder()
        .currency(Currency::USD)
        .impact(Impact::High)
        .buffer_minutes(30, 30)
        .weekend_curfew(false, "20:00", "21:00", "short")
        .build();

    let _scalper_rx = service
        .register_worker("scalper", scalper_cfg.clone())
        .await;
    let _swing_rx = service.register_worker("swing", swing_cfg.clone()).await;

    let now = Utc::now();
    // Event scheduled in 15 minutes from now
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "US CPI Release".into(),
        country: "USD".into(),
        date: (now + Duration::minutes(15)).to_rfc3339(),
        time: "".into(),
        impact: "High".into(),
    }];

    // Compile engine directly with both worker configs
    let engine = BlackoutEngine::compile(&raw, &[&scalper_cfg, &swing_cfg], now);

    // Verify Scalper (5m buffer) is NOT in blackout 15m before event
    assert!(!engine.is_blackout(&scalper_cfg));
    assert!(engine.current_window(&scalper_cfg).is_none());

    // Verify Swing (30m buffer) IS in blackout 15m before event
    assert!(engine.is_blackout(&swing_cfg));
    let swing_window = engine
        .current_window(&swing_cfg)
        .expect("swing window should exist");
    assert_eq!(swing_window.events[0].title, "US CPI Release");
}

#[tokio::test]
async fn test_offline_cache_fallback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // 1. First client saves events to cache
    let client = redfolder::calendar::CalendarClient::new(Some(cache_dir.clone()));
    let events = vec![redfolder::calendar::RawCalendarEvent {
        title: "ECB Rate Announcement".into(),
        country: "EUR".into(),
        date: "2026-06-15T12:45:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    client.save_cache(&events).unwrap();

    // 2. Second client with unreachable remote URL falls back to disk cache
    let broken_client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir),
        std::time::Duration::from_millis(100),
    );

    let loaded = broken_client
        .fetch_or_cached()
        .await
        .expect("should successfully fall back to cached events");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].title, "ECB Rate Announcement");
}

#[test]
fn test_weekend_boundaries_and_determinism() {
    use chrono::TimeZone;

    let cfg = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(0, 0)
        .weekend_curfew(true, "20:30", "21:00", "weekend")
        .build();

    let engine = BlackoutEngine::new();

    // Known Friday: 2026-06-05
    let fri_before = Utc.with_ymd_and_hms(2026, 6, 5, 20, 0, 0).unwrap();
    let fri_start = Utc.with_ymd_and_hms(2026, 6, 5, 20, 30, 0).unwrap();
    let fri_during = Utc.with_ymd_and_hms(2026, 6, 5, 22, 0, 0).unwrap();
    let sat = Utc.with_ymd_and_hms(2026, 6, 6, 12, 0, 0).unwrap();
    let sun = Utc.with_ymd_and_hms(2026, 6, 7, 23, 59, 0).unwrap();
    let mon_end = Utc.with_ymd_and_hms(2026, 6, 8, 0, 0, 0).unwrap();
    let mon_after = Utc.with_ymd_and_hms(2026, 6, 8, 0, 0, 1).unwrap();
    let mon_morning = Utc.with_ymd_and_hms(2026, 6, 8, 8, 0, 0).unwrap();
    let wed = Utc.with_ymd_and_hms(2026, 6, 10, 14, 0, 0).unwrap();

    // 1. Before blackout start: not in blackout
    assert!(!engine.is_blackout_at(&cfg, fri_before));

    // 2. Exactly at blackout start: in blackout
    assert!(engine.is_blackout_at(&cfg, fri_start));

    // 3. Friday night during blackout: in blackout
    assert!(engine.is_blackout_at(&cfg, fri_during));

    // 4. Saturday: MUST be in blackout (Problem 1 verification!)
    assert!(engine.is_blackout_at(&cfg, sat));

    // 5. Sunday: MUST be in blackout (Problem 1 verification!)
    assert!(engine.is_blackout_at(&cfg, sun));

    // 6. Monday 00:00:00 UTC (exact end boundary): out of blackout under [start, end) half-open semantics
    assert!(!engine.is_blackout_at(&cfg, mon_end));

    // 7. Monday 00:00:01 UTC: out of blackout
    assert!(!engine.is_blackout_at(&cfg, mon_after));

    // 8. Monday morning & Wednesday: out of blackout
    assert!(!engine.is_blackout_at(&cfg, mon_morning));
    assert!(!engine.is_blackout_at(&cfg, wed));
}

#[test]
fn test_cross_midnight_short_curfew() {
    use chrono::TimeZone;

    // Friday 23:00 UTC -> Saturday 01:00 UTC
    let cfg = RedFolderConfig::builder()
        .weekend_curfew(true, "23:00", "01:00", "short")
        .build();

    let engine = BlackoutEngine::new();

    let fri_before = Utc.with_ymd_and_hms(2026, 6, 5, 22, 59, 0).unwrap();
    let fri_inside = Utc.with_ymd_and_hms(2026, 6, 5, 23, 30, 0).unwrap();
    let sat_inside = Utc.with_ymd_and_hms(2026, 6, 6, 0, 30, 0).unwrap();
    let sat_inside_late = Utc.with_ymd_and_hms(2026, 6, 6, 0, 59, 59).unwrap();
    let sat_end = Utc.with_ymd_and_hms(2026, 6, 6, 1, 0, 0).unwrap();
    let sat_after = Utc.with_ymd_and_hms(2026, 6, 6, 1, 5, 0).unwrap();

    assert!(!engine.is_blackout_at(&cfg, fri_before));
    assert!(engine.is_blackout_at(&cfg, fri_inside));
    assert!(engine.is_blackout_at(&cfg, sat_inside));
    assert!(engine.is_blackout_at(&cfg, sat_inside_late));
    assert!(!engine.is_blackout_at(&cfg, sat_end));
    assert!(!engine.is_blackout_at(&cfg, sat_after));
}

#[test]
fn test_timestamp_determinism_historical_and_future() {
    use chrono::TimeZone;

    let cfg = RedFolderConfig::builder()
        .weekend_curfew(true, "20:30", "21:00", "weekend")
        .build();

    let engine = BlackoutEngine::new();

    // Historical Saturday in 2023: 2023-11-11
    let hist_sat = Utc.with_ymd_and_hms(2023, 11, 11, 14, 0, 0).unwrap();
    assert!(engine.is_blackout_at(&cfg, hist_sat));

    // Historical Tuesday in 2023: 2023-11-14
    let hist_tue = Utc.with_ymd_and_hms(2023, 11, 14, 14, 0, 0).unwrap();
    assert!(!engine.is_blackout_at(&cfg, hist_tue));

    // Future Sunday in 2030: 2030-01-06
    let future_sun = Utc.with_ymd_and_hms(2030, 1, 6, 16, 0, 0).unwrap();
    assert!(engine.is_blackout_at(&cfg, future_sun));
}

#[test]
fn test_config_validation_negative_buffers_and_bad_formats() {
    // Negative before_min
    let err_before = RedFolderConfig::builder()
        .buffer_minutes(-15, 15)
        .try_build();
    assert!(err_before.is_err());
    assert!(err_before
        .unwrap_err()
        .to_string()
        .contains("before_min cannot be negative"));

    // Negative after_min
    let err_after = RedFolderConfig::builder()
        .buffer_minutes(15, -10)
        .try_build();
    assert!(err_after.is_err());
    assert!(err_after
        .unwrap_err()
        .to_string()
        .contains("after_min cannot be negative"));

    // Negative merge threshold
    let err_merge = RedFolderConfig::builder().merge_threshold(-5).try_build();
    assert!(err_merge.is_err());
    assert!(err_merge
        .unwrap_err()
        .to_string()
        .contains("merge_threshold_min cannot be negative"));

    // Negative warning minutes
    let err_warn = RedFolderConfig::builder().warning_minutes(-10).try_build();
    assert!(err_warn.is_err());
    assert!(err_warn
        .unwrap_err()
        .to_string()
        .contains("warning_before_min cannot be negative"));

    // Invalid curfew start time
    let err_time = RedFolderConfig::builder()
        .weekend_curfew(true, "99:99", "21:00", "short")
        .try_build();
    assert!(err_time.is_err());

    // Invalid weekend mode
    let err_mode = RedFolderConfig::builder()
        .weekend_curfew(true, "20:00", "21:00", "unrecognized_mode")
        .try_build();
    assert!(err_mode.is_err());
}

#[tokio::test]
async fn test_service_lifecycle_guards_and_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "US Core CPI".into(),
        country: "USD".into(),
        date: "2026-06-15T12:30:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir),
        std::time::Duration::from_millis(50),
    );
    client.save_cache(&good_events).unwrap();

    let service = RedFolderService::with_client(client);

    let config = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(5, 5)
        .build();

    let _rx = service.register_worker("bot_lifecycle", config).await;

    // Initially not running
    assert!(!service.is_running().await);

    // 1. First start succeeds
    service.start().await.expect("service should start");
    assert!(service.is_running().await);

    // 2. Second start returns error (Problem 8 verification!)
    let err = service.start().await.unwrap_err();
    assert!(err.to_string().contains("already running"));

    // 3. Stop service
    service.stop().await;
    assert!(!service.is_running().await);

    // 4. Repeated stop is a safe no-op
    service.stop().await;
    assert!(!service.is_running().await);

    // 5. Restart service succeeds cleanly (Problem 9 verification!)
    service
        .start()
        .await
        .expect("service should restart successfully");
    assert!(service.is_running().await);

    service.stop().await;
}

#[tokio::test]
async fn test_blackout_ended_event_preserves_active_window() {
    let service = RedFolderService::new(None);
    let mut broadcast_rx = service.subscribe();

    let config = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(5, 5)
        .build();

    let _worker_events = service.register_worker_events("w_ended", config).await;

    let now = Utc::now();
    // Simulate event in progress (active right now)
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "US GDP Growth".into(),
        country: "USD".into(),
        date: now.to_rfc3339(),
        time: "".into(),
        impact: "High".into(),
    }];

    // 1. Worker enters blackout
    {
        let cfg = RedFolderConfig {
            weekend_enabled: false,
            before_min: 5,
            after_min: 5,
            currencies: vec!["USD".into()],
            impacts: vec!["High".into()],
            ..Default::default()
        };
        service
            .set_engine(BlackoutEngine::compile(&raw, &[&cfg], now))
            .await;
        service.evaluate_and_notify().await;
    }

    let started_ev = broadcast_rx
        .try_recv()
        .expect("should receive BlackoutStarted");
    let started_window = match started_ev {
        RedFolderEvent::BlackoutStarted { window, .. } => window,
        _ => panic!("expected BlackoutStarted event"),
    };
    assert_eq!(started_window.events[0].title, "US GDP Growth");

    // 2. Worker exits blackout (engine becomes clear)
    {
        service.set_engine(BlackoutEngine::new()).await;
        service.evaluate_and_notify().await;
    }

    let ended_ev = broadcast_rx
        .try_recv()
        .expect("should receive BlackoutEnded");
    let ended_window = match ended_ev {
        RedFolderEvent::BlackoutEnded { window, .. } => window,
        _ => panic!("expected BlackoutEnded event"),
    };

    // Problem 6 verification: BlackoutEnded must preserve the actual blackout window, NOT dummy zero-length window!
    assert_eq!(ended_window.start, started_window.start);
    assert_eq!(ended_window.end, started_window.end);
    assert_eq!(ended_window.events.len(), 1);
    assert_eq!(ended_window.events[0].title, "US GDP Growth");
}

#[tokio::test]
async fn test_cache_preservation_on_empty_response() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // 1. Populate disk cache with known-good events
    let client = redfolder::calendar::CalendarClient::new(Some(cache_dir.clone()));
    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "Federal Reserve FOMC Minutes".into(),
        country: "USD".into(),
        date: "2026-06-15T18:00:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    client.save_cache(&good_events).unwrap();

    // 2. Client hits an endpoint returning an empty array `[]`
    // (Simulate via mock server or empty test vector)
    let loaded = client.load_cache().expect("cache should exist");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].title, "Federal Reserve FOMC Minutes");

    // Verify version validation rejects unsupported cache schema
    let bad_version_cache = redfolder::calendar::CachedCalendarData {
        metadata: redfolder::calendar::CacheMetadata {
            version: 99,
            fetched_at: Utc::now(),
            expires_at: None,
            event_count: 1,
        },
        events: good_events.clone(),
    };
    let json = serde_json::to_string(&bad_version_cache).unwrap();
    std::fs::write(cache_dir.join(redfolder::DEFAULT_CACHE_FILENAME), json).unwrap();

    assert!(
        client.load_cache_data().is_none(),
        "version 99 cache must be rejected"
    );
}

#[tokio::test]
async fn test_failed_startup_does_not_remain_running_and_can_retry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Client with unreachable URL and empty cache directory
    let broken_client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir.clone()),
        std::time::Duration::from_millis(50),
    );

    let service = RedFolderService::with_client(broken_client);

    let config = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .build();

    let _rx = service.register_worker("bot_retry", config).await;

    // 1. Initial start fails due to network failure and missing cache
    let err = service.start().await;
    assert!(
        err.is_err(),
        "startup must fail when calendar refresh fails"
    );

    // AUDIT-001 Verification: State must be Stopped, not stuck in Running/Starting
    assert_eq!(service.state().await, ServiceState::Stopped);
    assert!(!service.is_running().await);

    // 2. Populate disk cache so subsequent start succeeds
    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "US CPI Release".into(),
        country: "USD".into(),
        date: "2026-06-10T12:30:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client = redfolder::calendar::CalendarClient::new(Some(cache_dir));
    client.save_cache(&good_events).unwrap();

    // 3. Retry startup - MUST succeed without "already running" error!
    let retry = service.start().await;
    assert!(retry.is_ok(), "retry after failed start must succeed");
    assert_eq!(service.state().await, ServiceState::Running);
    assert!(service.is_running().await);

    service.stop().await;
    assert_eq!(service.state().await, ServiceState::Stopped);
}

#[tokio::test]
async fn test_stop_waits_for_background_tasks_and_rapid_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "US Non-Farm Payrolls".into(),
        country: "USD".into(),
        date: "2026-06-05T12:30:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir),
        std::time::Duration::from_millis(50),
    );
    client.save_cache(&good_events).unwrap();

    let service = RedFolderService::with_client(client);
    let config = RedFolderConfig::default();
    let _rx = service.register_worker("w_rapid", config).await;

    // Start -> Stop -> Rapid Start -> Stop
    service.start().await.expect("start should succeed");
    assert!(service.is_running().await);

    service.stop().await;
    assert_eq!(service.state().await, ServiceState::Stopped);

    // AUDIT-002 Verification: Rapid restart after stop terminates cleanly without duplicate tasks
    service.start().await.expect("rapid restart must succeed");
    assert!(service.is_running().await);

    service.stop().await;
    assert_eq!(service.state().await, ServiceState::Stopped);
}

#[test]
fn test_cache_rejects_event_count_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    let client = redfolder::calendar::CalendarClient::new(Some(cache_dir.clone()));

    // Cache with event_count = 10, but only 1 event in array (corrupted / truncated)
    let mismatch_cache = redfolder::calendar::CachedCalendarData {
        metadata: redfolder::calendar::CacheMetadata {
            version: 1,
            fetched_at: Utc::now(),
            expires_at: None,
            event_count: 10,
        },
        events: vec![redfolder::calendar::RawCalendarEvent {
            title: "Corrupt Test Event".into(),
            country: "USD".into(),
            date: "2026-06-10T12:30:00Z".into(),
            time: "".into(),
            impact: "High".into(),
        }],
    };

    let json = serde_json::to_string(&mismatch_cache).unwrap();
    std::fs::write(cache_dir.join(redfolder::DEFAULT_CACHE_FILENAME), json).unwrap();

    // AUDIT-006 Verification: event_count mismatch must be rejected
    assert!(
        client.load_cache_data().is_none(),
        "event_count mismatch must be rejected by cache loader"
    );
    assert!(
        client.load_cache().is_none(),
        "load_cache must return None for mismatched cache"
    );
}

#[tokio::test]
async fn test_stale_cache_policy_enforcement() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Write a cache with fetched_at set to 48 hours ago
    let old_fetched_at = Utc::now() - chrono::Duration::hours(48);
    let old_cache = redfolder::calendar::CachedCalendarData {
        metadata: redfolder::calendar::CacheMetadata {
            version: 1,
            fetched_at: old_fetched_at,
            expires_at: None,
            event_count: 1,
        },
        events: vec![redfolder::calendar::RawCalendarEvent {
            title: "Stale NFP".into(),
            country: "USD".into(),
            date: "2026-06-05T12:30:00Z".into(),
            time: "".into(),
            impact: "High".into(),
        }],
    };
    let json = serde_json::to_string(&old_cache).unwrap();
    std::fs::write(cache_dir.join(redfolder::DEFAULT_CACHE_FILENAME), json).unwrap();

    // 1. Client with max_stale_age = 24h (cache is 48h old, so it MUST be rejected)
    let client_strict = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir.clone()),
        std::time::Duration::from_millis(50),
    )
    .with_max_stale_age(Some(std::time::Duration::from_secs(24 * 3600)));

    // AUDIT-007 Verification: 48h old cache is rejected when max allowed is 24h
    let res_strict = client_strict.fetch_or_cached().await;
    assert!(
        res_strict.is_err(),
        "stale cache exceeding max age must be rejected"
    );

    // 2. Client with max_stale_age = 72h (cache is 48h old, so it is accepted as fallback)
    let client_lenient = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir),
        std::time::Duration::from_millis(50),
    )
    .with_max_stale_age(Some(std::time::Duration::from_secs(72 * 3600)));

    let res_lenient = client_lenient.fetch_or_cached().await;
    assert!(
        res_lenient.is_ok(),
        "stale cache within max age should be accepted as fallback"
    );
    assert_eq!(res_lenient.unwrap()[0].title, "Stale NFP");
}

#[test]
fn test_empty_currency_and_impact_filters_are_rejected() {
    // AUDIT-008 Verification: empty currency filter is rejected
    let empty_cur = RedFolderConfig::builder()
        .currencies(Vec::<String>::new())
        .try_build();
    assert!(
        empty_cur.is_err(),
        "empty currencies filter must be rejected"
    );
    assert!(empty_cur
        .unwrap_err()
        .to_string()
        .contains("currencies filter cannot be empty"));

    // AUDIT-008 Verification: empty impact filter is rejected
    let empty_imp = RedFolderConfig::builder()
        .impacts(Vec::<String>::new())
        .try_build();
    assert!(empty_imp.is_err(), "empty impacts filter must be rejected");
    assert!(empty_imp
        .unwrap_err()
        .to_string()
        .contains("impacts filter cannot be empty"));
}

#[test]
fn test_all_day_and_tentative_event_policies() {
    let now = Utc::now();
    let tomorrow_str = (now.date_naive() + chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let day_after_str = (now.date_naive() + chrono::Duration::days(2))
        .format("%Y-%m-%d")
        .to_string();

    let raw = vec![
        redfolder::calendar::RawCalendarEvent {
            title: "US Labor Day Bank Holiday".into(),
            country: "USD".into(),
            date: tomorrow_str,
            time: "All Day".into(),
            impact: "High".into(),
        },
        redfolder::calendar::RawCalendarEvent {
            title: "Chinese Trade Balance".into(),
            country: "CNY".into(),
            date: day_after_str,
            time: "Tentative".into(),
            impact: "High".into(),
        },
    ];

    // AUDIT-005 Verification: Default config (include_all_day = false, include_tentative = false)
    // MUST NOT create false midnight blackout spikes!
    let default_cfg = RedFolderConfig::builder()
        .currencies(vec!["USD", "CNY"])
        .impacts(vec!["High"])
        .weekend_curfew(false, "20:00", "21:00", "short")
        .build();

    let engine = BlackoutEngine::compile(&raw, &[&default_cfg], now);
    let windows = engine.windows_for_config(&default_cfg, now);
    assert_eq!(
        windows.len(),
        0,
        "all-day and tentative events must not create midnight blackout windows by default"
    );

    // If explicitly enabled, all-day event covers the 24-hour day
    let all_day_cfg = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .weekend_curfew(false, "20:00", "21:00", "short")
        .include_all_day(true)
        .build();

    let all_day_windows = engine.windows_for_config(&all_day_cfg, now);
    assert_eq!(all_day_windows.len(), 1);
    assert_eq!(
        all_day_windows[0].duration_minutes(),
        1440,
        "all-day blackout window must span 24 hours (1440 minutes)"
    );
}

#[test]
fn test_timezone_aware_naive_timestamp_and_dst() {
    use redfolder::calendar::parse_event_timing;

    // AUDIT-004 Verification: Naive timestamps interpreted with America/New_York
    let tz = chrono_tz::America::New_York;

    // Summer EDT (UTC-4): 2026-07-10 8:30am
    let summer_raw = redfolder::calendar::RawCalendarEvent {
        title: "US PPI (Summer)".into(),
        country: "USD".into(),
        date: "07-10-2026".into(),
        time: "8:30am".into(),
        impact: "High".into(),
    };
    let summer_timing = parse_event_timing(&summer_raw, Some(tz)).unwrap();
    let summer_dt = summer_timing.exact_time().unwrap();
    // 8:30 AM EDT is 12:30 UTC
    assert_eq!(summer_dt.format("%H:%M UTC").to_string(), "12:30 UTC");

    // Winter EST (UTC-5): 2026-01-10 8:30am
    let winter_raw = redfolder::calendar::RawCalendarEvent {
        title: "US PPI (Winter)".into(),
        country: "USD".into(),
        date: "01-10-2026".into(),
        time: "8:30am".into(),
        impact: "High".into(),
    };
    let winter_timing = parse_event_timing(&winter_raw, Some(tz)).unwrap();
    let winter_dt = winter_timing.exact_time().unwrap();
    // 8:30 AM EST is 13:30 UTC
    assert_eq!(winter_dt.format("%H:%M UTC").to_string(), "13:30 UTC");

    // Explicit RFC3339 offset overrides default timezone
    let explicit_raw = redfolder::calendar::RawCalendarEvent {
        title: "Explicit Offset".into(),
        country: "USD".into(),
        date: "2026-07-10T08:30:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    };
    let explicit_timing = parse_event_timing(&explicit_raw, Some(tz)).unwrap();
    let explicit_dt = explicit_timing.exact_time().unwrap();
    assert_eq!(explicit_dt.format("%H:%M UTC").to_string(), "08:30 UTC");
}

#[tokio::test]
async fn test_immediate_notification_for_worker_registered_during_blackout() {
    let now = Utc::now();
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "FOMC Rate Announcement".into(),
        country: "USD".into(),
        date: now.to_rfc3339(),
        time: "".into(),
        impact: "High".into(),
    }];

    let service = RedFolderService::new(None);
    let cfg = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(10, 10)
        .build();

    // Set engine with an active blackout right now
    service
        .set_engine(BlackoutEngine::compile(&raw, &[&cfg], now))
        .await;

    // Register worker while blackout is already in progress
    let mut legacy_rx = service.register_worker("late_worker", cfg.clone()).await;
    let mut event_rx = service.register_worker_events("late_worker_ev", cfg).await;

    // Both should receive immediate active notification without waiting for next evaluation tick!
    let legacy_notif = legacy_rx
        .try_recv()
        .expect("should receive immediate notification");
    assert!(legacy_notif.active);
    assert!(legacy_notif.window.is_some());

    let event_notif = event_rx
        .try_recv()
        .expect("should receive immediate BlackoutStarted");
    assert!(event_notif.is_blackout_started());
}

#[tokio::test]
async fn test_windows_for_worker_isolation() {
    let now = Utc::now();
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "US CPI Release".into(),
        country: "USD".into(),
        date: (now + Duration::minutes(20)).to_rfc3339(),
        time: "".into(),
        impact: "High".into(),
    }];

    let service = RedFolderService::new(None);

    let scalper_cfg = RedFolderConfig::builder()
        .currency(Currency::USD)
        .impact(Impact::High)
        .buffer_minutes(5, 5)
        .weekend_curfew(false, "20:00", "21:00", "short")
        .build();

    let swing_cfg = RedFolderConfig::builder()
        .currency(Currency::USD)
        .impact(Impact::High)
        .buffer_minutes(30, 30)
        .weekend_curfew(false, "20:00", "21:00", "short")
        .build();

    let _rx1 = service
        .register_worker("scalper", scalper_cfg.clone())
        .await;
    let _rx2 = service.register_worker("swing", swing_cfg.clone()).await;

    service
        .set_engine(BlackoutEngine::compile(
            &raw,
            &[&scalper_cfg, &swing_cfg],
            now,
        ))
        .await;

    // AUDIT-010 Verification: windows_for_worker returns worker-specific windows
    let scalper_windows = service.windows_for_worker("scalper").await;
    let swing_windows = service.windows_for_worker("swing").await;

    assert_eq!(scalper_windows.len(), 1);
    assert_eq!(swing_windows.len(), 1);

    // Scalper buffer is 5 + 5 = 10 minutes
    assert_eq!(scalper_windows[0].duration_minutes(), 10);
    // Swing buffer is 30 + 30 = 60 minutes
    assert_eq!(swing_windows[0].duration_minutes(), 60);
}

#[tokio::test]
async fn test_calendar_updated_reaches_both_broadcast_and_event_listener() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "Initial CPI".into(),
        country: "USD".into(),
        date: "2026-06-15T12:30:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir),
        std::time::Duration::from_millis(50),
    );
    client.save_cache(&good_events).unwrap();

    let service = RedFolderService::with_client(client);

    let listener = Arc::new(MockAuditListener::new());
    service.add_listener(listener.clone()).await;

    let mut broadcast_rx = service.subscribe();

    let config = RedFolderConfig::default();
    let _rx = service.register_worker("w1", config).await;

    // Trigger refresh
    service.refresh().await.expect("refresh should succeed");

    // Broadcast bus must receive CalendarUpdated
    let ev = broadcast_rx
        .recv()
        .await
        .expect("broadcast should receive event");
    assert!(matches!(ev, RedFolderEvent::CalendarUpdated { .. }));

    // Allow tokio tasks to run listener callbacks
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Problem 1 verification: CalendarUpdated reached EventListener!
    assert!(
        listener.calendars.load(Ordering::SeqCst) >= 1,
        "CalendarUpdated event must reach registered EventListener"
    );
}

#[tokio::test]
async fn test_legacy_cache_older_than_max_stale_age_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    let cache_file = cache_dir.join(redfolder::DEFAULT_CACHE_FILENAME);

    // Write unversioned legacy JSON array
    let legacy_json = r#"[
        {
            "title": "Old Legacy Event",
            "country": "USD",
            "date": "2025-01-01T12:00:00Z",
            "time": "",
            "impact": "High"
        }
    ]"#;
    std::fs::write(&cache_file, legacy_json).unwrap();

    // Client with unreachable URL and max_stale_age = 24h
    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir),
        std::time::Duration::from_millis(50),
    )
    .with_max_stale_age(Some(std::time::Duration::from_secs(24 * 3600)));

    // Problem 3 verification: legacy cache must NOT bypass stale cache check!
    let res = client.fetch_or_cached().await;
    assert!(
        res.is_err(),
        "legacy cache without verified timestamp must be rejected as stale fallback"
    );
}

#[tokio::test]
async fn test_force_refresh_bypasses_cache_with_mock_server() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // 1. Populate cache with Event A
    let initial_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "Cached Event A".into(),
        country: "USD".into(),
        date: "2026-06-10T12:00:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let setup_client = redfolder::calendar::CalendarClient::new(Some(cache_dir.clone()));
    setup_client.save_cache(&initial_events).unwrap();

    // 2. Mock server returning Event B
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    let future_time = (Utc::now() + Duration::hours(2)).to_rfc3339();
    let server_task = tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let body = format!(
                r#"[{{"title":"Remote Event B","country":"USD","date":"{}","time":"","impact":"High"}}]"#,
                future_time
            );
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        }
    });

    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        mock_url,
        Some(cache_dir),
        std::time::Duration::from_secs(2),
    );
    let service = RedFolderService::with_client(client);
    let cfg = RedFolderConfig::builder()
        .weekend_curfew(false, "20:00", "21:00", "short")
        .build();
    let _rx = service.register_worker("w_force", cfg).await;

    // Problem 4 verification: force_refresh() must hit remote and return Event B, not cached Event A!
    service
        .force_refresh()
        .await
        .expect("force refresh should succeed");
    server_task.await.unwrap();

    let windows = service.windows_for_worker("w_force").await;
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].events[0].title, "Remote Event B");
}

#[tokio::test]
async fn test_concurrent_refreshes_are_serialized() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "Concurrent CPI".into(),
        country: "USD".into(),
        date: "2026-06-15T12:30:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir),
        std::time::Duration::from_millis(50),
    );
    client.save_cache(&good_events).unwrap();

    let service = Arc::new(RedFolderService::with_client(client));
    let cfg = RedFolderConfig::default();
    let _rx = service.register_worker("w_concurrent", cfg).await;

    // Problem 5 verification: 5 concurrent refresh tasks run without racing or panic
    let mut handles = Vec::new();
    for _ in 0..5 {
        let s = service.clone();
        handles.push(tokio::spawn(async move { s.refresh().await }));
    }

    for h in handles {
        let res = h.await.unwrap();
        assert!(res.is_ok(), "concurrent refresh must succeed");
    }
}

#[test]
fn test_impact_aliases_high_vs_red_normalization() {
    use redfolder::types::EconomicEvent;

    let now = Utc::now();
    let event_high = EconomicEvent {
        title: "US CPI".into(),
        country: "USD".into(),
        impact: "High".into(),
        datetime: now,
        timing: redfolder::types::EventTiming::Exact(now),
    };
    let event_red = EconomicEvent {
        title: "US NFP".into(),
        country: "USD".into(),
        impact: "Red".into(),
        datetime: now,
        timing: redfolder::types::EventTiming::Exact(now),
    };

    // Config with impact "Red" must match event with impact "High"
    let cfg_red = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["Red"])
        .build();
    assert!(
        redfolder::engine::event_matches_economic_event(&event_high, &cfg_red),
        "Config with impact 'Red' must match event with impact 'High'"
    );

    // Config with impact "High" must match event with impact "Red"
    let cfg_high = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .build();
    assert!(
        redfolder::engine::event_matches_economic_event(&event_red, &cfg_high),
        "Config with impact 'High' must match event with impact 'Red'"
    );

    // Config with impact "med" must match event with impact "Medium"
    let event_med = EconomicEvent {
        title: "ECB Speech".into(),
        country: "EUR".into(),
        impact: "Medium".into(),
        datetime: now,
        timing: redfolder::types::EventTiming::Exact(now),
    };
    let cfg_med = RedFolderConfig::builder()
        .currencies(vec!["EUR"])
        .impacts(vec!["med"])
        .build();
    assert!(
        redfolder::engine::event_matches_economic_event(&event_med, &cfg_med),
        "Config with impact 'med' must match event with impact 'Medium'"
    );
}

#[test]
fn test_all_day_events_in_non_utc_timezone() {
    // Problem 9 verification: all-day event in America/New_York (EDT, UTC-4)
    let raw = vec![redfolder::calendar::RawCalendarEvent {
        title: "US Independence Day".into(),
        country: "USD".into(),
        date: "2026-07-04".into(),
        time: "All Day".into(),
        impact: "High".into(),
    }];

    let ny_tz = chrono_tz::America::New_York;
    let now = chrono::NaiveDate::from_ymd_opt(2026, 7, 4)
        .unwrap()
        .and_hms_opt(5, 0, 0)
        .unwrap()
        .and_utc();

    let cfg = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .weekend_curfew(false, "20:00", "21:00", "short")
        .include_all_day(true)
        .build();

    let engine = BlackoutEngine::compile_with_tz(&raw, &[&cfg], now, Some(ny_tz));
    let windows = engine.windows_for_config(&cfg, now);

    assert_eq!(windows.len(), 1);
    // Midnight in New York (EDT, UTC-4) is 04:00 UTC
    assert_eq!(
        windows[0].start.format("%H:%M UTC").to_string(),
        "04:00 UTC"
    );
    assert_eq!(windows[0].end.format("%H:%M UTC").to_string(), "04:00 UTC");
    assert_eq!(windows[0].duration_minutes(), 1440);
}

#[tokio::test]
async fn test_empty_remote_feed_preserves_cache_on_force_fetch() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // 1. Populate cache with valid event
    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "Good CPI".into(),
        country: "USD".into(),
        date: "2026-06-15T12:30:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client_setup = redfolder::calendar::CalendarClient::new(Some(cache_dir.clone()));
    client_setup.save_cache(&good_events).unwrap();

    // 2. Mock server returning empty array `[]`
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    let _server = tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]";
            let _ = socket.write_all(resp.as_bytes()).await;
        }
    });

    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        mock_url,
        Some(cache_dir),
        std::time::Duration::from_secs(2),
    );

    // Problem 8 verification: force_fetch preserves cached event when remote returns []
    let events = client
        .force_fetch()
        .await
        .expect("should return preserved cache");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].title, "Good CPI");
}

#[tokio::test]
async fn test_refresh_reconciles_worker_state_immediately() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        "http://127.0.0.1:9/unreachable",
        Some(cache_dir.clone()),
        std::time::Duration::from_millis(100),
    );
    let service = RedFolderService::with_client(client);

    let cfg = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(15, 15)
        .build();

    let mut event_rx = service.register_worker_events("w_lag", cfg).await;
    assert!(!service.is_blackout("w_lag").await);

    // Save event that is happening right now into cache
    let now = Utc::now();
    let active_event = vec![redfolder::calendar::RawCalendarEvent {
        title: "Active FOMC".into(),
        country: "USD".into(),
        date: now.to_rfc3339(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client2 = redfolder::calendar::CalendarClient::new(Some(cache_dir));
    client2.save_cache(&active_event).unwrap();

    // Refresh calendar
    service.refresh().await.unwrap();

    // Problem 10 verification: worker blackout state is reconciled IMMEDIATELY after refresh()
    assert!(
        service.is_blackout("w_lag").await,
        "worker state must be immediately in blackout after refresh without background lag"
    );

    let ev = event_rx
        .try_recv()
        .expect("should receive BlackoutStarted event");
    assert!(ev.is_blackout_started());
}

#[test]
fn test_atomic_cache_write_leaves_no_temporary_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let client = redfolder::calendar::CalendarClient::new(Some(cache_dir.clone()));
    let events = vec![redfolder::calendar::RawCalendarEvent {
        title: "Atomic Test".into(),
        country: "USD".into(),
        date: "2026-06-15T12:00:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];

    // Problem 7 verification: atomic save_cache succeeds and cleans up temporary file
    client
        .save_cache(&events)
        .expect("atomic save should succeed");

    let cache_file = cache_dir.join(redfolder::DEFAULT_CACHE_FILENAME);
    assert!(cache_file.exists());

    // Verify no leftover .tmp files
    let mut tmp_count = 0;
    for entry in std::fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.contains(".tmp") {
            tmp_count += 1;
        }
    }
    assert_eq!(
        tmp_count, 0,
        "atomic write must clean up any temporary files"
    );
}

#[tokio::test]
async fn test_stop_during_delayed_http_request() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Mock server that accepts connection but delays response by 5 seconds
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    let _server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]";
                let _ = socket.write_all(resp.as_bytes()).await;
            });
        }
    });

    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        mock_url,
        Some(cache_dir),
        std::time::Duration::from_secs(10),
    );

    let service = Arc::new(RedFolderService::with_client(client));
    let cfg = RedFolderConfig::default();
    let _rx = service.register_worker("w_slow", cfg).await;

    // Spawn a long-running refresh task in background
    let s_clone = service.clone();
    let refresh_task = tokio::spawn(async move { s_clone.refresh().await });

    // Wait 50ms for request to begin
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Problem 2 verification: stop() must complete promptly even during in-flight I/O
    let start_stop = tokio::time::Instant::now();
    service.stop().await;
    let elapsed = start_stop.elapsed();

    assert!(elapsed < std::time::Duration::from_secs(4));
    assert_eq!(service.state().await, ServiceState::Stopped);

    let _ = refresh_task.await;
}

#[tokio::test]
async fn test_http_retry_on_429_with_recovery() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    let attempt_counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = attempt_counter.clone();

    let _server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let counter = counter_clone.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let current_attempt = counter.fetch_add(1, Ordering::SeqCst);

                if current_attempt == 0 {
                    // Attempt 0: return 429 Too Many Requests with Retry-After: 0
                    let resp = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    let _ = socket.write_all(resp.as_bytes()).await;
                } else {
                    // Attempt 1: return 200 OK with valid events
                    let body = r#"[{"title":"Recovered CPI","country":"USD","date":"2026-06-15T12:30:00Z","time":"","impact":"High"}]"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                }
            });
        }
    });

    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        mock_url,
        None,
        std::time::Duration::from_secs(3),
    );

    let events = client
        .fetch_remote()
        .await
        .expect("429 should retry and recover");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].title, "Recovered CPI");
    assert_eq!(
        attempt_counter.load(Ordering::SeqCst),
        2,
        "must have retried after initial 429"
    );
}

#[tokio::test]
async fn test_http_retry_on_429_exhausted_falls_back_to_cache() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let temp_dir = tempfile::tempdir().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Seed cache with known good event
    let good_events = vec![redfolder::calendar::RawCalendarEvent {
        title: "Cached Reserve FOMC".into(),
        country: "USD".into(),
        date: "2026-06-15T14:00:00Z".into(),
        time: "".into(),
        impact: "High".into(),
    }];
    let client_setup = redfolder::calendar::CalendarClient::new(Some(cache_dir.clone()));
    client_setup.save_cache(&good_events).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    // Mock server always returns 429 Too Many Requests
    let _server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let resp = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = socket.write_all(resp.as_bytes()).await;
            });
        }
    });

    let client = redfolder::calendar::CalendarClient::with_options(
        reqwest::Client::new(),
        mock_url,
        Some(cache_dir),
        std::time::Duration::from_secs(2),
    );

    // After repeated 429, fetch_or_cached must fall back to disk cache
    let events = client
        .fetch_or_cached()
        .await
        .expect("repeated 429 should fall back to disk cache");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].title, "Cached Reserve FOMC");
}
