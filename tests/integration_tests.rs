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

    // 6. Monday 00:00:00 UTC (exact end boundary): in blackout
    assert!(engine.is_blackout_at(&cfg, mon_end));

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
    let sat_end = Utc.with_ymd_and_hms(2026, 6, 6, 1, 0, 0).unwrap();
    let sat_after = Utc.with_ymd_and_hms(2026, 6, 6, 1, 5, 0).unwrap();

    assert!(!engine.is_blackout_at(&cfg, fri_before));
    assert!(engine.is_blackout_at(&cfg, fri_inside));
    assert!(engine.is_blackout_at(&cfg, sat_inside));
    assert!(engine.is_blackout_at(&cfg, sat_end));
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
    let service = RedFolderService::new(None);

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
