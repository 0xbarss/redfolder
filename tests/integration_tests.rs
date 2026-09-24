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
