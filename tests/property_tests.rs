use chrono::{Duration, TimeZone, Utc};
use proptest::prelude::*;
use redfolder::calendar::{CalendarClient, RawCalendarEvent};
use redfolder::config::RedFolderConfig;
use redfolder::engine::BlackoutEngine;
use redfolder::types::Currency;
use std::io::Write;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn test_window_merging_invariants(
        event_offsets in prop::collection::vec(-500i64..500i64, 1..20),
        before_min in 1i64..60i64,
        after_min in 1i64..60i64,
        merge_threshold_min in 0i64..120i64,
    ) {
        let base_time = Utc.with_ymd_and_hms(2026, 10, 2, 12, 0, 0).unwrap();
        let query_origin = base_time - Duration::minutes(2000);
        let cfg = RedFolderConfig {
            currencies: vec![Currency::USD],
            impacts: vec![redfolder::types::Impact::High],
            before_min,
            after_min,
            merge_threshold_min,
            weekend_enabled: false,
            ..Default::default()
        };

        let raw_events: Vec<RawCalendarEvent> = event_offsets
            .iter()
            .enumerate()
            .map(|(idx, offset)| {
                let dt = base_time + Duration::minutes(*offset);
                RawCalendarEvent {
                    title: format!("Event {idx}"),
                    country: "USD".into(),
                    date: dt.to_rfc3339(),
                    time: String::new(),
                    impact: "High".into(),
                }
            })
            .collect();

        let engine = BlackoutEngine::compile(&raw_events, &[&cfg], query_origin);
        let windows = engine.windows_for_config(&cfg, query_origin);

        // Invariant 1: Windows are strictly sorted by start time
        for pair in windows.windows(2) {
            prop_assert!(pair[0].start <= pair[1].start, "windows must be sorted: {:?} > {:?}", pair[0].start, pair[1].start);
        }

        // Invariant 2: Windows are mutually non-overlapping
        for pair in windows.windows(2) {
            prop_assert!(pair[0].end <= pair[1].start, "windows must not overlap: end {:?} > next start {:?}", pair[0].end, pair[1].start);
        }

        // Invariant 3: Gap between adjacent windows must exceed merge_threshold_min
        let merge_threshold_dur = Duration::minutes(merge_threshold_min);
        for pair in windows.windows(2) {
            let gap = pair[1].start - pair[0].end;
            prop_assert!(gap >= merge_threshold_dur, "adjacent windows separated by {:?} which is less than threshold {:?}", gap, merge_threshold_dur);
        }

        // Invariant 4: Every event timestamp falls inside at least one window
        for offset in &event_offsets {
            let event_dt = base_time + Duration::minutes(*offset);
            let covered = windows.iter().any(|w| event_dt >= w.start && event_dt < w.end);
            prop_assert!(covered, "event at {:?} must be covered by a window", event_dt);
        }

        // Invariant 5: Point-in-time blackout check agrees with current_window
        for check_offset in (-550..550).step_by(15) {
            let t = base_time + Duration::minutes(check_offset);
            let in_blackout = engine.is_blackout_at(&cfg, t);
            let active_window = engine.current_window_at(&cfg, t);
            prop_assert_eq!(in_blackout, active_window.is_some(), "is_blackout_at and current_window_at must agree at {:?}", t);
        }
    }

    #[test]
    fn test_parse_timing_adversarial_input_never_panics(
        date_str in "\\PC{0,100}",
        time_str in "\\PC{0,50}",
        country_str in "\\PC{0,20}",
        title_str in "\\PC{0,200}",
        impact_str in "\\PC{0,20}",
    ) {
        let raw = RawCalendarEvent {
            title: title_str,
            country: country_str,
            date: date_str,
            time: time_str,
            impact: impact_str,
        };

        let cfg = RedFolderConfig::default();
        let engine = BlackoutEngine::compile(&[raw], &[&cfg], Utc::now());
        prop_assert!(engine.dropped_count() <= 1);
    }

    #[test]
    fn test_cache_loading_arbitrary_bytes_never_panics(
        garbage_bytes in prop::collection::vec(any::<u8>(), 0..2048),
    ) {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache_file = temp_dir.path().join("economic_calendar_cache.json");

        let mut f = std::fs::File::create(&cache_file).unwrap();
        f.write_all(&garbage_bytes).unwrap();
        drop(f);

        let client = CalendarClient::new(Some(temp_dir.path().to_path_buf()));
        let data = client.load_cache_data();
        prop_assert!(data.is_none());
    }
}
