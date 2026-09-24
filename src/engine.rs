use crate::calendar::{parse_event_datetime, RawCalendarEvent};
use crate::config::RedFolderConfig;
use crate::curfew::{next_weekend_window, weekend_window_title};
use crate::types::{BlackoutWindow, EconomicEvent, WindowEvent};
use chrono::{DateTime, Duration, Utc};
use tracing::info;

/// In-memory engine that manages calendar events and derives per-worker blackout windows.
#[derive(Debug, Clone, Default)]
pub struct BlackoutEngine {
    /// Raw calendar events provided to the engine.
    raw_events: Vec<RawCalendarEvent>,
    /// Pre-parsed economic events.
    parsed_events: Vec<EconomicEvent>,
    /// Precompiled windows for backwards-compatible reference and single-worker setups.
    windows: Vec<BlackoutWindow>,
}

impl BlackoutEngine {
    /// Create an empty engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            raw_events: Vec::new(),
            parsed_events: Vec::new(),
            windows: Vec::new(),
        }
    }

    /// Construct engine from raw calendar events.
    #[must_use]
    pub fn from_events(raw_events: &[RawCalendarEvent]) -> Self {
        let parsed_events: Vec<EconomicEvent> = raw_events
            .iter()
            .filter_map(|raw| {
                let dt = parse_event_datetime(raw)?;
                Some(EconomicEvent {
                    title: raw.title.clone(),
                    country: raw.country.clone(),
                    impact: raw.impact.clone(),
                    datetime: dt,
                })
            })
            .collect();

        Self {
            raw_events: raw_events.to_vec(),
            parsed_events,
            windows: Vec::new(),
        }
    }

    /// Compile raw calendar events and worker configurations into blackout windows.
    ///
    /// Preserves worker isolation by deriving blackout windows per worker config
    /// rather than widening all workers to the global maximum buffers.
    #[must_use]
    pub fn compile(
        raw_events: &[RawCalendarEvent],
        configs: &[&RedFolderConfig],
        now: DateTime<Utc>,
    ) -> Self {
        let mut engine = Self::from_events(raw_events);

        if !configs.is_empty() {
            if configs.len() == 1 {
                engine.windows = engine.windows_for_config(configs[0], now);
            } else {
                let mut all_windows = Vec::new();
                for cfg in configs {
                    if cfg.enabled {
                        all_windows.extend(engine.windows_for_config(cfg, now));
                    }
                }
                all_windows.sort_by_key(|w| w.start);
                engine.windows = all_windows;
            }
        } else {
            let default_cfg = RedFolderConfig::default();
            engine.windows = engine.windows_for_config(&default_cfg, now);
        }

        info!(
            windows = %engine.windows.len(),
            events = %engine.parsed_events.len(),
            "compiled blackout engine"
        );
        engine
    }

    /// Access raw calendar events stored in the engine.
    #[must_use]
    pub fn raw_events(&self) -> &[RawCalendarEvent] {
        &self.raw_events
    }

    /// Access pre-parsed economic events stored in the engine.
    #[must_use]
    pub fn parsed_events(&self) -> &[EconomicEvent] {
        &self.parsed_events
    }

    /// Access reference precompiled windows.
    #[must_use]
    pub fn windows(&self) -> &[BlackoutWindow] {
        &self.windows
    }

    /// Whether the engine contains no events and no windows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw_events.is_empty() && self.windows.is_empty()
    }

    /// Derives worker-specific blackout windows at runtime using the worker's own
    /// timing buffers (`before_min`, `after_min`), merge threshold, currencies, impacts,
    /// and weekend curfew configuration.
    #[must_use]
    pub fn windows_for_config(
        &self,
        config: &RedFolderConfig,
        now: DateTime<Utc>,
    ) -> Vec<BlackoutWindow> {
        if !config.enabled {
            return Vec::new();
        }

        let before_min = config.before_min;
        let after_min = config.after_min;
        let cutoff = now + Duration::hours(48);
        let lower_cutoff = now - Duration::minutes(after_min);

        let mut individual: Vec<(DateTime<Utc>, DateTime<Utc>, WindowEvent)> = Vec::new();

        // 1. Process parsed economic releases matching this worker config
        for event in &self.parsed_events {
            if event.datetime < lower_cutoff || event.datetime > cutoff {
                continue;
            }

            if event_matches_economic_event(event, config) {
                let start = event.datetime - Duration::minutes(before_min);
                let end = event.datetime + Duration::minutes(after_min);
                individual.push((
                    start,
                    end,
                    WindowEvent {
                        is_custom: false,
                        event_time: event.datetime,
                        country: event.country.clone(),
                        impact: event.impact.clone(),
                        title: event.title.clone(),
                    },
                ));
            }
        }

        // 2. Add weekend curfew if enabled for this worker
        if config.weekend_enabled {
            if let Ok((start, end)) =
                next_weekend_window(&config.weekend_start, &config.weekend_end, &config.weekend_mode)
            {
                if end >= now && start <= cutoff {
                    let title = weekend_window_title(
                        &config.weekend_start,
                        &config.weekend_end,
                        &config.weekend_mode,
                    );
                    individual.push((
                        start,
                        end,
                        WindowEvent {
                            is_custom: true,
                            event_time: start,
                            country: "Global".into(),
                            impact: "High".into(),
                            title,
                        },
                    ));
                }
            }
        }

        // 3. Sort individual windows by start time
        individual.sort_by_key(|(start, _, _)| *start);

        // 4. Merge overlapping or threshold-adjacent windows using worker's merge threshold
        let merge_gap = Duration::minutes(config.merge_threshold_min);
        let mut merged: Vec<BlackoutWindow> = Vec::new();

        for (start, end, event) in individual {
            match merged.last_mut() {
                Some(last) if (start - last.end) <= merge_gap => {
                    if end > last.end {
                        last.end = end;
                    }
                    last.events.push(event);
                }
                _ => merged.push(BlackoutWindow {
                    start,
                    end,
                    events: vec![event],
                }),
            }
        }

        merged
    }

    /// Checks if a blackout is currently active for the given configuration.
    #[must_use]
    pub fn is_blackout(&self, config: &RedFolderConfig) -> bool {
        self.is_blackout_at(config, Utc::now())
    }

    /// Checks if a blackout was active at a specific point in time for the given configuration.
    #[must_use]
    pub fn is_blackout_at(&self, config: &RedFolderConfig, time: DateTime<Utc>) -> bool {
        if !config.enabled {
            return false;
        }
        let windows = self.windows_for_config(config, time);
        windows.iter().any(|w| w.is_active_at(time))
    }

    /// Returns the active `BlackoutWindow` matching the configuration, if any.
    #[must_use]
    pub fn current_window(&self, config: &RedFolderConfig) -> Option<BlackoutWindow> {
        self.current_window_at(config, Utc::now())
    }

    /// Returns the active `BlackoutWindow` matching the configuration at a specific timestamp, if any.
    #[must_use]
    pub fn current_window_at(
        &self,
        config: &RedFolderConfig,
        time: DateTime<Utc>,
    ) -> Option<BlackoutWindow> {
        if !config.enabled {
            return None;
        }
        let windows = self.windows_for_config(config, time);
        windows.into_iter().find(|w| w.is_active_at(time))
    }

    /// Returns upcoming blackout windows within `hours` hours matching the configuration.
    #[must_use]
    pub fn upcoming_blackouts(&self, config: &RedFolderConfig, hours: u32) -> Vec<BlackoutWindow> {
        let now = Utc::now();
        let cutoff = now + Duration::hours(hours as i64);
        let windows = self.windows_for_config(config, now);

        windows
            .into_iter()
            .filter(|w| w.start >= now && w.start <= cutoff)
            .collect()
    }
}

/// Evaluates whether any window in `windows` is active for `config` at `at`.
#[must_use]
pub fn is_blackout_for_config(
    config: &RedFolderConfig,
    windows: &[BlackoutWindow],
    at: DateTime<Utc>,
) -> bool {
    if !config.enabled {
        return false;
    }

    for window in windows {
        if window.is_active_at(at) {
            for event in &window.events {
                if event_matches_config(event, config) {
                    return true;
                }
            }
        }
    }
    false
}

/// Returns the matching `BlackoutWindow` active at `at` for `config`, if any.
#[must_use]
pub fn current_window_for_config(
    config: &RedFolderConfig,
    windows: &[BlackoutWindow],
    at: DateTime<Utc>,
) -> Option<BlackoutWindow> {
    if !config.enabled {
        return None;
    }

    for window in windows {
        if window.is_active_at(at) {
            let matching: Vec<WindowEvent> = window
                .events
                .iter()
                .filter(|e| event_matches_config(e, config))
                .cloned()
                .collect();

            if !matching.is_empty() {
                return Some(BlackoutWindow {
                    start: window.start,
                    end: window.end,
                    events: matching,
                });
            }
        }
    }
    None
}

/// Checks if an `EconomicEvent` matches a given worker `RedFolderConfig`.
#[must_use]
pub fn event_matches_economic_event(event: &EconomicEvent, config: &RedFolderConfig) -> bool {
    let currency_match = event.country.eq_ignore_ascii_case("All")
        || event.country.eq_ignore_ascii_case("Global")
        || config
            .currencies
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&event.country) || c.eq_ignore_ascii_case("All"));
    let impact_match = config
        .impacts
        .iter()
        .any(|i| i.eq_ignore_ascii_case(&event.impact));

    currency_match && impact_match
}

/// Checks if a single `WindowEvent` matches a given worker `RedFolderConfig`.
#[must_use]
pub fn event_matches_config(event: &WindowEvent, config: &RedFolderConfig) -> bool {
    if event.is_custom {
        config.weekend_enabled
    } else {
        let currency_match = event.country.eq_ignore_ascii_case("All")
            || event.country.eq_ignore_ascii_case("Global")
            || config
                .currencies
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&event.country) || c.eq_ignore_ascii_case("All"));
        let impact_match = config
            .impacts
            .iter()
            .any(|i| i.eq_ignore_ascii_case(&event.impact));

        currency_match && impact_match
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlapping_event_merging() {
        let now = Utc::now();
        let raw = vec![
            RawCalendarEvent {
                title: "US CPI".into(),
                country: "USD".into(),
                date: (now + Duration::minutes(60)).to_rfc3339(),
                time: "".into(),
                impact: "High".into(),
            },
            RawCalendarEvent {
                title: "FOMC Rate Decision".into(),
                country: "USD".into(),
                date: (now + Duration::minutes(90)).to_rfc3339(),
                time: "".into(),
                impact: "High".into(),
            },
        ];

        let config = RedFolderConfig {
            weekend_enabled: false,
            ..Default::default()
        };
        let engine = BlackoutEngine::compile(&raw, &[&config], now);

        assert_eq!(engine.windows().len(), 1);
        let merged = &engine.windows()[0];
        assert_eq!(merged.events.len(), 2);
        assert_eq!(merged.duration_minutes(), 90);
    }

    #[test]
    fn test_currency_and_impact_filtering() {
        let now = Utc::now();
        let window = BlackoutWindow {
            start: now - Duration::minutes(10),
            end: now + Duration::minutes(10),
            events: vec![WindowEvent {
                is_custom: false,
                event_time: now,
                country: "USD".into(),
                impact: "High".into(),
                title: "USD NFP".into(),
            }],
        };

        let usd_config = RedFolderConfig::builder()
            .currencies(vec!["USD"])
            .impacts(vec!["High"])
            .build();
        let eur_config = RedFolderConfig::builder()
            .currencies(vec!["EUR"])
            .impacts(vec!["High"])
            .build();

        assert!(is_blackout_for_config(
            &usd_config,
            std::slice::from_ref(&window),
            now
        ));
        assert!(!is_blackout_for_config(&eur_config, &[window], now));
    }

    #[test]
    fn test_worker_specific_buffers_isolation() {
        let now = Utc::now();
        let event_time = now + Duration::minutes(15);
        let raw = vec![RawCalendarEvent {
            title: "US Non-Farm Payrolls".into(),
            country: "USD".into(),
            date: event_time.to_rfc3339(),
            time: "".into(),
            impact: "High".into(),
        }];

        // Scalper: 5 min before, 5 min after
        let scalper = RedFolderConfig::builder()
            .currencies(vec!["USD"])
            .impacts(vec!["High"])
            .buffer_minutes(5, 5)
            .weekend_curfew(false, "20:00", "21:00", "short")
            .build();

        // Swing: 30 min before, 30 min after
        let swing = RedFolderConfig::builder()
            .currencies(vec!["USD"])
            .impacts(vec!["High"])
            .buffer_minutes(30, 30)
            .weekend_curfew(false, "20:00", "21:00", "short")
            .build();

        let engine = BlackoutEngine::compile(&raw, &[&scalper, &swing], now);

        // At T=now (15 mins before event):
        // Scalper (5m buffer) must NOT be in blackout!
        assert!(!engine.is_blackout_at(&scalper, now));
        // Swing (30m buffer) MUST be in blackout!
        assert!(engine.is_blackout_at(&swing, now));

        // At T = event_time - 3 mins (3 mins before event):
        // Both workers should now be in blackout!
        let three_mins_before = event_time - Duration::minutes(3);
        assert!(engine.is_blackout_at(&scalper, three_mins_before));
        assert!(engine.is_blackout_at(&swing, three_mins_before));

        // At T = event_time + 7 mins (7 mins after event):
        // Scalper's 5 min buffer has expired -> FALSE
        // Swing's 30 min buffer is still active -> TRUE
        let seven_mins_after = event_time + Duration::minutes(7);
        assert!(!engine.is_blackout_at(&scalper, seven_mins_after));
        assert!(engine.is_blackout_at(&swing, seven_mins_after));

        // At T = event_time + 35 mins:
        // Neither is in blackout
        let thirty_five_mins_after = event_time + Duration::minutes(35);
        assert!(!engine.is_blackout_at(&scalper, thirty_five_mins_after));
        assert!(!engine.is_blackout_at(&swing, thirty_five_mins_after));
    }

    #[test]
    fn test_weekend_and_news_overlap() {
        // Find upcoming Friday 20:15 UTC (15 mins before weekend curfew at 20:30 UTC)
        let (curfew_start, _curfew_end) =
            next_weekend_window("20:30", "21:00", "weekend").unwrap();
        let news_time = curfew_start - Duration::minutes(15);

        let raw = vec![RawCalendarEvent {
            title: "Federal Budget Balance".into(),
            country: "USD".into(),
            date: news_time.to_rfc3339(),
            time: "".into(),
            impact: "High".into(),
        }];

        let config = RedFolderConfig::builder()
            .currencies(vec!["USD"])
            .impacts(vec!["High"])
            .buffer_minutes(30, 30) // News blackout: curfew_start - 45m to curfew_start + 15m
            .merge_threshold(30)
            .weekend_curfew(true, "20:30", "21:00", "weekend")
            .build();

        let engine = BlackoutEngine::compile(&raw, &[&config], news_time - Duration::hours(1));
        let windows = engine.windows_for_config(&config, news_time - Duration::hours(1));

        // News window and curfew window overlap (curfew starts at 20:30, while news window ends at 20:45)
        // They must merge cleanly into a single continuous window
        assert_eq!(windows.len(), 1);
        let merged = &windows[0];
        // Start should be news start (news_time - 30m)
        assert_eq!(merged.start, news_time - Duration::minutes(30));
        // And window should contain both news and curfew events
        assert!(merged.events.len() >= 2);
    }
}
