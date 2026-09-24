use crate::calendar::{parse_event_datetime, RawCalendarEvent};
use crate::config::RedFolderConfig;
use crate::curfew::{next_weekend_window, weekend_window_title};
use crate::types::{BlackoutWindow, WindowEvent};
use chrono::{DateTime, Duration, Utc};
use std::collections::HashSet;
use tracing::{debug, info};

/// In-memory engine that compiles raw calendar events and curfew rules into merged blackout windows.
#[derive(Debug, Clone, Default)]
pub struct BlackoutEngine {
    windows: Vec<BlackoutWindow>,
}

impl BlackoutEngine {
    /// Create an empty engine.
    pub fn new() -> Self {
        Self {
            windows: Vec::new(),
        }
    }

    /// Compile raw calendar events and worker configurations into merged blackout windows.
    pub fn compile(
        raw_events: &[RawCalendarEvent],
        configs: &[&RedFolderConfig],
        now: DateTime<Utc>,
    ) -> Self {
        // Collect union of currencies and impacts, and maximum timing parameters across all enabled configs
        let mut all_currencies: HashSet<String> = HashSet::new();
        let mut all_impacts: HashSet<String> = HashSet::new();
        let mut max_before = 0i64;
        let mut max_after = 0i64;
        let mut max_merge = 0i64;

        for cfg in configs {
            if !cfg.enabled {
                continue;
            }
            all_currencies.extend(cfg.currencies.iter().cloned());
            all_impacts.extend(cfg.impacts.iter().cloned());
            max_before = max_before.max(cfg.before_min);
            max_after = max_after.max(cfg.after_min);
            max_merge = max_merge.max(cfg.merge_threshold_min);
        }

        if all_currencies.is_empty() {
            all_currencies.insert("USD".into());
        }
        if all_impacts.is_empty() {
            all_impacts.insert("High".into());
        }

        let before_min = if max_before > 0 { max_before } else { 30 };
        let after_min = if max_after > 0 { max_after } else { 30 };
        let cutoff = now + Duration::hours(48);

        let mut individual: Vec<(DateTime<Utc>, DateTime<Utc>, WindowEvent)> = Vec::new();

        // 1. Process external economic calendar releases
        for raw in raw_events {
            let Some(event_dt) = parse_event_datetime(raw) else {
                continue;
            };

            // Only consider events from now up to 48 hours into the future
            if event_dt < now || event_dt > cutoff {
                continue;
            }

            let matches_currency = all_currencies
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&raw.country) || c.eq_ignore_ascii_case("All"));
            let matches_impact = all_impacts
                .iter()
                .any(|i| i.eq_ignore_ascii_case(&raw.impact));

            if matches_currency && matches_impact {
                let start = event_dt - Duration::minutes(before_min);
                let end = event_dt + Duration::minutes(after_min);
                individual.push((
                    start,
                    end,
                    WindowEvent {
                        is_custom: false,
                        event_time: event_dt,
                        country: raw.country.clone(),
                        impact: raw.impact.clone(),
                        title: raw.title.clone(),
                    },
                ));
            }
        }

        // 2. Add weekend curfew windows for configs where weekend protection is enabled
        let mut weekend_added = false;
        for cfg in configs {
            if cfg.enabled && cfg.weekend_enabled {
                if let Ok((start, end)) =
                    next_weekend_window(&cfg.weekend_start, &cfg.weekend_end, &cfg.weekend_mode)
                {
                    // Only add if within the cutoff or currently active
                    if end >= now && start <= cutoff {
                        let title = weekend_window_title(
                            &cfg.weekend_start,
                            &cfg.weekend_end,
                            &cfg.weekend_mode,
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
                        weekend_added = true;
                    }
                }
            }
        }

        if weekend_added {
            debug!("added weekend curfew blackout window");
        }

        // 3. Sort individual windows by start time
        individual.sort_by_key(|(start, _, _)| *start);

        // 4. Merge overlapping or threshold-adjacent windows
        let merge_gap = Duration::minutes(if max_merge > 0 { max_merge } else { 30 });
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

        info!(count = %merged.len(), "compiled and merged blackout windows");
        Self { windows: merged }
    }

    /// Access all compiled windows.
    pub fn windows(&self) -> &[BlackoutWindow] {
        &self.windows
    }

    /// Checks if a blackout is currently active for the given configuration.
    pub fn is_blackout(&self, config: &RedFolderConfig) -> bool {
        is_blackout_for_config(config, &self.windows, Utc::now())
    }

    /// Checks if a blackout was active at a specific point in time.
    pub fn is_blackout_at(&self, config: &RedFolderConfig, time: DateTime<Utc>) -> bool {
        is_blackout_for_config(config, &self.windows, time)
    }

    /// Returns the active `BlackoutWindow` matching the configuration, if any.
    pub fn current_window(&self, config: &RedFolderConfig) -> Option<BlackoutWindow> {
        current_window_for_config(config, &self.windows, Utc::now())
    }

    /// Returns upcoming blackout windows within `hours` hours matching the configuration.
    pub fn upcoming_blackouts(
        &self,
        config: &RedFolderConfig,
        hours: u32,
    ) -> Vec<BlackoutWindow> {
        let now = Utc::now();
        let cutoff = now + Duration::hours(hours as i64);

        self.windows
            .iter()
            .filter(|w| w.start >= now && w.start <= cutoff)
            .filter_map(|w| {
                let matching_events: Vec<WindowEvent> = w
                    .events
                    .iter()
                    .filter(|e| event_matches_config(e, config))
                    .cloned()
                    .collect();

                if matching_events.is_empty() {
                    None
                } else {
                    Some(BlackoutWindow {
                        start: w.start,
                        end: w.end,
                        events: matching_events,
                    })
                }
            })
            .collect()
    }
}

/// Evaluates whether any window in `windows` is active for `config` at `at`.
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

/// Checks if a single `WindowEvent` matches a given worker `RedFolderConfig`.
pub fn event_matches_config(event: &WindowEvent, config: &RedFolderConfig) -> bool {
    if event.is_custom {
        config.weekend_enabled
    } else {
        let currency_match = config.currencies.iter().any(|c| {
            c.eq_ignore_ascii_case(&event.country) || c.eq_ignore_ascii_case("All")
        });
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

        assert!(is_blackout_for_config(&usd_config, &[window.clone()], now));
        assert!(!is_blackout_for_config(&eur_config, &[window], now));
    }
}
