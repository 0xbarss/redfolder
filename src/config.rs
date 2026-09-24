use serde::{Deserialize, Serialize};

/// Configuration for economic news blackout filtering and weekend curfew windows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RedFolderConfig {
    /// Whether blackout checking is globally enabled for this worker/strategy.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Currencies to monitor (e.g. `["USD", "EUR", "GBP"]` or `["All"]`).
    #[serde(default = "default_currencies")]
    pub currencies: Vec<String>,

    /// Impact levels to filter on (e.g. `["High"]` or `["High", "Medium"]`).
    #[serde(default = "default_impacts")]
    pub impacts: Vec<String>,

    /// Minutes before the scheduled event time to initiate the blackout window.
    #[serde(default = "default_30")]
    pub before_min: i64,

    /// Minutes after the scheduled event time to maintain the blackout window.
    #[serde(default = "default_30")]
    pub after_min: i64,

    /// Threshold in minutes to merge closely spaced blackout windows into a single window.
    #[serde(default = "default_30")]
    pub merge_threshold_min: i64,

    /// Whether weekend curfew / market close protection is enabled.
    #[serde(default = "default_true", alias = "friday_night_enabled")]
    pub weekend_enabled: bool,

    /// Start time for weekend curfew in UTC (e.g. `"20:30"`).
    #[serde(default = "default_weekend_start", alias = "friday_night_start")]
    pub weekend_start: String,

    /// End time for weekend curfew in UTC (e.g. `"21:00"`).
    #[serde(default = "default_weekend_end", alias = "friday_night_end")]
    pub weekend_end: String,

    /// Curfew mode:
    /// - `"short"` (default): curfew window ends at `weekend_end` on Friday.
    /// - `"weekend"`: curfew window extends throughout the weekend until Monday 00:00 UTC.
    #[serde(default = "default_weekend_mode", alias = "friday_night_mode")]
    pub weekend_mode: String,
}

fn default_true() -> bool {
    true
}

fn default_30() -> i64 {
    30
}

fn default_currencies() -> Vec<String> {
    vec!["USD".into()]
}

fn default_impacts() -> Vec<String> {
    vec!["High".into()]
}

fn default_weekend_start() -> String {
    "20:30".to_string()
}

fn default_weekend_end() -> String {
    "21:00".to_string()
}

fn default_weekend_mode() -> String {
    "short".to_string()
}

impl Default for RedFolderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            currencies: default_currencies(),
            impacts: default_impacts(),
            before_min: 30,
            after_min: 30,
            merge_threshold_min: 30,
            weekend_enabled: true,
            weekend_start: default_weekend_start(),
            weekend_end: default_weekend_end(),
            weekend_mode: default_weekend_mode(),
        }
    }
}

impl RedFolderConfig {
    /// Create a new configuration builder.
    pub fn builder() -> RedFolderConfigBuilder {
        RedFolderConfigBuilder::default()
    }

    /// Preset tailored for prop firm trading challenges (FTMO, FundedNext, MFF):
    /// Strict 5 minutes before and after high-impact news, plus full weekend curfew until Monday.
    pub fn prop_firm_strict() -> Self {
        Self {
            enabled: true,
            currencies: vec![
                "USD".into(),
                "EUR".into(),
                "GBP".into(),
                "JPY".into(),
                "CAD".into(),
                "AUD".into(),
                "NZD".into(),
                "CHF".into(),
            ],
            impacts: vec!["High".into()],
            before_min: 5,
            after_min: 5,
            merge_threshold_min: 15,
            weekend_enabled: true,
            weekend_start: "20:00".into(),
            weekend_end: "21:00".into(),
            weekend_mode: "weekend".into(),
        }
    }

    /// Preset with conservative 30-minute buffers for high and medium impact releases.
    pub fn conservative() -> Self {
        Self {
            enabled: true,
            currencies: vec!["USD".into(), "EUR".into(), "GBP".into()],
            impacts: vec!["High".into(), "Medium".into()],
            before_min: 30,
            after_min: 30,
            merge_threshold_min: 30,
            weekend_enabled: true,
            weekend_start: "20:30".into(),
            weekend_end: "21:00".into(),
            weekend_mode: "weekend".into(),
        }
    }
}

/// Builder for `RedFolderConfig`.
#[derive(Debug, Default)]
pub struct RedFolderConfigBuilder {
    config: RedFolderConfig,
}

impl RedFolderConfigBuilder {
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    pub fn currencies<I, S>(mut self, currencies: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.currencies = currencies.into_iter().map(Into::into).collect();
        self
    }

    pub fn impacts<I, S>(mut self, impacts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.impacts = impacts.into_iter().map(Into::into).collect();
        self
    }

    pub fn buffer_minutes(mut self, before: i64, after: i64) -> Self {
        self.config.before_min = before;
        self.config.after_min = after;
        self
    }

    pub fn merge_threshold(mut self, minutes: i64) -> Self {
        self.config.merge_threshold_min = minutes;
        self
    }

    pub fn weekend_curfew(
        mut self,
        enabled: bool,
        start_utc: impl Into<String>,
        end_utc: impl Into<String>,
        mode: impl Into<String>,
    ) -> Self {
        self.config.weekend_enabled = enabled;
        self.config.weekend_start = start_utc.into();
        self.config.weekend_end = end_utc.into();
        self.config.weekend_mode = mode.into();
        self
    }

    pub fn build(self) -> RedFolderConfig {
        self.config
    }
}

/// Backwards compatibility alias for `RedFolderConfig`.
pub type NewsConfig = RedFolderConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let cfg = RedFolderConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.currencies, vec!["USD"]);
        assert_eq!(cfg.impacts, vec!["High"]);
        assert_eq!(cfg.before_min, 30);
        assert_eq!(cfg.after_min, 30);
        assert!(cfg.weekend_enabled);
        assert_eq!(cfg.weekend_mode, "short");
    }

    #[test]
    fn test_deserialization_with_legacy_aliases() {
        // Test that legacy JSON keys (with friday_night) deserialize cleanly into weekend fields
        let legacy_json = r#"{
            "enabled": true,
            "friday_night_enabled": true,
            "friday_night_start": "20:00",
            "friday_night_end": "22:00",
            "friday_night_mode": "weekend"
        }"#;

        let cfg: RedFolderConfig = serde_json::from_str(legacy_json).unwrap();
        assert!(cfg.weekend_enabled);
        assert_eq!(cfg.weekend_start, "20:00");
        assert_eq!(cfg.weekend_end, "22:00");
        assert_eq!(cfg.weekend_mode, "weekend");
    }

    #[test]
    fn test_builder() {
        let cfg = RedFolderConfig::builder()
            .currencies(vec!["EUR", "USD"])
            .impacts(vec!["High"])
            .buffer_minutes(10, 10)
            .weekend_curfew(true, "19:00", "23:00", "weekend")
            .build();

        assert_eq!(cfg.currencies, vec!["EUR", "USD"]);
        assert_eq!(cfg.before_min, 10);
        assert_eq!(cfg.after_min, 10);
        assert_eq!(cfg.weekend_start, "19:00");
    }
}
