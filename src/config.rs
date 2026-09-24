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

    /// Optional advance warning in minutes before a blackout window starts.
    /// If configured, `RedFolderEvent::BlackoutWarning` will be emitted in advance.
    #[serde(default)]
    pub warning_before_min: Option<i64>,
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
            warning_before_min: None,
        }
    }
}

use crate::types::{Currency, Impact};

impl RedFolderConfig {
    /// Create a new configuration builder.
    #[must_use]
    pub fn builder() -> RedFolderConfigBuilder {
        RedFolderConfigBuilder::default()
    }

    /// Preset tailored for prop firm trading challenges (FTMO, FundedNext, MFF):
    /// Strict 5 minutes before and after high-impact news, plus full weekend curfew until Monday.
    #[must_use]
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
            warning_before_min: Some(15),
        }
    }

    /// Preset with conservative 30-minute buffers for high and medium impact releases.
    #[must_use]
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
            warning_before_min: Some(30),
        }
    }

    /// Validates the configuration parameters, ensuring non-negative buffers and valid curfew formats.
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.before_min < 0 {
            return Err(crate::error::RedFolderError::Config(format!(
                "before_min cannot be negative (got {})",
                self.before_min
            )));
        }
        if self.after_min < 0 {
            return Err(crate::error::RedFolderError::Config(format!(
                "after_min cannot be negative (got {})",
                self.after_min
            )));
        }
        if self.merge_threshold_min < 0 {
            return Err(crate::error::RedFolderError::Config(format!(
                "merge_threshold_min cannot be negative (got {})",
                self.merge_threshold_min
            )));
        }
        if let Some(warn) = self.warning_before_min {
            if warn < 0 {
                return Err(crate::error::RedFolderError::Config(format!(
                    "warning_before_min cannot be negative (got {})",
                    warn
                )));
            }
        }
        if self.weekend_enabled {
            crate::curfew::parse_time(&self.weekend_start)?;
            let mode: crate::curfew::WeekendMode = self.weekend_mode.parse()?;
            if mode == crate::curfew::WeekendMode::Short {
                crate::curfew::parse_time(&self.weekend_end)?;
            }
        }
        Ok(())
    }
}

/// Builder for `RedFolderConfig`.
#[derive(Debug, Default)]
pub struct RedFolderConfigBuilder {
    config: RedFolderConfig,
    custom_currencies: bool,
    custom_impacts: bool,
}

impl RedFolderConfigBuilder {
    #[must_use]
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    #[must_use]
    pub fn currencies<I, S>(mut self, currencies: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.currencies = currencies.into_iter().map(Into::into).collect();
        self.custom_currencies = true;
        self
    }

    #[must_use]
    pub fn currency(mut self, currency: impl Into<Currency>) -> Self {
        if !self.custom_currencies {
            self.config.currencies.clear();
            self.custom_currencies = true;
        }
        self.config.currencies.push(currency.into().to_string());
        self
    }

    #[must_use]
    pub fn impacts<I, S>(mut self, impacts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.impacts = impacts.into_iter().map(Into::into).collect();
        self.custom_impacts = true;
        self
    }

    #[must_use]
    pub fn impact(mut self, impact: impl Into<Impact>) -> Self {
        if !self.custom_impacts {
            self.config.impacts.clear();
            self.custom_impacts = true;
        }
        self.config.impacts.push(impact.into().to_string());
        self
    }

    #[must_use]
    pub fn buffer_minutes(mut self, before: i64, after: i64) -> Self {
        self.config.before_min = before;
        self.config.after_min = after;
        self
    }

    #[must_use]
    pub fn merge_threshold(mut self, minutes: i64) -> Self {
        self.config.merge_threshold_min = minutes;
        self
    }

    #[must_use]
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

    #[must_use]
    pub fn warning_minutes(mut self, minutes: i64) -> Self {
        self.config.warning_before_min = Some(minutes);
        self
    }

    /// Builds the configuration, panicking if parameters are invalid.
    #[must_use]
    pub fn build(self) -> RedFolderConfig {
        self.try_build()
            .expect("invalid RedFolderConfig parameters")
    }

    /// Validates and builds the configuration safely without panicking.
    pub fn try_build(self) -> crate::error::Result<RedFolderConfig> {
        self.config.validate()?;
        Ok(self.config)
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

    #[test]
    fn test_typed_builder() {
        use crate::curfew::WeekendMode;
        let cfg = RedFolderConfig::builder()
            .currency(Currency::USD)
            .currency(Currency::EUR)
            .impact(Impact::High)
            .buffer_minutes(15, 15)
            .weekend_curfew(true, "20:00", "21:00", WeekendMode::Weekend)
            .build();

        assert_eq!(cfg.currencies, vec!["USD", "EUR"]);
        assert_eq!(cfg.impacts, vec!["High"]);
        assert_eq!(cfg.weekend_mode, "weekend");
    }
}
