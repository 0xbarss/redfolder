use crate::curfew::WeekendMode;
use crate::types::{Currency, FailSafeMode, Impact};
use serde::{Deserialize, Serialize};

/// Configuration for economic news blackout filtering and weekend curfew windows.
///
/// Fields are strongly typed with [`Currency`], [`Impact`], and [`WeekendMode`],
/// while supporting transparent Serde deserialization from JSON/TOML strings and legacy aliases.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RedFolderConfig {
    /// Whether blackout checking is globally enabled for this worker/strategy.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Currencies to monitor (e.g. `[Currency::USD, Currency::EUR]` or `[Currency::All]`).
    #[serde(default = "default_currencies")]
    pub currencies: Vec<Currency>,

    /// Impact levels to filter on (e.g. `[Impact::High]` or `[Impact::High, Impact::Medium]`).
    #[serde(default = "default_impacts")]
    pub impacts: Vec<Impact>,

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
    ///
    /// **Important**: When enabled, the weekend curfew window applies globally to this worker
    /// as a synthetic market-close blackout (`WindowEvent::is_custom == true`), intentionally
    /// bypassing currency and impact filters. Even if a worker configures specific currency
    /// or impact restrictions (e.g. only `"Medium"` impact), the weekend curfew will still
    /// enforce a blackout during Friday close if `weekend_enabled` is `true`.
    #[serde(default = "default_true", alias = "friday_night_enabled")]
    pub weekend_enabled: bool,

    /// Start time for weekend curfew in UTC (e.g. `"20:30"`).
    #[serde(default = "default_weekend_start", alias = "friday_night_start")]
    pub weekend_start: String,

    /// End time for weekend curfew in UTC (e.g. `"21:00"`).
    #[serde(default = "default_weekend_end", alias = "friday_night_end")]
    pub weekend_end: String,

    /// Curfew mode:
    /// - [`WeekendMode::Short`]: curfew window ends at `weekend_end` on Friday.
    /// - [`WeekendMode::Weekend`]: curfew window extends throughout the weekend until Monday 00:00 UTC.
    #[serde(default = "default_weekend_mode", alias = "friday_night_mode")]
    pub weekend_mode: WeekendMode,

    /// Optional advance warning in minutes before a blackout window starts.
    /// If configured, `RedFolderEvent::BlackoutWarning` will be emitted in advance.
    #[serde(default)]
    pub warning_before_min: Option<i64>,

    /// Whether all-day events create a 24-hour blackout window (00:00 to 24:00 UTC).
    /// Defaults to `false` to avoid inaccurate midnight spikes for economic holidays.
    #[serde(default)]
    pub include_all_day: bool,

    /// Whether tentative events create a blackout window.
    /// Defaults to `false` to avoid premature blackout timing for unconfirmed releases.
    #[serde(default)]
    pub include_tentative: bool,

    /// Policy governing trading blackout behavior when calendar data is unavailable, empty, or stale.
    ///
    /// - [`FailSafeMode::FailOpen`]: Treats data outages permissively (assumes no blackout, continues trading).
    /// - [`FailSafeMode::FailClosed`]: Treats data outages defensively (assumes blackout active, halts trading).
    ///
    /// Defaults to [`FailSafeMode::FailOpen`] for standard setups; prop firm presets default to `FailClosed`.
    #[serde(default)]
    pub fail_safe_mode: FailSafeMode,
}

fn default_true() -> bool {
    true
}

fn default_30() -> i64 {
    30
}

fn default_currencies() -> Vec<Currency> {
    vec![Currency::USD]
}

fn default_impacts() -> Vec<Impact> {
    vec![Impact::High]
}

fn default_weekend_start() -> String {
    "20:30".to_string()
}

fn default_weekend_end() -> String {
    "21:00".to_string()
}

fn default_weekend_mode() -> WeekendMode {
    WeekendMode::Short
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
            include_all_day: false,
            include_tentative: false,
            fail_safe_mode: FailSafeMode::FailOpen,
        }
    }
}

impl RedFolderConfig {
    /// Create a new configuration builder.
    #[must_use]
    pub fn builder() -> RedFolderConfigBuilder {
        RedFolderConfigBuilder::default()
    }

    /// Preset tailored for prop firm trading challenges (FTMO, FundedNext, MFF):
    /// Strict 5 minutes before and after high-impact news, plus full weekend curfew until Monday.
    /// Uses [`FailSafeMode::FailClosed`] to halt trading on data feed outages.
    #[must_use]
    pub fn prop_firm_strict() -> Self {
        Self {
            enabled: true,
            currencies: vec![
                Currency::USD,
                Currency::EUR,
                Currency::GBP,
                Currency::JPY,
                Currency::CAD,
                Currency::AUD,
                Currency::NZD,
                Currency::CHF,
            ],
            impacts: vec![Impact::High],
            before_min: 5,
            after_min: 5,
            merge_threshold_min: 15,
            weekend_enabled: true,
            weekend_start: "20:00".into(),
            weekend_end: "21:00".into(),
            weekend_mode: WeekendMode::Weekend,
            warning_before_min: Some(15),
            include_all_day: false,
            include_tentative: false,
            fail_safe_mode: FailSafeMode::FailClosed,
        }
    }

    /// Preset with conservative 30-minute buffers for high and medium impact releases.
    /// Uses [`FailSafeMode::FailClosed`] to halt trading on data feed outages.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            enabled: true,
            currencies: vec![Currency::USD, Currency::EUR, Currency::GBP],
            impacts: vec![Impact::High, Impact::Medium],
            before_min: 30,
            after_min: 30,
            merge_threshold_min: 30,
            weekend_enabled: true,
            weekend_start: "20:30".into(),
            weekend_end: "21:00".into(),
            weekend_mode: WeekendMode::Weekend,
            warning_before_min: Some(30),
            include_all_day: false,
            include_tentative: false,
            fail_safe_mode: FailSafeMode::FailClosed,
        }
    }

    /// Validates the configuration parameters, ensuring non-negative buffers, non-empty filters,
    /// non-blank currency/impact entries, and valid curfew formats.
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.currencies.is_empty() {
            return Err(crate::error::RedFolderError::Config(
                "currencies filter cannot be empty; specify at least one currency or 'All'"
                    .to_string(),
            ));
        }
        for c in &self.currencies {
            if c.as_str().trim().is_empty() {
                return Err(crate::error::RedFolderError::Config(
                    "currency filter cannot contain empty or blank strings".to_string(),
                ));
            }
        }
        if self.impacts.is_empty() {
            return Err(crate::error::RedFolderError::Config(
                "impacts filter cannot be empty; specify at least one impact level (e.g. 'High')"
                    .to_string(),
            ));
        }
        for i in &self.impacts {
            if i.to_string().trim().is_empty() {
                return Err(crate::error::RedFolderError::Config(
                    "impact filter cannot contain empty or blank strings".to_string(),
                ));
            }
        }
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

        // Unconditionally validate weekend curfew time formats even if weekend_enabled is false
        crate::curfew::parse_time(&self.weekend_start)?;
        if self.weekend_mode == WeekendMode::Short || !self.weekend_end.is_empty() {
            crate::curfew::parse_time(&self.weekend_end)?;
        }
        Ok(())
    }

    /// Performs strict validation, verifying all standard configuration rules and ensuring
    /// that only standard known currency codes (USD, EUR, GBP, JPY, AUD, CAD, CHF, NZD, CNY, ALL)
    /// and standard impact levels (High, Medium, Low, Non-Economic) are accepted.
    pub fn validate_strict(&self) -> crate::error::Result<()> {
        self.validate()?;

        for c in &self.currencies {
            if let Currency::Custom(ref s) = c {
                return Err(crate::error::RedFolderError::Config(format!(
                    "unknown non-standard currency '{}' rejected in strict mode",
                    s
                )));
            }
        }

        for i in &self.impacts {
            if let Impact::Custom(ref s) = i {
                return Err(crate::error::RedFolderError::Config(format!(
                    "unknown non-standard impact '{}' rejected in strict mode",
                    s
                )));
            }
        }

        Ok(())
    }

    /// Return configured currencies as a slice of typed [`Currency`] variants.
    #[must_use]
    pub fn typed_currencies(&self) -> &[Currency] {
        &self.currencies
    }

    /// Return configured impacts as a slice of typed [`Impact`] variants.
    #[must_use]
    pub fn typed_impacts(&self) -> &[Impact] {
        &self.impacts
    }

    /// Return configured weekend mode as a typed [`WeekendMode`].
    pub fn typed_weekend_mode(&self) -> crate::error::Result<WeekendMode> {
        Ok(self.weekend_mode)
    }

    /// Helper returning currencies as a list of strings.
    #[must_use]
    pub fn currencies_as_strings(&self) -> Vec<String> {
        self.currencies.iter().map(|c| c.to_string()).collect()
    }

    /// Helper returning impacts as a list of strings.
    #[must_use]
    pub fn impacts_as_strings(&self) -> Vec<String> {
        self.impacts.iter().map(|i| i.to_string()).collect()
    }
}

/// Helper trait to accept either `WeekendMode`, `&str`, or `String` in configuration builders.
pub trait IntoWeekendMode {
    fn into_weekend_mode(self) -> crate::error::Result<WeekendMode>;
}

impl IntoWeekendMode for WeekendMode {
    fn into_weekend_mode(self) -> crate::error::Result<WeekendMode> {
        Ok(self)
    }
}

impl IntoWeekendMode for &str {
    fn into_weekend_mode(self) -> crate::error::Result<WeekendMode> {
        self.parse()
    }
}

impl IntoWeekendMode for &String {
    fn into_weekend_mode(self) -> crate::error::Result<WeekendMode> {
        self.as_str().parse()
    }
}

impl IntoWeekendMode for String {
    fn into_weekend_mode(self) -> crate::error::Result<WeekendMode> {
        self.parse()
    }
}

/// Builder for `RedFolderConfig`.
#[derive(Debug, Default)]
pub struct RedFolderConfigBuilder {
    config: RedFolderConfig,
    custom_currencies: bool,
    custom_impacts: bool,
    mode_err: Option<String>,
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
        S: Into<Currency>,
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
        self.config.currencies.push(currency.into());
        self
    }

    #[must_use]
    pub fn impacts<I, S>(mut self, impacts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Impact>,
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
        self.config.impacts.push(impact.into());
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
        mode: impl IntoWeekendMode,
    ) -> Self {
        self.config.weekend_enabled = enabled;
        self.config.weekend_start = start_utc.into();
        self.config.weekend_end = end_utc.into();
        match mode.into_weekend_mode() {
            Ok(m) => {
                self.config.weekend_mode = m;
                self.mode_err = None;
            }
            Err(e) => {
                self.mode_err = Some(e.to_string());
            }
        }
        self
    }

    /// Sets the weekend curfew mode using a typed [`WeekendMode`].
    #[must_use]
    pub fn weekend_mode_typed(mut self, mode: WeekendMode) -> Self {
        self.config.weekend_mode = mode;
        self.mode_err = None;
        self
    }

    #[must_use]
    pub fn warning_minutes(mut self, minutes: i64) -> Self {
        self.config.warning_before_min = Some(minutes);
        self
    }

    #[must_use]
    pub fn include_all_day(mut self, include: bool) -> Self {
        self.config.include_all_day = include;
        self
    }

    #[must_use]
    pub fn include_tentative(mut self, include: bool) -> Self {
        self.config.include_tentative = include;
        self
    }

    /// Sets the policy for data loss or stale calendar handling ([`FailSafeMode`]).
    #[must_use]
    pub fn fail_safe_mode(mut self, mode: FailSafeMode) -> Self {
        self.config.fail_safe_mode = mode;
        self
    }

    /// Builds the configuration, panicking if parameters fail validation.
    #[must_use]
    pub fn build(self) -> RedFolderConfig {
        self.try_build().expect(
            "invalid RedFolderConfig parameters: use try_build() for fallible initialization",
        )
    }

    /// Validates and builds the configuration safely without panicking.
    pub fn try_build(self) -> crate::error::Result<RedFolderConfig> {
        if let Some(err) = self.mode_err {
            return Err(crate::error::RedFolderError::Curfew(err));
        }
        self.config.validate()?;
        Ok(self.config)
    }

    /// Validates strictly and builds the configuration safely without panicking.
    pub fn try_build_strict(self) -> crate::error::Result<RedFolderConfig> {
        if let Some(err) = self.mode_err {
            return Err(crate::error::RedFolderError::Curfew(err));
        }
        self.config.validate_strict()?;
        Ok(self.config)
    }

    /// Validates strictly and builds the configuration, panicking if strict validation fails.
    #[must_use]
    pub fn build_strict(self) -> RedFolderConfig {
        self.try_build_strict().expect(
            "invalid RedFolderConfig parameters in strict mode: use try_build_strict() for fallible initialization",
        )
    }
}

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

    #[test]
    fn test_semantic_validation_rejects_blank_items() {
        // Blank currency string in list must be rejected
        let bad_cur = RedFolderConfig::builder()
            .currencies(vec!["USD", "   "])
            .try_build();
        assert!(bad_cur.is_err());
        assert!(bad_cur
            .unwrap_err()
            .to_string()
            .contains("currency filter cannot contain empty or blank strings"));

        // Blank impact string in list must be rejected
        let bad_imp = RedFolderConfig::builder()
            .impacts(vec!["High", ""])
            .try_build();
        assert!(bad_imp.is_err());
        assert!(bad_imp
            .unwrap_err()
            .to_string()
            .contains("impact filter cannot contain empty or blank strings"));
    }

    #[test]
    fn test_validate_strict() {
        // Custom currency is valid in standard mode
        let custom_cfg = RedFolderConfig::builder()
            .currencies(vec!["USD", "XAU"])
            .impacts(vec!["High"])
            .build();
        assert!(
            custom_cfg.validate().is_ok(),
            "custom currency should be allowed in default mode"
        );

        // Custom currency is rejected in strict mode
        let strict_err = custom_cfg.validate_strict();
        assert!(
            strict_err.is_err(),
            "custom currency should be rejected in strict mode"
        );
        assert!(strict_err
            .unwrap_err()
            .to_string()
            .contains("rejected in strict mode"));

        // Standard-only config passes strict validation
        let standard_cfg = RedFolderConfig::builder()
            .currencies(vec!["USD", "EUR"])
            .impacts(vec!["High", "Medium"])
            .build();
        assert!(standard_cfg.validate_strict().is_ok());
    }

    #[test]
    fn test_builder_strict_methods() {
        let ok_cfg = RedFolderConfig::builder()
            .currencies(vec!["USD", "EUR"])
            .impacts(vec!["High"])
            .try_build_strict();
        assert!(ok_cfg.is_ok());

        let err_cfg = RedFolderConfig::builder()
            .currencies(vec!["USDD"])
            .impacts(vec!["High"])
            .try_build_strict();
        assert!(err_cfg.is_err());
        assert!(err_cfg
            .unwrap_err()
            .to_string()
            .contains("unknown non-standard currency 'USDD'"));

        let built = RedFolderConfig::builder()
            .currencies(vec!["GBP"])
            .impacts(vec!["High"])
            .build_strict();
        assert_eq!(built.currencies, vec!["GBP"]);
    }

    #[test]
    fn test_typed_helpers() {
        let cfg = RedFolderConfig::builder()
            .currencies(vec!["USD", "EUR"])
            .impacts(vec!["High", "Medium"])
            .weekend_mode_typed(crate::curfew::WeekendMode::Weekend)
            .build();

        assert_eq!(cfg.typed_currencies(), vec![Currency::USD, Currency::EUR]);
        assert_eq!(cfg.typed_impacts(), vec![Impact::High, Impact::Medium]);
        assert_eq!(
            cfg.typed_weekend_mode().unwrap(),
            crate::curfew::WeekendMode::Weekend
        );
    }

    #[test]
    fn test_weekend_validation_when_disabled() {
        // Bad weekend start time should fail even if weekend_enabled == false
        let bad_start = RedFolderConfig::builder()
            .weekend_curfew(false, "99:99", "21:00", "short")
            .try_build();
        assert!(
            bad_start.is_err(),
            "invalid start time must fail validation even when weekend_enabled is false"
        );

        // Bad weekend mode should fail even if weekend_enabled == false
        let bad_mode = RedFolderConfig::builder()
            .weekend_curfew(false, "20:00", "21:00", "bogus_mode")
            .try_build();
        assert!(
            bad_mode.is_err(),
            "invalid weekend mode must fail validation even when weekend_enabled is false"
        );

        // Bad weekend end time in short mode should fail even if weekend_enabled == false
        let bad_end = RedFolderConfig::builder()
            .weekend_curfew(false, "20:00", "25:99", "short")
            .try_build();
        assert!(
            bad_end.is_err(),
            "invalid end time must fail validation even when weekend_enabled is false"
        );
    }

    #[test]
    fn test_fail_safe_mode_config() {
        let default_cfg = RedFolderConfig::default();
        assert_eq!(
            default_cfg.fail_safe_mode,
            crate::types::FailSafeMode::FailOpen
        );

        let prop_firm = RedFolderConfig::prop_firm_strict();
        assert_eq!(
            prop_firm.fail_safe_mode,
            crate::types::FailSafeMode::FailClosed
        );

        let conservative = RedFolderConfig::conservative();
        assert_eq!(
            conservative.fail_safe_mode,
            crate::types::FailSafeMode::FailClosed
        );

        let custom = RedFolderConfig::builder()
            .fail_safe_mode(crate::types::FailSafeMode::FailClosed)
            .build();
        assert_eq!(
            custom.fail_safe_mode,
            crate::types::FailSafeMode::FailClosed
        );
    }
}
