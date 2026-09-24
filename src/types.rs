use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Currency code representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Currency {
    #[serde(alias = "usd")]
    USD,
    #[serde(alias = "eur")]
    EUR,
    #[serde(alias = "gbp")]
    GBP,
    #[serde(alias = "jpy")]
    JPY,
    #[serde(alias = "aud")]
    AUD,
    #[serde(alias = "cad")]
    CAD,
    #[serde(alias = "chf")]
    CHF,
    #[serde(alias = "nzd")]
    NZD,
    #[serde(alias = "cny")]
    CNY,
    #[serde(alias = "all", alias = "ALL", alias = "Global", alias = "global")]
    All,
    #[serde(untagged)]
    Custom(String),
}

impl Currency {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Currency::USD => "USD",
            Currency::EUR => "EUR",
            Currency::GBP => "GBP",
            Currency::JPY => "JPY",
            Currency::AUD => "AUD",
            Currency::CAD => "CAD",
            Currency::CHF => "CHF",
            Currency::NZD => "NZD",
            Currency::CNY => "CNY",
            Currency::All => "All",
            Currency::Custom(s) => s.as_str(),
        }
    }

    #[must_use]
    pub fn matches_str(&self, text: &str) -> bool {
        match self {
            Currency::All => true,
            Currency::Custom(s) => {
                s.eq_ignore_ascii_case(text)
                    || text.eq_ignore_ascii_case("all")
                    || text.eq_ignore_ascii_case("global")
            }
            standard => {
                standard.as_str().eq_ignore_ascii_case(text)
                    || text.eq_ignore_ascii_case("all")
                    || text.eq_ignore_ascii_case("global")
            }
        }
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for Currency {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        Ok(match trimmed.to_uppercase().as_str() {
            "USD" => Currency::USD,
            "EUR" => Currency::EUR,
            "GBP" => Currency::GBP,
            "JPY" => Currency::JPY,
            "AUD" => Currency::AUD,
            "CAD" => Currency::CAD,
            "CHF" => Currency::CHF,
            "NZD" => Currency::NZD,
            "CNY" => Currency::CNY,
            "ALL" | "GLOBAL" => Currency::All,
            _ => Currency::Custom(trimmed.to_string()),
        })
    }
}

impl From<&str> for Currency {
    fn from(s: &str) -> Self {
        s.parse().unwrap()
    }
}

impl From<String> for Currency {
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}

impl From<Currency> for String {
    fn from(c: Currency) -> Self {
        c.to_string()
    }
}

impl AsRef<str> for Currency {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Impact severity level of an economic event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Impact {
    #[serde(rename = "Non-Economic", alias = "None", alias = "Holiday")]
    NonEconomic,
    #[serde(rename = "Low", alias = "low")]
    Low,
    #[serde(rename = "Medium", alias = "medium", alias = "Med")]
    Medium,
    #[serde(rename = "High", alias = "high", alias = "Red")]
    High,
    #[serde(untagged)]
    Custom(String),
}

impl Impact {
    #[must_use]
    pub fn is_high(&self) -> bool {
        matches!(self, Impact::High)
    }

    #[must_use]
    pub fn is_red_folder(&self) -> bool {
        self.is_high()
    }

    #[must_use]
    pub fn matches_str(&self, text: &str) -> bool {
        match self {
            Impact::High => text.eq_ignore_ascii_case("High") || text.eq_ignore_ascii_case("Red"),
            Impact::Medium => {
                text.eq_ignore_ascii_case("Medium") || text.eq_ignore_ascii_case("Med")
            }
            Impact::Low => text.eq_ignore_ascii_case("Low"),
            Impact::NonEconomic => {
                text.eq_ignore_ascii_case("Non-Economic")
                    || text.eq_ignore_ascii_case("None")
                    || text.eq_ignore_ascii_case("Holiday")
            }
            Impact::Custom(s) => s.eq_ignore_ascii_case(text),
        }
    }
}

impl From<Impact> for String {
    fn from(i: Impact) -> Self {
        i.to_string()
    }
}

impl From<&str> for Impact {
    fn from(s: &str) -> Self {
        s.parse().unwrap()
    }
}

impl fmt::Display for Impact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Impact::High => write!(f, "High"),
            Impact::Medium => write!(f, "Medium"),
            Impact::Low => write!(f, "Low"),
            Impact::NonEconomic => write!(f, "Non-Economic"),
            Impact::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl FromStr for Impact {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.trim().to_lowercase().as_str() {
            "high" | "red" => Impact::High,
            "medium" | "med" => Impact::Medium,
            "low" => Impact::Low,
            "non-economic" | "none" | "holiday" => Impact::NonEconomic,
            other => Impact::Custom(other.to_string()),
        })
    }
}

/// A parsed economic calendar event from the calendar feed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EconomicEvent {
    pub title: String,
    pub country: String,
    pub impact: String,
    pub datetime: DateTime<Utc>,
}

/// Backwards compatibility alias for `EconomicEvent`.
pub type NewsEvent = EconomicEvent;

/// An individual event recorded within a merged blackout window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WindowEvent {
    /// True if generated by a weekend curfew or custom rule rather than external calendar API.
    pub is_custom: bool,
    pub event_time: DateTime<Utc>,
    pub country: String,
    pub impact: String,
    pub title: String,
}

/// A merged blackout window containing one or more events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlackoutWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub events: Vec<WindowEvent>,
}

impl BlackoutWindow {
    /// Returns the minutes remaining until this blackout window ends.
    /// If the window has already passed, returns 0.
    #[must_use]
    pub fn remaining_minutes(&self) -> i64 {
        (self.end - Utc::now()).num_seconds().max(0) / 60
    }

    /// Total duration of the window in minutes.
    #[must_use]
    pub fn duration_minutes(&self) -> i64 {
        (self.end - self.start).num_seconds() / 60
    }

    /// Whether this window is currently active at the specified timestamp.
    #[must_use]
    pub fn is_active_at(&self, time: DateTime<Utc>) -> bool {
        self.start <= time && time <= self.end
    }

    /// Whether this window is currently active right now.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.is_active_at(Utc::now())
    }

    /// Primary event title in this window (or summary if multiple).
    #[must_use]
    pub fn summary_title(&self) -> String {
        if self.events.is_empty() {
            "Blackout Window".to_string()
        } else if self.events.len() == 1 {
            self.events[0].title.clone()
        } else {
            format!(
                "{} (+{} events)",
                self.events[0].title,
                self.events.len() - 1
            )
        }
    }
}

/// Event sent to workers or subscribers when blackout status changes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlackoutNotification {
    pub active: bool,
    pub window: Option<BlackoutWindow>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_impact_parsing_and_matching() {
        assert_eq!("High".parse::<Impact>().unwrap(), Impact::High);
        assert_eq!("red".parse::<Impact>().unwrap(), Impact::High);
        assert_eq!("Medium".parse::<Impact>().unwrap(), Impact::Medium);
        assert_eq!("low".parse::<Impact>().unwrap(), Impact::Low);

        let high = Impact::High;
        assert!(high.is_high());
        assert!(high.matches_str("high"));
        assert!(high.matches_str("red"));
        assert!(!high.matches_str("medium"));
    }

    #[test]
    fn test_blackout_window_methods() {
        let now = Utc::now();
        let window = BlackoutWindow {
            start: now - Duration::minutes(10),
            end: now + Duration::minutes(20),
            events: vec![WindowEvent {
                is_custom: false,
                event_time: now,
                country: "USD".to_string(),
                impact: "High".to_string(),
                title: "US CPI Release".to_string(),
            }],
        };

        assert!(window.is_active());
        assert_eq!(window.duration_minutes(), 30);
        assert!(window.remaining_minutes() >= 19 && window.remaining_minutes() <= 20);
        assert_eq!(window.summary_title(), "US CPI Release");
    }

    #[test]
    fn test_currency_parsing_and_matching() {
        assert_eq!("USD".parse::<Currency>().unwrap(), Currency::USD);
        assert_eq!("usd".parse::<Currency>().unwrap(), Currency::USD);
        assert_eq!("eur".parse::<Currency>().unwrap(), Currency::EUR);
        assert_eq!("ALL".parse::<Currency>().unwrap(), Currency::All);
        assert_eq!(
            "XAU".parse::<Currency>().unwrap(),
            Currency::Custom("XAU".into())
        );

        let usd = Currency::USD;
        assert!(usd.matches_str("USD"));
        assert!(usd.matches_str("usd"));
        assert!(usd.matches_str("all"));
        assert!(!usd.matches_str("EUR"));

        let all = Currency::All;
        assert!(all.matches_str("USD"));
        assert!(all.matches_str("JPY"));

        assert_eq!(String::from(Currency::EUR), "EUR");
        assert_eq!(Currency::from("GBP"), Currency::GBP);
    }
}
