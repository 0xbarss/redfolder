use crate::error::{RedFolderError, Result};
use chrono::{DateTime, Datelike, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Mode for weekend market close curfew windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WeekendMode {
    /// Window ends at the configured end time on Friday evening.
    Short,
    /// Window extends throughout the entire weekend until Monday 00:00 UTC.
    Weekend,
}

impl fmt::Display for WeekendMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WeekendMode::Short => write!(f, "short"),
            WeekendMode::Weekend => write!(f, "weekend"),
        }
    }
}

impl FromStr for WeekendMode {
    type Err = RedFolderError;

    fn from_str(s: &str) -> Result<Self> {
        if s.eq_ignore_ascii_case("weekend") {
            Ok(WeekendMode::Weekend)
        } else if s.eq_ignore_ascii_case("short") {
            Ok(WeekendMode::Short)
        } else {
            Err(RedFolderError::Curfew(format!(
                "invalid weekend mode '{s}': expected 'short' or 'weekend'"
            )))
        }
    }
}

impl From<WeekendMode> for String {
    fn from(m: WeekendMode) -> Self {
        m.to_string()
    }
}

/// Parses a 24-hour time string in "HH:MM" format (e.g. "20:30").
pub fn parse_time(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() == 2 {
        let h = parts[0]
            .trim()
            .parse::<u32>()
            .map_err(|_| RedFolderError::ParseTime(s.to_string()))?;
        let m = parts[1]
            .trim()
            .parse::<u32>()
            .map_err(|_| RedFolderError::ParseTime(s.to_string()))?;
        if h < 24 && m < 60 {
            return Ok((h, m));
        }
    }
    Err(RedFolderError::ParseTime(s.to_string()))
}

/// Calculates the next weekend market close blackout window based on current UTC time.
///
/// - `start_str`: Time in UTC when curfew begins on Friday (e.g. "20:30").
/// - `end_str`: Time in UTC when curfew ends on Friday (e.g. "21:00") for "short" mode.
/// - `mode`: "short" (ends same day at `end_str`) or "weekend" (runs through to Monday 00:00 UTC).
pub fn next_weekend_window(
    start_str: &str,
    end_str: &str,
    mode: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let (sh, sm) = parse_time(start_str)?;
    let parsed_mode: WeekendMode = mode.parse()?;

    let now = Utc::now();
    // Monday = 0 .. Friday = 4 .. Sunday = 6
    let weekday_num = now.weekday().num_days_from_monday() as i64;
    let days_ahead = (4 - weekday_num).rem_euclid(7);

    let target_friday = (now + Duration::days(days_ahead)).date_naive();
    let mut start = DateTime::<Utc>::from_naive_utc_and_offset(
        target_friday
            .and_hms_opt(sh, sm, 0)
            .ok_or_else(|| RedFolderError::Curfew("invalid start timestamp".to_string()))?,
        Utc,
    );

    // If today is Friday (days_ahead == 0) and current time is already past start, advance to next Friday
    if days_ahead == 0 && now >= start {
        let next_friday = (now + Duration::days(7)).date_naive();
        start = DateTime::<Utc>::from_naive_utc_and_offset(
            next_friday.and_hms_opt(sh, sm, 0).ok_or_else(|| {
                RedFolderError::Curfew("invalid next start timestamp".to_string())
            })?,
            Utc,
        );
    }

    let end = match parsed_mode {
        WeekendMode::Weekend => {
            // End at Monday 00:00 UTC (3 days after Friday)
            let monday_date = (start + Duration::days(3)).date_naive();
            DateTime::<Utc>::from_naive_utc_and_offset(
                monday_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
                    RedFolderError::Curfew("invalid monday timestamp".to_string())
                })?,
                Utc,
            )
        }
        WeekendMode::Short => {
            let (eh, em) = parse_time(end_str)?;
            DateTime::<Utc>::from_naive_utc_and_offset(
                start
                    .date_naive()
                    .and_hms_opt(eh, em, 0)
                    .ok_or_else(|| RedFolderError::Curfew("invalid end timestamp".to_string()))?,
                Utc,
            )
        }
    };

    Ok((start, end))
}

/// Helper that generates human-readable description for weekend curfew.
#[must_use]
pub fn weekend_window_title(start_str: &str, end_str: &str, mode: &str) -> String {
    if mode.eq_ignore_ascii_case("weekend") {
        "Weekend Blackout (Market Close)".to_string()
    } else {
        format!("Weekend Curfew ({start_str}-{end_str} UTC)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_time() {
        assert_eq!(parse_time("20:30").unwrap(), (20, 30));
        assert_eq!(parse_time("00:00").unwrap(), (0, 0));
        assert_eq!(parse_time("23:59").unwrap(), (23, 59));
        assert!(parse_time("24:00").is_err());
        assert!(parse_time("2030").is_err());
        assert!(parse_time("abc:def").is_err());
    }

    #[test]
    fn test_next_weekend_window_short_mode() {
        let (start, end) = next_weekend_window("20:30", "21:00", "short").unwrap();
        assert!(start < end);
        assert_eq!((end - start).num_minutes(), 30);
    }

    #[test]
    fn test_next_weekend_window_weekend_mode() {
        let (start, end) = next_weekend_window("20:30", "21:00", "weekend").unwrap();
        assert!(start < end);
        // Friday 20:30 UTC to Monday 00:00 UTC is 51.5 hours
        assert_eq!((end - start).num_minutes(), 51 * 60 + 30);
    }

    #[test]
    fn test_weekend_window_title() {
        assert_eq!(
            weekend_window_title("20:30", "21:00", "weekend"),
            "Weekend Blackout (Market Close)"
        );
        assert_eq!(
            weekend_window_title("20:30", "21:00", "short"),
            "Weekend Curfew (20:30-21:00 UTC)"
        );
    }

    #[test]
    fn test_next_weekend_window_invalid_mode() {
        // Typos like "weeknd" or unrecognized strings should fail explicitly rather than silently falling back
        let err = next_weekend_window("20:30", "21:00", "weeknd").unwrap_err();
        assert!(matches!(err, RedFolderError::Curfew(_)));
        assert!(err.to_string().contains("invalid weekend mode 'weeknd'"));

        assert!("short".parse::<WeekendMode>().is_ok());
        assert!("weekend".parse::<WeekendMode>().is_ok());
        assert!("WEEKEND".parse::<WeekendMode>().is_ok());
        assert!("invalid".parse::<WeekendMode>().is_err());
    }
}
