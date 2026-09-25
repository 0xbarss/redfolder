use crate::error::{RedFolderError, Result};
use chrono::{DateTime, Datelike, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Mode for weekend market close curfew windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WeekendMode {
    #[serde(alias = "Short", alias = "SHORT")]
    /// Window ends at the configured end time on Friday evening.
    #[default]
    Short,
    #[serde(alias = "Weekend", alias = "WEEKEND")]
    /// Window extends throughout the entire weekend until Monday 00:00 UTC.
    Weekend,
}

impl AsRef<str> for WeekendMode {
    fn as_ref(&self) -> &str {
        match self {
            WeekendMode::Short => "short",
            WeekendMode::Weekend => "weekend",
        }
    }
}

impl PartialEq<&str> for WeekendMode {
    fn eq(&self, other: &&str) -> bool {
        self.as_ref().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<WeekendMode> for &str {
    fn eq(&self, other: &WeekendMode) -> bool {
        other.eq(self)
    }
}

impl PartialEq<String> for WeekendMode {
    fn eq(&self, other: &String) -> bool {
        self.as_ref().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<WeekendMode> for String {
    fn eq(&self, other: &WeekendMode) -> bool {
        other.eq(self)
    }
}

impl TryFrom<&str> for WeekendMode {
    type Error = RedFolderError;
    fn try_from(s: &str) -> Result<Self> {
        s.parse()
    }
}

impl TryFrom<String> for WeekendMode {
    type Error = RedFolderError;
    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
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

/// Calculates the weekend market close blackout window for a given timestamp and typed mode.
pub fn weekend_window_for_mode(
    at: DateTime<Utc>,
    start_str: &str,
    end_str: &str,
    mode: WeekendMode,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let (sh, sm) = parse_time(start_str)?;

    // Monday = 0 .. Friday = 4 .. Sunday = 6
    let weekday_num = at.weekday().num_days_from_monday() as i64;
    let days_since_friday = (weekday_num + 3).rem_euclid(7);

    let recent_friday = (at - Duration::days(days_since_friday)).date_naive();
    let recent_start = DateTime::<Utc>::from_naive_utc_and_offset(
        recent_friday
            .and_hms_opt(sh, sm, 0)
            .ok_or_else(|| RedFolderError::Curfew("invalid start timestamp".to_string()))?,
        Utc,
    );

    let recent_end = match mode {
        WeekendMode::Weekend => {
            let monday_date = recent_friday + Duration::days(3);
            DateTime::<Utc>::from_naive_utc_and_offset(
                monday_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
                    RedFolderError::Curfew("invalid monday timestamp".to_string())
                })?,
                Utc,
            )
        }
        WeekendMode::Short => {
            let (eh, em) = parse_time(end_str)?;
            // Support cross-midnight short curfews (e.g. 23:00 -> 01:00 extends into Saturday)
            let end_date = if (eh, em) <= (sh, sm) {
                recent_friday + Duration::days(1)
            } else {
                recent_friday
            };

            DateTime::<Utc>::from_naive_utc_and_offset(
                end_date
                    .and_hms_opt(eh, em, 0)
                    .ok_or_else(|| RedFolderError::Curfew("invalid end timestamp".to_string()))?,
                Utc,
            )
        }
    };

    if at < recent_end {
        Ok((recent_start, recent_end))
    } else {
        // Recent window has already passed; advance to next week's window
        Ok((
            recent_start + Duration::days(7),
            recent_end + Duration::days(7),
        ))
    }
}

/// Calculates the weekend market close blackout window for a given timestamp.
///
/// If `at` is currently inside the active weekend window, returns that active window.
/// If `at` is outside (or past) the previous window, returns the next upcoming window.
///
/// - `at`: The evaluation timestamp in UTC (enables 100% deterministic queries/backtesting).
/// - `start_str`: Time in UTC when curfew begins on Friday (e.g. "20:30").
/// - `end_str`: Time in UTC when curfew ends on Friday (or Saturday if cross-midnight) for "short" mode.
/// - `mode`: "short" or "weekend" (runs through to Monday 00:00 UTC).
pub fn weekend_window_at(
    at: DateTime<Utc>,
    start_str: &str,
    end_str: &str,
    mode: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let parsed_mode: WeekendMode = mode.parse()?;
    weekend_window_for_mode(at, start_str, end_str, parsed_mode)
}

/// Calculates the next weekend market close blackout window based on current UTC time.
///
/// Backwards-compatible convenience wrapper around [`weekend_window_at`].
pub fn next_weekend_window(
    start_str: &str,
    end_str: &str,
    mode: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    weekend_window_at(Utc::now(), start_str, end_str, mode)
}

/// Helper that generates human-readable description for typed weekend curfew mode.
#[must_use]
pub fn weekend_window_title_for_mode(start_str: &str, end_str: &str, mode: WeekendMode) -> String {
    match mode {
        WeekendMode::Weekend => "Weekend Blackout (Market Close)".to_string(),
        WeekendMode::Short => format!("Weekend Curfew ({start_str}-{end_str} UTC)"),
    }
}

/// Helper that generates human-readable description for weekend curfew.
#[must_use]
pub fn weekend_window_title(start_str: &str, end_str: &str, mode: &str) -> String {
    let m: WeekendMode = mode.parse().unwrap_or(WeekendMode::Short);
    weekend_window_title_for_mode(start_str, end_str, m)
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

    #[test]
    fn test_weekend_window_exact_end_boundary() {
        use chrono::TimeZone;
        // Friday 2026-06-05 20:30 UTC to Monday 2026-06-08 00:00 UTC
        let fri_start = Utc.with_ymd_and_hms(2026, 6, 5, 20, 30, 0).unwrap();
        let mon_end = Utc.with_ymd_and_hms(2026, 6, 8, 0, 0, 0).unwrap();

        // One second before Monday midnight: still in current weekend window
        let just_before = mon_end - Duration::seconds(1);
        let (start1, end1) = weekend_window_at(just_before, "20:30", "21:00", "weekend").unwrap();
        assert_eq!(start1, fri_start);
        assert_eq!(end1, mon_end);

        // Exactly at Monday 00:00:00 UTC: window has ended, must return next weekend's window
        let (start2, end2) = weekend_window_at(mon_end, "20:30", "21:00", "weekend").unwrap();
        assert_eq!(start2, fri_start + Duration::days(7));
        assert_eq!(end2, mon_end + Duration::days(7));
    }
}
