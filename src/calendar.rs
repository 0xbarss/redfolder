use crate::error::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Default URL for FairEconomy / ForexFactory weekly economic calendar JSON.
pub const CALENDAR_URL: &str = "https://nfs.faireconomy.media/ff_calendar_thisweek.json";

/// Default file name for the local disk cache.
pub const DEFAULT_CACHE_FILENAME: &str = "economic_calendar.json";

/// Raw event representation from the ForexFactory / FairEconomy JSON API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawCalendarEvent {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub country: String,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub time: String,
    #[serde(default)]
    pub impact: String,
}

/// Client responsible for fetching economic calendar releases over HTTP and managing disk caching.
#[derive(Debug, Clone)]
pub struct CalendarClient {
    client: Client,
    calendar_url: String,
    cache_path: Option<PathBuf>,
    request_timeout: Duration,
}

impl Default for CalendarClient {
    fn default() -> Self {
        Self::new(None)
    }
}

impl CalendarClient {
    /// Returns the default platform cache directory for redfolder.
    pub fn default_cache_dir() -> PathBuf {
        std::env::var("HOME")
            .ok()
            .map(|h| PathBuf::from(h).join(".cache").join("redfolder"))
            .unwrap_or_else(|| std::env::temp_dir().join("redfolder_cache"))
    }

    /// Create a new `CalendarClient` with an optional cache directory.
    /// If `None`, defaults to `CalendarClient::default_cache_dir()`.
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        let dir = cache_dir.or_else(|| Some(Self::default_cache_dir()));
        Self::with_options(
            Client::builder()
                .timeout(Duration::from_secs(30))
                .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()
                .unwrap_or_else(|_| Client::new()),
            CALENDAR_URL,
            dir,
            Duration::from_secs(30),
        )
    }

    /// Create a new `CalendarClient` without any local disk caching.
    pub fn without_cache() -> Self {
        Self::with_options(
            Client::builder()
                .timeout(Duration::from_secs(30))
                .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()
                .unwrap_or_else(|_| Client::new()),
            CALENDAR_URL,
            None,
            Duration::from_secs(30),
        )
    }

    /// Create with custom options.
    pub fn with_options(
        client: Client,
        calendar_url: impl Into<String>,
        cache_dir: Option<PathBuf>,
        request_timeout: Duration,
    ) -> Self {
        let cache_path = cache_dir.map(|dir| dir.join(DEFAULT_CACHE_FILENAME));
        Self {
            client,
            calendar_url: calendar_url.into(),
            cache_path,
            request_timeout,
        }
    }

    /// Set an explicit cache file path.
    pub fn set_cache_path(&mut self, path: impl AsRef<Path>) {
        self.cache_path = Some(path.as_ref().to_path_buf());
    }

    /// Cache file path if configured.
    pub fn cache_path(&self) -> Option<&Path> {
        self.cache_path.as_deref()
    }

    /// Fetch fresh calendar events directly from the remote API.
    pub async fn fetch_remote(&self) -> Result<Vec<RawCalendarEvent>> {
        debug!(url=%self.calendar_url, "fetching economic calendar");
        let resp = self
            .client
            .get(&self.calendar_url)
            .timeout(self.request_timeout)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<serde_json::Value>>()
            .await?;

        let events: Vec<RawCalendarEvent> = resp
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect();

        info!(count = %events.len(), "downloaded calendar events successfully");
        Ok(events)
    }

    /// Save raw calendar events to the local disk cache (if cache path is set).
    pub fn save_cache(&self, events: &[RawCalendarEvent]) -> Result<()> {
        let Some(path) = &self.cache_path else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(events)?;
        std::fs::write(path, json)?;
        debug!(path=%path.display(), count=%events.len(), "saved calendar cache");
        Ok(())
    }

    /// Load raw calendar events from the local disk cache (if exists).
    pub fn load_cache(&self) -> Option<Vec<RawCalendarEvent>> {
        let path = self.cache_path.as_ref()?;
        if !path.exists() {
            return None;
        }

        let data = std::fs::read_to_string(path).ok()?;
        let events = serde_json::from_str(&data).ok()?;
        debug!(path=%path.display(), "loaded calendar cache");
        Some(events)
    }

    /// Fetch fresh events from the remote API, automatically saving to cache on success,
    /// or falling back to local cache if the remote request fails.
    pub async fn fetch_or_cached(&self) -> Result<Vec<RawCalendarEvent>> {
        match self.fetch_remote().await {
            Ok(events) => {
                if let Err(e) = self.save_cache(&events) {
                    warn!(err=%e, "failed to persist calendar cache");
                }
                Ok(events)
            }
            Err(e) => {
                error!(err=%e, "failed to download calendar; checking disk cache");
                match self.load_cache() {
                    Some(cached) => {
                        warn!(count=%cached.len(), "using fallback cached calendar data");
                        Ok(cached)
                    }
                    None => Err(e),
                }
            }
        }
    }
}

/// Parse the date and time strings of a `RawCalendarEvent` into a UTC `DateTime`.
/// Supports RFC-3339 timestamps (with timezone offset) as well as 12-hour AM/PM formats.
pub fn parse_event_datetime(raw: &RawCalendarEvent) -> Option<DateTime<Utc>> {
    let date_trimmed = raw.date.trim();
    if date_trimmed.is_empty() {
        return None;
    }

    // 1. Try timezone-aware RFC3339 string (e.g. "2024-06-10T12:30:00-04:00")
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_trimmed) {
        return Some(dt.with_timezone(&Utc));
    }

    // 2. Combine date + time (e.g. "06-10-2024 8:30am" or "2024-06-10 14:30")
    let time_str = if raw.time.trim().is_empty() {
        "12:00am"
    } else {
        raw.time.trim()
    };
    let dt_str = format!("{date_trimmed} {time_str}");

    // Try common date/time formats
    let formats = [
        "%m-%d-%Y %I:%M%p",
        "%Y-%m-%d %I:%M%p",
        "%m/%d/%Y %I:%M%p",
        "%Y/%m/%d %I:%M%p",
        "%m-%d-%Y %H:%M",
        "%Y-%m-%d %H:%M",
    ];

    for fmt in formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(&dt_str, fmt) {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rfc3339_datetime() {
        let raw = RawCalendarEvent {
            title: "US Non-Farm Payrolls".into(),
            country: "USD".into(),
            date: "2026-06-05T12:30:00-04:00".into(),
            time: String::new(),
            impact: "High".into(),
        };

        let parsed = parse_event_datetime(&raw).expect("should parse rfc3339");
        assert_eq!(parsed.to_rfc3339(), "2026-06-05T16:30:00+00:00");
    }

    #[test]
    fn test_parse_ampm_datetime() {
        let raw = RawCalendarEvent {
            title: "US CPI".into(),
            country: "USD".into(),
            date: "06-10-2026".into(),
            time: "8:30am".into(),
            impact: "High".into(),
        };

        let parsed = parse_event_datetime(&raw).expect("should parse date + ampm");
        assert_eq!(parsed.to_rfc3339(), "2026-06-10T08:30:00+00:00");
    }

    #[test]
    fn test_cache_save_and_load() {
        let temp_dir = tempfile::tempdir().unwrap();
        let client = CalendarClient::new(Some(temp_dir.path().to_path_buf()));

        let events = vec![RawCalendarEvent {
            title: "FOMC Rate Decision".into(),
            country: "USD".into(),
            date: "2026-06-10T18:00:00Z".into(),
            time: "".into(),
            impact: "High".into(),
        }];

        client.save_cache(&events).unwrap();
        let loaded = client.load_cache().expect("cache should load");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].title, "FOMC Rate Decision");
    }
}
