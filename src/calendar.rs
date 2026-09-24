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

/// Metadata associated with cached economic calendar data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheMetadata {
    /// Schema version for the cache structure.
    pub version: u32,
    /// Timestamp when the data was fetched from the remote source.
    pub fetched_at: DateTime<Utc>,
    /// Optional expiration timestamp based on configured TTL.
    pub expires_at: Option<DateTime<Utc>>,
    /// Number of raw events contained in the cached dataset.
    pub event_count: usize,
}

/// Disk cache envelope bundling events with validation metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedCalendarData {
    pub metadata: CacheMetadata,
    pub events: Vec<RawCalendarEvent>,
}

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

impl RawCalendarEvent {
    /// Whether this event is scheduled as an all-day event or multiday summit.
    #[must_use]
    pub fn is_all_day(&self) -> bool {
        let t = self.time.trim();
        t.eq_ignore_ascii_case("all day")
            || t.to_lowercase().starts_with("day ")
            || self.date.to_lowercase().contains("all day")
    }

    /// Whether this event's exact release time is marked as tentative.
    #[must_use]
    pub fn is_tentative(&self) -> bool {
        self.time.trim().eq_ignore_ascii_case("tentative")
    }
}

/// Client responsible for fetching economic calendar releases over HTTP and managing disk caching.
#[derive(Debug, Clone)]
pub struct CalendarClient {
    client: Client,
    calendar_url: String,
    cache_path: Option<PathBuf>,
    request_timeout: Duration,
    cache_ttl: Option<Duration>,
}

impl Default for CalendarClient {
    fn default() -> Self {
        Self::new(None)
    }
}

impl CalendarClient {
    /// Returns the default platform cache directory for redfolder.
    #[must_use]
    pub fn default_cache_dir() -> PathBuf {
        std::env::var("HOME")
            .ok()
            .map(|h| PathBuf::from(h).join(".cache").join("redfolder"))
            .unwrap_or_else(|| std::env::temp_dir().join("redfolder_cache"))
    }

    /// Create a new `CalendarClient` with an optional cache directory.
    /// If `None`, defaults to `CalendarClient::default_cache_dir()`.
    #[must_use]
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
    #[must_use]
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
            cache_ttl: None,
        }
    }

    /// Configure an optional Time-To-Live (TTL) for the local disk cache.
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = Some(ttl);
        self
    }

    /// Set an optional Time-To-Live (TTL) for the local disk cache.
    pub fn set_cache_ttl(&mut self, ttl: Option<Duration>) {
        self.cache_ttl = ttl;
    }

    /// Configured cache TTL if set.
    #[must_use]
    pub fn cache_ttl(&self) -> Option<Duration> {
        self.cache_ttl
    }

    /// Set an explicit cache file path.
    pub fn set_cache_path(&mut self, path: impl AsRef<Path>) {
        self.cache_path = Some(path.as_ref().to_path_buf());
    }

    /// Cache file path if configured.
    #[must_use]
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

        let now = Utc::now();
        let expires_at = self.cache_ttl.and_then(|ttl| {
            chrono::Duration::from_std(ttl).ok().map(|d| now + d)
        });

        let cached_data = CachedCalendarData {
            metadata: CacheMetadata {
                version: 1,
                fetched_at: now,
                expires_at,
                event_count: events.len(),
            },
            events: events.to_vec(),
        };

        let json = serde_json::to_string_pretty(&cached_data)?;
        std::fs::write(path, json)?;
        debug!(path=%path.display(), count=%events.len(), "saved calendar cache with metadata");
        Ok(())
    }

    /// Load full cached calendar data including metadata, with recovery from legacy and corrupted caches.
    pub fn load_cache_data(&self) -> Option<CachedCalendarData> {
        let path = self.cache_path.as_ref()?;
        if !path.exists() {
            return None;
        }

        let data = match std::fs::read_to_string(path) {
            Ok(d) => d,
            Err(e) => {
                warn!(path=%path.display(), err=%e, "failed to read calendar cache file");
                return None;
            }
        };

        // 1. Try parsing full CachedCalendarData with metadata
        if let Ok(cached) = serde_json::from_str::<CachedCalendarData>(&data) {
            debug!(path=%path.display(), version=%cached.metadata.version, count=%cached.events.len(), "loaded structured calendar cache");
            return Some(cached);
        }

        // 2. Fall back to parsing legacy raw Vec<RawCalendarEvent> for backwards compatibility
        if let Ok(events) = serde_json::from_str::<Vec<RawCalendarEvent>>(&data) {
            debug!(path=%path.display(), count=%events.len(), "loaded legacy calendar cache");
            return Some(CachedCalendarData {
                metadata: CacheMetadata {
                    version: 0,
                    fetched_at: Utc::now(),
                    expires_at: None,
                    event_count: events.len(),
                },
                events,
            });
        }

        // 3. Corrupted cache file: log warning, recover gracefully by returning None
        warn!(path=%path.display(), "corrupted calendar cache file; ignoring and falling back");
        None
    }

    /// Load raw calendar events from the local disk cache (if exists and readable).
    #[must_use]
    pub fn load_cache(&self) -> Option<Vec<RawCalendarEvent>> {
        self.load_cache_data().map(|c| c.events)
    }

    /// Checks whether the disk cache exists and is within its configured TTL.
    #[must_use]
    pub fn is_cache_valid(&self) -> bool {
        let Some(data) = self.load_cache_data() else {
            return false;
        };

        if let Some(expires_at) = data.metadata.expires_at {
            Utc::now() <= expires_at
        } else {
            true
        }
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
/// Supports RFC-3339 timestamps (with timezone offset) as well as 12-hour AM/PM formats,
/// all-day events, and tentative releases.
pub fn parse_event_datetime(raw: &RawCalendarEvent) -> Option<DateTime<Utc>> {
    let date_trimmed = raw.date.trim();
    if date_trimmed.is_empty() {
        return None;
    }

    // 1. Try timezone-aware RFC3339 string (e.g. "2024-06-10T12:30:00-04:00")
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_trimmed) {
        return Some(dt.with_timezone(&Utc));
    }

    // 2. Try standard ISO8601 with offset or Z
    if let Ok(dt) = DateTime::parse_from_str(date_trimmed, "%+") {
        return Some(dt.with_timezone(&Utc));
    }

    // 3. Handle "All Day" or "Tentative" events
    if raw.is_all_day() || raw.is_tentative() {
        let date_part = date_trimmed.split_whitespace().next().unwrap_or(date_trimmed);
        let date_formats = ["%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y/%m/%d"];
        for fmt in date_formats {
            if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(date_part, fmt) {
                if let Some(naive_dt) = naive_date.and_hms_opt(0, 0, 0) {
                    return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc));
                }
            }
        }
    }

    // 4. Combine date + time (e.g. "06-10-2024 8:30am" or "2024-06-10 14:30")
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
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
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

        // Verify metadata
        let structured = client.load_cache_data().expect("structured cache should load");
        assert_eq!(structured.metadata.version, 1);
        assert_eq!(structured.metadata.event_count, 1);
        assert!(client.is_cache_valid());
    }

    #[test]
    fn test_corrupted_cache_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let client = CalendarClient::new(Some(temp_dir.path().to_path_buf()));
        let cache_file = temp_dir.path().join(DEFAULT_CACHE_FILENAME);

        // Write corrupt garbage to cache file
        std::fs::write(&cache_file, "INVALID JSON { [[[[ }").unwrap();

        // Should not panic, but gracefully return None
        assert!(client.load_cache().is_none());
        assert!(!client.is_cache_valid());
    }

    #[test]
    fn test_legacy_cache_fallback() {
        let temp_dir = tempfile::tempdir().unwrap();
        let client = CalendarClient::new(Some(temp_dir.path().to_path_buf()));
        let cache_file = temp_dir.path().join(DEFAULT_CACHE_FILENAME);

        // Write old-style raw JSON array
        let legacy_json = r#"[
            {
                "title": "Legacy CPI",
                "country": "USD",
                "date": "2026-06-10T12:00:00Z",
                "time": "",
                "impact": "High"
            }
        ]"#;
        std::fs::write(&cache_file, legacy_json).unwrap();

        let loaded = client.load_cache().expect("legacy cache should be supported");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].title, "Legacy CPI");
    }

    #[test]
    fn test_all_day_and_tentative_parsing() {
        let all_day = RawCalendarEvent {
            title: "OPEC-JMMC Meetings".into(),
            country: "ALL".into(),
            date: "2026-06-15".into(),
            time: "All Day".into(),
            impact: "High".into(),
        };
        assert!(all_day.is_all_day());
        let parsed_all_day = parse_event_datetime(&all_day).expect("should parse all-day event");
        assert_eq!(parsed_all_day.to_rfc3339(), "2026-06-15T00:00:00+00:00");

        let tentative = RawCalendarEvent {
            title: "Chinese Trade Balance".into(),
            country: "CNY".into(),
            date: "2026-07-10".into(),
            time: "Tentative".into(),
            impact: "Medium".into(),
        };
        assert!(tentative.is_tentative());
        let parsed_tentative = parse_event_datetime(&tentative).expect("should parse tentative event");
        assert_eq!(parsed_tentative.to_rfc3339(), "2026-07-10T00:00:00+00:00");
    }

    #[test]
    fn test_dst_boundary_parsing() {
        // Winter: US Eastern Standard Time (UTC-5)
        let winter = RawCalendarEvent {
            title: "US CPI (Winter)".into(),
            country: "USD".into(),
            date: "2026-01-15T08:30:00-05:00".into(),
            time: "".into(),
            impact: "High".into(),
        };
        let parsed_winter = parse_event_datetime(&winter).unwrap();
        // 08:30 EST (-05:00) is 13:30 UTC
        assert_eq!(parsed_winter.to_rfc3339(), "2026-01-15T13:30:00+00:00");

        // Summer: US Eastern Daylight Time (UTC-4)
        let summer = RawCalendarEvent {
            title: "US CPI (Summer)".into(),
            country: "USD".into(),
            date: "2026-07-15T08:30:00-04:00".into(),
            time: "".into(),
            impact: "High".into(),
        };
        let parsed_summer = parse_event_datetime(&summer).unwrap();
        // 08:30 EDT (-04:00) is 12:30 UTC
        assert_eq!(parsed_summer.to_rfc3339(), "2026-07-15T12:30:00+00:00");
    }
}
