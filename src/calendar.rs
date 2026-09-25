use crate::error::Result;
use crate::types::EventTiming;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Default URL for FairEconomy / ForexFactory weekly economic calendar JSON.
pub const CALENDAR_URL: &str = "https://nfs.faireconomy.media/ff_calendar_thisweek.json";

/// Default file name for the local disk cache.
pub const DEFAULT_CACHE_FILENAME: &str = "economic_calendar.json";

/// Default maximum response body size in bytes (10 MiB) to prevent unbounded memory allocation.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;

/// Default maximum raw event count permitted from upstream feed.
pub const DEFAULT_MAX_EVENT_COUNT: usize = 50_000;

/// Default User-Agent header used for calendar HTTP requests.
///
/// FairEconomy / ForexFactory endpoints block generic bot User-Agents (including the default
/// reqwest header) with HTTP 403 Forbidden. This standard browser User-Agent is used by default to ensure
/// reliable retrieval.
///
/// # Upstream Dependency & Compliance Notice
///
/// Scraping third-party feeds under a spoofed browser User-Agent carries Terms of Service (ToS)
/// and availability risks if upstream providers employ more aggressive fingerprinting, rate limiting,
/// or alter their endpoint structure.
///
/// Callers who prefer explicit custom identification or organization-compliant client configurations
/// can specify a custom User-Agent via [`CalendarClient::with_user_agent`], or provide alternate/backup
/// feeds via [`CalendarClient::with_fallback_url`].
pub const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/// Computes a deterministic SHA-256 hex string over serialized raw calendar events for cache integrity verification.
fn compute_events_sha256(events: &[RawCalendarEvent]) -> String {
    let mut hasher = Sha256::new();
    if let Ok(bytes) = serde_json::to_vec(events) {
        hasher.update(&bytes);
    }
    let result = hasher.finalize();
    format!("{result:x}")
}

/// Pluggable validator callback for custom cryptographic verification or business sanity checks on fetched calendar events.
pub type CalendarIntegrityValidator = Arc<dyn Fn(&[RawCalendarEvent]) -> Result<()> + Send + Sync>;

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
    /// SHA-256 digest of the serialized events array for disk integrity verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
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
#[derive(Clone)]
pub struct CalendarClient {
    client: Client,
    calendar_url: String,
    fallback_urls: Vec<String>,
    cache_path: Option<PathBuf>,
    request_timeout: Duration,
    cache_ttl: Option<Duration>,
    calendar_timezone: Option<chrono_tz::Tz>,
    max_stale_cache_age: Option<Duration>,
    max_response_bytes: usize,
    max_event_count: usize,
    max_retries: usize,
    backoff_initial_delay: Duration,
    max_retry_after: Duration,
    overall_timeout: Option<Duration>,
    integrity_validator: Option<CalendarIntegrityValidator>,
}

impl std::fmt::Debug for CalendarClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CalendarClient")
            .field("calendar_url", &self.calendar_url)
            .field("fallback_urls", &self.fallback_urls)
            .field("cache_path", &self.cache_path)
            .field("request_timeout", &self.request_timeout)
            .field("cache_ttl", &self.cache_ttl)
            .field("calendar_timezone", &self.calendar_timezone)
            .field("max_stale_cache_age", &self.max_stale_cache_age)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_event_count", &self.max_event_count)
            .field("max_retries", &self.max_retries)
            .field("backoff_initial_delay", &self.backoff_initial_delay)
            .field("max_retry_after", &self.max_retry_after)
            .field("overall_timeout", &self.overall_timeout)
            .field(
                "integrity_validator",
                &self
                    .integrity_validator
                    .as_ref()
                    .map(|_| "<custom_validator>"),
            )
            .finish()
    }
}

impl Default for CalendarClient {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Builds a fallback `reqwest::Client` ensuring the configured User-Agent is preserved.
///
/// In the unlikely event that the builder fails (e.g. system TLS subsystem initialization failure),
/// logs a warning and falls back to `Client::default()`.
fn fallback_client() -> Client {
    Client::builder()
        .user_agent(DEFAULT_USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|err| {
            warn!(
                err = %err,
                "failed to build reqwest::Client with custom User-Agent; falling back to Client::default()"
            );
            Client::default()
        })
}

impl CalendarClient {
    /// Returns the default platform cache directory for redfolder.
    ///
    /// - On Windows: Respects `%LOCALAPPDATA%\redfolder\cache`, `%APPDATA%\redfolder\cache`, or `%USERPROFILE%\.cache\redfolder`.
    /// - On Linux/Unix: Respects `$XDG_CACHE_HOME/redfolder` or `$HOME/.cache/redfolder`.
    /// - Fallback: System temporary directory (`redfolder_cache`).
    #[must_use]
    pub fn default_cache_dir() -> PathBuf {
        #[cfg(windows)]
        {
            if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
                return PathBuf::from(local_app_data)
                    .join("redfolder")
                    .join("cache");
            }
            if let Ok(app_data) = std::env::var("APPDATA") {
                return PathBuf::from(app_data).join("redfolder").join("cache");
            }
            if let Ok(user_profile) = std::env::var("USERPROFILE") {
                return PathBuf::from(user_profile).join(".cache").join("redfolder");
            }
        }

        #[cfg(not(windows))]
        {
            if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
                return PathBuf::from(xdg).join("redfolder");
            }
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join(".cache").join("redfolder");
            }
        }

        // Generic cross-platform fallback for custom or test environments
        if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
            return PathBuf::from(xdg).join("redfolder");
        }
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(".cache").join("redfolder");
        }
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
            return PathBuf::from(local_app_data)
                .join("redfolder")
                .join("cache");
        }
        if let Ok(user_profile) = std::env::var("USERPROFILE") {
            return PathBuf::from(user_profile).join(".cache").join("redfolder");
        }

        warn!(
            "falling back to system temp directory for calendar cache. On shared/multi-user systems, consider configuring an explicit cache path or XDG_CACHE_HOME/LOCALAPPDATA to prevent cache tampering"
        );
        std::env::temp_dir().join("redfolder_cache")
    }

    /// Create a new `CalendarClient` with an optional cache directory fallibly.
    pub fn try_new(cache_dir: Option<PathBuf>) -> Result<Self> {
        let dir = cache_dir.or_else(|| Some(Self::default_cache_dir()));
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(DEFAULT_USER_AGENT)
            .build()?;
        Ok(Self::with_options(
            client,
            CALENDAR_URL,
            dir,
            Duration::from_secs(30),
        ))
    }

    /// Create a new `CalendarClient` with an optional cache directory.
    /// If `None`, defaults to `CalendarClient::default_cache_dir()`.
    #[must_use]
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        Self::try_new(cache_dir.clone()).unwrap_or_else(|err| {
            error!(err=%err, "failed to build configured HTTP client; falling back to default with User-Agent");
            let dir = cache_dir.or_else(|| Some(Self::default_cache_dir()));
            Self::with_options(fallback_client(), CALENDAR_URL, dir, Duration::from_secs(30))
        })
    }

    /// Create a new `CalendarClient` without any local disk caching fallibly.
    pub fn try_without_cache() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(DEFAULT_USER_AGENT)
            .build()?;
        Ok(Self::with_options(
            client,
            CALENDAR_URL,
            None,
            Duration::from_secs(30),
        ))
    }

    /// Create a new `CalendarClient` without any local disk caching.
    #[must_use]
    pub fn without_cache() -> Self {
        Self::try_without_cache().unwrap_or_else(|err| {
            error!(err=%err, "failed to build configured HTTP client; falling back to default with User-Agent");
            Self::with_options(fallback_client(), CALENDAR_URL, None, Duration::from_secs(30))
        })
    }

    /// Create a new `CalendarClient` with a custom User-Agent string.
    pub fn with_user_agent(cache_dir: Option<PathBuf>, user_agent: &str) -> Result<Self> {
        let dir = cache_dir.or_else(|| Some(Self::default_cache_dir()));
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(user_agent)
            .build()?;
        Ok(Self::with_options(
            client,
            CALENDAR_URL,
            dir,
            Duration::from_secs(30),
        ))
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
            fallback_urls: Vec::new(),
            cache_path,
            request_timeout,
            cache_ttl: None,
            calendar_timezone: None,
            max_stale_cache_age: Some(Duration::from_secs(36 * 3600)),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_event_count: DEFAULT_MAX_EVENT_COUNT,
            max_retries: 2,
            backoff_initial_delay: Duration::from_millis(500),
            max_retry_after: Duration::from_secs(10),
            overall_timeout: None,
            integrity_validator: None,
        }
    }

    /// Add a fallback calendar endpoint URL used if the primary URL fails.
    #[must_use]
    pub fn with_fallback_url(mut self, url: impl Into<String>) -> Self {
        self.fallback_urls.push(url.into());
        self
    }

    /// Add multiple fallback calendar endpoint URLs.
    #[must_use]
    pub fn with_fallback_urls<I, S>(mut self, urls: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fallback_urls.extend(urls.into_iter().map(Into::into));
        self
    }

    /// Configured fallback calendar URLs.
    #[must_use]
    pub fn fallback_urls(&self) -> &[String] {
        &self.fallback_urls
    }

    /// Primary calendar URL.
    #[must_use]
    pub fn calendar_url(&self) -> &str {
        &self.calendar_url
    }

    /// Set the primary calendar URL.
    pub fn set_calendar_url(&mut self, url: impl Into<String>) {
        self.calendar_url = url.into();
    }

    /// Add a fallback calendar URL.
    pub fn add_fallback_url(&mut self, url: impl Into<String>) {
        self.fallback_urls.push(url.into());
    }

    /// Configure the maximum response body size in bytes to prevent unbounded allocations.
    #[must_use]
    pub fn with_max_response_bytes(mut self, max_bytes: usize) -> Self {
        self.max_response_bytes = max_bytes;
        self
    }

    /// Set maximum allowable response body size in bytes.
    pub fn set_max_response_bytes(&mut self, max_bytes: usize) {
        self.max_response_bytes = max_bytes;
    }

    /// Maximum allowable response body size in bytes.
    #[must_use]
    pub fn max_response_bytes(&self) -> usize {
        self.max_response_bytes
    }

    /// Configure the maximum number of raw events permitted from upstream.
    #[must_use]
    pub fn with_max_event_count(mut self, max_events: usize) -> Self {
        self.max_event_count = max_events;
        self
    }

    /// Set maximum allowable raw event count.
    pub fn set_max_event_count(&mut self, max_events: usize) {
        self.max_event_count = max_events;
    }

    /// Maximum allowable raw event count.
    #[must_use]
    pub fn max_event_count(&self) -> usize {
        self.max_event_count
    }

    /// Configure maximum HTTP retry attempts for transient errors (429, 408, 5xx).
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Configured maximum retry attempts.
    #[must_use]
    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    /// Configure exponential backoff parameters.
    #[must_use]
    pub fn with_backoff(mut self, initial_delay: Duration, max_retry_after: Duration) -> Self {
        self.backoff_initial_delay = initial_delay;
        self.max_retry_after = max_retry_after;
        self
    }

    /// Initial exponential backoff delay duration.
    #[must_use]
    pub fn backoff_initial_delay(&self) -> Duration {
        self.backoff_initial_delay
    }

    /// Maximum duration allowed for server-specified Retry-After delays.
    #[must_use]
    pub fn max_retry_after(&self) -> Duration {
        self.max_retry_after
    }

    /// Configure an overall wall-clock timeout ceiling spanning all retry attempts and backoffs.
    #[must_use]
    pub fn with_overall_timeout(mut self, timeout: Duration) -> Self {
        self.overall_timeout = Some(timeout);
        self
    }

    /// Overall wall-clock timeout ceiling if set.
    #[must_use]
    pub fn overall_timeout(&self) -> Option<Duration> {
        self.overall_timeout
    }

    /// Configure a custom pluggable integrity validator callback.
    #[must_use]
    pub fn with_integrity_validator(
        mut self,
        validator: impl Fn(&[RawCalendarEvent]) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.integrity_validator = Some(Arc::new(validator));
        self
    }

    /// Configure the default source timezone for naive calendar timestamps.
    #[must_use]
    pub fn with_timezone(mut self, tz: chrono_tz::Tz) -> Self {
        self.calendar_timezone = Some(tz);
        self
    }

    /// Set the default source timezone for naive calendar timestamps.
    pub fn set_calendar_timezone(&mut self, tz: Option<chrono_tz::Tz>) {
        self.calendar_timezone = tz;
    }

    /// Default source timezone if configured.
    #[must_use]
    pub fn calendar_timezone(&self) -> Option<chrono_tz::Tz> {
        self.calendar_timezone
    }

    /// Configure the maximum allowable age for stale cache fallback on network failures.
    /// If `None`, stale cache fallback is disabled.
    #[must_use]
    pub fn with_max_stale_age(mut self, max_age: Option<Duration>) -> Self {
        self.max_stale_cache_age = max_age;
        self
    }

    /// Set maximum allowable age for stale cache fallback.
    pub fn set_max_stale_cache_age(&mut self, max_age: Option<Duration>) {
        self.max_stale_cache_age = max_age;
    }

    /// Configured maximum stale cache age.
    #[must_use]
    pub fn max_stale_cache_age(&self) -> Option<Duration> {
        self.max_stale_cache_age
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

    /// Fetch fresh calendar events directly from the remote API with bounded retry, response size checks,
    /// and fallback URL failover.
    ///
    /// # Latency Ceiling Guarantee
    ///
    /// Worst-case wall-clock latency per URL candidate is strictly bounded:
    /// with default settings (30s request timeout, 2 retries, 10s max backoff), worst-case latency under
    /// total network timeouts is `(1 + 2) * 30s + 20s = 110s`. Under immediate 429/5xx status responses,
    /// worst-case latency is `<= 21s`.
    ///
    /// If an [`overall_timeout`](Self::with_overall_timeout) is set, the entire operation across all candidates
    /// is terminated when the deadline expires.
    pub async fn fetch_remote(&self) -> Result<Vec<RawCalendarEvent>> {
        if let Some(timeout) = self.overall_timeout {
            tokio::time::timeout(timeout, self.fetch_remote_candidates())
                .await
                .map_err(|_| {
                    crate::error::RedFolderError::Calendar(format!(
                        "calendar fetch exceeded overall timeout ceiling of {:?}",
                        timeout
                    ))
                })?
        } else {
            self.fetch_remote_candidates().await
        }
    }

    async fn fetch_remote_candidates(&self) -> Result<Vec<RawCalendarEvent>> {
        let mut candidate_urls: Vec<&str> = Vec::with_capacity(1 + self.fallback_urls.len());
        candidate_urls.push(&self.calendar_url);
        for fb in &self.fallback_urls {
            candidate_urls.push(fb);
        }

        let mut last_error = None;

        for (url_idx, &url) in candidate_urls.iter().enumerate() {
            debug!(url=%url, url_idx, "fetching economic calendar");
            let mut url_error = None;

            for attempt in 0..=self.max_retries {
                match self
                    .client
                    .get(url)
                    .timeout(self.request_timeout)
                    .send()
                    .await
                {
                    Ok(mut resp) => {
                        let status = resp.status();
                        if status.is_success() {
                            // 1. Content-Length check
                            if let Some(len) = resp.content_length() {
                                if len > self.max_response_bytes as u64 {
                                    url_error = Some(crate::error::RedFolderError::Calendar(format!(
                                        "upstream response Content-Length ({len} bytes) exceeds maximum limit of {} bytes",
                                        self.max_response_bytes
                                    )));
                                    break;
                                }
                            }

                            // 2. Stream chunks with byte cap
                            let mut body_bytes = Vec::new();
                            let mut stream_err = None;
                            while let Some(chunk_res) = resp.chunk().await.transpose() {
                                match chunk_res {
                                    Ok(chunk) => {
                                        if body_bytes.len() + chunk.len() > self.max_response_bytes
                                        {
                                            stream_err = Some(crate::error::RedFolderError::Calendar(format!(
                                                "upstream response body exceeded maximum limit of {} bytes",
                                                self.max_response_bytes
                                            )));
                                            break;
                                        }
                                        body_bytes.extend_from_slice(&chunk);
                                    }
                                    Err(e) => {
                                        stream_err = Some(crate::error::RedFolderError::Http(e));
                                        break;
                                    }
                                }
                            }

                            if let Some(e) = stream_err {
                                warn!(err = %e, attempt, url = %url, "failed streaming response body");
                                url_error = Some(e);
                                if attempt < self.max_retries {
                                    let delay = self.backoff_initial_delay * (1 << attempt);
                                    tokio::time::sleep(delay).await;
                                    continue;
                                }
                                break;
                            }

                            match serde_json::from_slice::<Vec<serde_json::Value>>(&body_bytes) {
                                Ok(raw_items) => {
                                    let total_count = raw_items.len();
                                    if total_count > self.max_event_count {
                                        return Err(crate::error::RedFolderError::Calendar(format!(
                                            "upstream response contained {total_count} events, exceeding limit of {}",
                                            self.max_event_count
                                        )));
                                    }

                                    let mut events = Vec::with_capacity(total_count);
                                    let mut malformed_count = 0;

                                    for item in raw_items {
                                        match serde_json::from_value::<RawCalendarEvent>(item) {
                                            Ok(ev) => {
                                                let trimmed_date = ev.date.trim();
                                                let is_length_valid =
                                                    ev.title.len() <= 500 && ev.country.len() <= 10;
                                                if trimmed_date.is_empty() || !is_length_valid {
                                                    malformed_count += 1;
                                                } else {
                                                    events.push(ev);
                                                }
                                            }
                                            Err(_) => {
                                                malformed_count += 1;
                                            }
                                        }
                                    }

                                    if total_count > 0 && malformed_count == total_count {
                                        return Err(crate::error::RedFolderError::Calendar(
                                            "all upstream calendar events were malformed"
                                                .to_string(),
                                        ));
                                    }

                                    if malformed_count > 0 {
                                        warn!(
                                            malformed = %malformed_count,
                                            total = %total_count,
                                            "some upstream events failed validation"
                                        );
                                        if malformed_count * 5 > total_count {
                                            return Err(crate::error::RedFolderError::Calendar(format!(
                                                "upstream response corrupted: {malformed_count}/{total_count} events malformed"
                                            )));
                                        }
                                    }

                                    // Run pluggable integrity validator if configured
                                    if let Some(ref validator) = self.integrity_validator {
                                        validator(&events)?;
                                    }

                                    info!(url=%url, count = %events.len(), "downloaded calendar events successfully");
                                    return Ok(events);
                                }
                                Err(e) => {
                                    warn!(err = %e, attempt, url = %url, "failed to parse calendar response JSON");
                                    url_error = Some(crate::error::RedFolderError::Json(e));
                                    if attempt < self.max_retries {
                                        let delay = self.backoff_initial_delay * (1 << attempt);
                                        tokio::time::sleep(delay).await;
                                        continue;
                                    }
                                }
                            }
                        } else {
                            let is_rate_limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS;
                            let is_timeout = status == reqwest::StatusCode::REQUEST_TIMEOUT;
                            let is_server_err = status.is_server_error();
                            let is_retryable = is_rate_limited || is_timeout || is_server_err;

                            let retry_after_duration = if is_rate_limited {
                                resp.headers()
                                    .get(reqwest::header::RETRY_AFTER)
                                    .and_then(|val| val.to_str().ok())
                                    .and_then(|s| s.trim().parse::<u64>().ok())
                                    .map(|secs| Duration::from_secs(secs).min(self.max_retry_after))
                            } else {
                                None
                            };

                            warn!(
                                status = %status,
                                retryable = is_retryable,
                                retry_after = ?retry_after_duration,
                                attempt,
                                url = %url,
                                "calendar HTTP fetch returned non-success status"
                            );

                            if let Err(e) = resp.error_for_status() {
                                url_error = Some(crate::error::RedFolderError::Http(e));
                            }

                            if !is_retryable || attempt == self.max_retries {
                                break;
                            }

                            let delay = retry_after_duration
                                .unwrap_or_else(|| self.backoff_initial_delay * (1 << attempt));
                            debug!(delay = ?delay, "sleeping before retrying rate-limited or transient failure");
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!(err = %e, attempt, url = %url, "calendar HTTP transport request failed");
                        url_error = Some(crate::error::RedFolderError::Http(e));
                        if attempt < self.max_retries {
                            let delay = self.backoff_initial_delay * (1 << attempt);
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                }
            }

            if let Some(err) = url_error {
                warn!(url=%url, err=%err, "calendar candidate endpoint failed; trying next fallback if available");
                last_error = Some(err);
            }
        }

        Err(last_error.unwrap_or_else(|| {
            crate::error::RedFolderError::Calendar(
                "fetch failed after retries on all candidate endpoints".into(),
            )
        }))
    }

    /// Save raw calendar events to the local disk cache atomically (if cache path is set).
    ///
    /// On Unix systems, applies restrictive file permissions (0600) and directory permissions (0700)
    /// to prevent unauthorized access or tampering on shared multi-user hosts.
    pub fn save_cache(&self, events: &[RawCalendarEvent]) -> Result<()> {
        let Some(path) = &self.cache_path else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }

        let now = Utc::now();
        let expires_at = self
            .cache_ttl
            .and_then(|ttl| chrono::Duration::from_std(ttl).ok().map(|d| now + d));

        let events_sha256 = compute_events_sha256(events);

        let cached_data = CachedCalendarData {
            metadata: CacheMetadata {
                version: 1,
                fetched_at: now,
                expires_at,
                event_count: events.len(),
                sha256: Some(events_sha256),
            },
            events: events.to_vec(),
        };

        let json = serde_json::to_string_pretty(&cached_data)?;

        static CACHE_WRITE_COUNTER: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);

        // Atomic write: write to unique sibling temp file with restrictive 0600 perms on Unix,
        // flush to disk, then rename.
        // Combines PID, timestamp nanoseconds, and an atomic counter to prevent collision
        // when multiple threads or tasks write cache concurrently within the same process.
        let counter = CACHE_WRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let temp_path = path.with_extension(format!("tmp.{pid}.{nanos}.{counter}"));
        let write_result = (|| -> std::io::Result<()> {
            use std::io::Write;
            #[cfg(unix)]
            let mut file = {
                use std::os::unix::fs::OpenOptionsExt;
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .open(&temp_path)?
            };
            #[cfg(not(unix))]
            let mut file = std::fs::File::create(&temp_path)?;

            file.write_all(json.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temp_path, path)?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            }

            Ok(())
        })();

        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(crate::error::RedFolderError::Io(e));
        }

        debug!(path=%path.display(), count=%events.len(), "saved calendar cache atomically with metadata and checksum");
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
            if cached.metadata.version == 1 {
                if cached.metadata.event_count != cached.events.len() {
                    warn!(
                        path = %path.display(),
                        expected = cached.metadata.event_count,
                        actual = cached.events.len(),
                        "calendar cache event count mismatch; rejecting corrupted cache"
                    );
                    return None;
                }

                if let Some(ref expected_sha) = cached.metadata.sha256 {
                    let actual_sha = compute_events_sha256(&cached.events);
                    if actual_sha != *expected_sha {
                        warn!(
                            path = %path.display(),
                            expected = %expected_sha,
                            actual = %actual_sha,
                            "calendar cache sha256 checksum mismatch; rejecting tampered or corrupted cache"
                        );
                        return None;
                    }
                }

                debug!(path=%path.display(), version=%cached.metadata.version, count=%cached.events.len(), "loaded structured calendar cache v1");
                return Some(cached);
            } else {
                warn!(path=%path.display(), version=%cached.metadata.version, "unsupported cache version; ignoring");
                return None;
            }
        }

        // 2. Fall back to parsing legacy raw Vec<RawCalendarEvent> for backwards compatibility
        if let Ok(events) = serde_json::from_str::<Vec<RawCalendarEvent>>(&data) {
            debug!(path=%path.display(), count=%events.len(), "loaded legacy calendar cache");
            return Some(CachedCalendarData {
                metadata: CacheMetadata {
                    version: 0,
                    fetched_at: DateTime::<Utc>::UNIX_EPOCH,
                    expires_at: None,
                    event_count: events.len(),
                    sha256: None,
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
    ///
    /// Legacy caches (version 0) lacking fetch timestamps are rejected.
    #[must_use]
    pub fn is_cache_valid(&self) -> bool {
        let Some(data) = self.load_cache_data() else {
            return false;
        };

        // Legacy cache (version 0) has no fetch timestamp or expiration; cannot verify validity
        if data.metadata.version == 0 {
            return false;
        }

        if let Some(expires_at) = data.metadata.expires_at {
            Utc::now() <= expires_at
        } else {
            true
        }
    }

    /// Fetch fresh calendar events directly from the remote API, bypassing cache,
    /// protecting against empty responses overwriting good data, and saving to cache on success.
    pub async fn force_fetch(&self) -> Result<Vec<RawCalendarEvent>> {
        let events = self.fetch_remote().await?;
        if events.is_empty() {
            warn!("remote calendar returned 0 events during forced fetch; checking disk cache to protect existing data");
            if let Some(cached) = self.load_cache() {
                if !cached.is_empty() {
                    warn!(count=%cached.len(), "preserved valid disk cache instead of overwriting with empty remote response");
                    return Ok(cached);
                }
            }
            Ok(events)
        } else {
            if let Err(e) = self.save_cache(&events) {
                warn!(err=%e, "failed to persist calendar cache");
            }
            Ok(events)
        }
    }

    /// Fetch fresh events from the remote API, automatically saving to cache on success,
    /// or falling back to local cache if the remote request fails.
    ///
    /// Respects configured cache TTL and protects against empty responses overwriting good data.
    pub async fn fetch_or_cached(&self) -> Result<Vec<RawCalendarEvent>> {
        // 1. If TTL is active and disk cache is valid and non-empty, use cache
        if self.cache_ttl.is_some() && self.is_cache_valid() {
            if let Some(cached) = self.load_cache() {
                if !cached.is_empty() {
                    debug!(count=%cached.len(), "serving calendar from valid local cache (TTL active)");
                    return Ok(cached);
                }
            }
        }

        // 2. Otherwise fetch from remote with protection against empty / failed responses
        match self.fetch_remote().await {
            Ok(events) => {
                if events.is_empty() {
                    warn!("remote calendar returned 0 events; checking disk cache to protect existing data");
                    if let Some(cached) = self.load_cache() {
                        if !cached.is_empty() {
                            warn!(count=%cached.len(), "preserved valid disk cache instead of overwriting with empty remote response");
                            return Ok(cached);
                        }
                    }
                    Ok(events)
                } else {
                    if let Err(e) = self.save_cache(&events) {
                        warn!(err=%e, "failed to persist calendar cache");
                    }
                    Ok(events)
                }
            }
            Err(e) => {
                error!(err=%e, "failed to download calendar; checking disk cache fallback");
                if let Some(cached_data) = self.load_cache_data() {
                    if !cached_data.events.is_empty() {
                        if cached_data.metadata.version == 0 {
                            warn!(
                                path = %self.cache_path.as_deref().unwrap_or(Path::new("")).display(),
                                "legacy cache lacks fetch timestamp; rejecting as stale fallback"
                            );
                            return Err(e);
                        }

                        let is_acceptable = if let Some(max_stale) = self.max_stale_cache_age {
                            let max_stale_chrono = chrono::Duration::from_std(max_stale)
                                .unwrap_or_else(|_| chrono::Duration::hours(36));
                            let age = Utc::now() - cached_data.metadata.fetched_at;
                            age <= max_stale_chrono
                        } else {
                            false
                        };

                        if is_acceptable {
                            warn!(count=%cached_data.events.len(), "using fallback cached calendar data");
                            return Ok(cached_data.events);
                        } else {
                            warn!(
                                fetched_at = %cached_data.metadata.fetched_at,
                                "cached calendar data is too stale or stale fallback disabled; rejecting fallback"
                            );
                        }
                    }
                }
                Err(e)
            }
        }
    }
}

/// Parse the timing structure of a `RawCalendarEvent`.
///
/// Distinguishes between exact releases, tentative releases, and all-day events.
/// If `default_tz` is provided and the timestamp is naive, interprets the time in that timezone
/// with automatic Daylight Saving Time (DST) adjustment.
pub fn parse_event_timing(
    raw: &RawCalendarEvent,
    default_tz: Option<chrono_tz::Tz>,
) -> Option<EventTiming> {
    let date_trimmed = raw.date.trim();
    if date_trimmed.is_empty() {
        return None;
    }

    // 1. All-Day events
    if raw.is_all_day() {
        let date_part = date_trimmed
            .split_whitespace()
            .next()
            .unwrap_or(date_trimmed);
        let date_formats = ["%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y/%m/%d"];
        for fmt in date_formats {
            if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(date_part, fmt) {
                return Some(EventTiming::AllDay(naive_date));
            }
        }
    }

    // 2. Tentative events
    if raw.is_tentative() {
        let date_part = date_trimmed
            .split_whitespace()
            .next()
            .unwrap_or(date_trimmed);
        let date_formats = ["%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y/%m/%d"];
        for fmt in date_formats {
            if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(date_part, fmt) {
                return Some(EventTiming::TentativeDate(naive_date));
            }
        }
    }

    // 3. Timezone-aware RFC3339 string (e.g. "2026-06-05T12:30:00-04:00")
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_trimmed) {
        return Some(EventTiming::Exact(dt.with_timezone(&Utc)));
    }

    // 4. Standard ISO8601 with offset or Z
    if let Ok(dt) = DateTime::parse_from_str(date_trimmed, "%+") {
        return Some(EventTiming::Exact(dt.with_timezone(&Utc)));
    }

    // 5. If time string is empty:
    // Check if the event is a date-only entry. If date_trimmed matches a date format and has no time,
    // classify it as a tentative date-only event to prevent fabricating a false midnight exact blackout.
    if raw.time.trim().is_empty() {
        let date_formats = ["%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y/%m/%d"];
        for fmt in date_formats {
            if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(date_trimmed, fmt) {
                warn!(
                    title = %raw.title,
                    country = %raw.country,
                    date = %date_trimmed,
                    "event has date but missing release time; classifying as tentative date-only event"
                );
                return Some(EventTiming::TentativeDate(naive_date));
            }
        }
    }

    // 6. Combine naive date + time (or parse naive datetime if time was embedded in date string)
    let dt_str = if raw.time.trim().is_empty() {
        date_trimmed.to_string()
    } else {
        format!("{date_trimmed} {}", raw.time.trim())
    };

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
            let utc_dt = if let Some(tz) = default_tz {
                match tz.from_local_datetime(&naive) {
                    chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
                    chrono::LocalResult::Ambiguous(earliest, _) => earliest.with_timezone(&Utc),
                    chrono::LocalResult::None => {
                        // Nonexistent local time due to DST spring-forward gap.
                        // Shift forward by 1 hour to advance past the gap into the first valid daylight instant.
                        let shifted = naive + chrono::Duration::hours(1);
                        match tz.from_local_datetime(&shifted) {
                            chrono::LocalResult::Single(dt)
                            | chrono::LocalResult::Ambiguous(dt, _) => {
                                warn!(
                                    local_time = %naive,
                                    timezone = %tz.name(),
                                    shifted_time = %shifted,
                                    "local time falls in DST spring-forward gap; shifted forward 1h to valid instant"
                                );
                                dt.with_timezone(&Utc)
                            }
                            chrono::LocalResult::None => {
                                warn!(
                                    local_time = %naive,
                                    timezone = %tz.name(),
                                    "local time in DST gap could not be resolved; rejecting invalid timestamp"
                                );
                                return None;
                            }
                        }
                    }
                }
            } else {
                DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc)
            };
            return Some(EventTiming::Exact(utc_dt));
        }
    }

    None
}

/// Converts a naive calendar date to the start of that day (00:00:00) in the given timezone,
/// converted to UTC. If no timezone is provided, defaults to 00:00:00 UTC.
pub fn date_to_utc_start(
    date: chrono::NaiveDate,
    default_tz: Option<chrono_tz::Tz>,
) -> Option<DateTime<Utc>> {
    let naive = date.and_hms_opt(0, 0, 0)?;
    if let Some(tz) = default_tz {
        match tz.from_local_datetime(&naive) {
            chrono::LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
            chrono::LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
            chrono::LocalResult::None => {
                // If 00:00 does not exist due to DST spring-forward transition, advance to 01:00:00
                let shifted = naive + chrono::Duration::hours(1);
                match tz.from_local_datetime(&shifted) {
                    chrono::LocalResult::Single(dt) | chrono::LocalResult::Ambiguous(dt, _) => {
                        warn!(
                            local_date = %date,
                            timezone = %tz.name(),
                            "midnight falls in DST spring-forward gap; using 01:00:00 local time"
                        );
                        Some(dt.with_timezone(&Utc))
                    }
                    chrono::LocalResult::None => {
                        warn!(
                            local_date = %date,
                            timezone = %tz.name(),
                            "midnight in DST gap could not be resolved; rejecting invalid date"
                        );
                        None
                    }
                }
            }
        }
    } else {
        Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
    }
}

/// Parse the date and time strings of a `RawCalendarEvent` into a UTC `DateTime`,
/// using an optional default source timezone for naive timestamps and all-day/tentative events.
pub fn parse_event_datetime_with_tz(
    raw: &RawCalendarEvent,
    default_tz: Option<chrono_tz::Tz>,
) -> Option<DateTime<Utc>> {
    let timing = parse_event_timing(raw, default_tz)?;
    match timing {
        EventTiming::Exact(dt) => Some(dt),
        EventTiming::AllDay(d) | EventTiming::TentativeDate(d) => date_to_utc_start(d, default_tz),
    }
}

/// Parse the date and time strings of a `RawCalendarEvent` into a UTC `DateTime`.
/// Supports RFC-3339 timestamps (with timezone offset) as well as 12-hour AM/PM formats,
/// all-day events, and tentative releases (defaulting naive timestamps to UTC).
pub fn parse_event_datetime(raw: &RawCalendarEvent) -> Option<DateTime<Utc>> {
    parse_event_datetime_with_tz(raw, None)
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
        let structured = client
            .load_cache_data()
            .expect("structured cache should load");
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

        let loaded = client
            .load_cache()
            .expect("legacy cache should be supported");
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
        let parsed_tentative =
            parse_event_datetime(&tentative).expect("should parse tentative event");
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

    #[test]
    fn test_calendar_client_custom_user_agent() {
        let client = CalendarClient::with_user_agent(None, "custom-agent/1.0.0");
        assert!(client.is_ok());
    }

    #[test]
    fn test_dst_spring_forward_gap_shift() {
        // America/New_York on 2026-03-08 jumps from 02:00:00 EST to 03:00:00 EDT.
        // 02:30:00 local time does not exist.
        let tz = chrono_tz::America::New_York;
        let event = RawCalendarEvent {
            title: "Sunday Special Announcement".into(),
            country: "USD".into(),
            date: "03-08-2026".into(),
            time: "2:30am".into(),
            impact: "High".into(),
        };

        let timing =
            parse_event_timing(&event, Some(tz)).expect("should resolve DST gap by shifting");
        let dt = timing.exact_time().expect("should be exact time");
        // Shifted +1h to 03:30 EDT (-04:00) => 07:30 UTC.
        // Silently falling back to UTC would have produced 02:30 UTC!
        assert_eq!(
            dt.format("%Y-%m-%d %H:%M UTC").to_string(),
            "2026-03-08 07:30 UTC"
        );
    }

    #[test]
    fn test_dst_fall_back_ambiguous() {
        // America/New_York on 2026-11-01 falls back from 02:00:00 EDT to 01:00:00 EST.
        // 01:30:00 local time occurs twice: first at 05:30 UTC (EDT), then at 06:30 UTC (EST).
        let tz = chrono_tz::America::New_York;
        let event = RawCalendarEvent {
            title: "Fall-Back Release".into(),
            country: "USD".into(),
            date: "11-01-2026".into(),
            time: "1:30am".into(),
            impact: "High".into(),
        };

        let timing = parse_event_timing(&event, Some(tz)).expect("should resolve ambiguous time");
        let dt = timing.exact_time().expect("should be exact time");
        // Safe conservative policy chooses earliest instant: 05:30 UTC
        assert_eq!(
            dt.format("%Y-%m-%d %H:%M UTC").to_string(),
            "2026-11-01 05:30 UTC"
        );
    }

    #[test]
    fn test_date_only_event_without_time_is_tentative() {
        let event = RawCalendarEvent {
            title: "G7 Summit".into(),
            country: "ALL".into(),
            date: "2026-08-15".into(),
            time: "".into(),
            impact: "High".into(),
        };

        let timing = parse_event_timing(&event, None).expect("should parse date-only event");
        assert!(
            timing.is_tentative(),
            "date-only event without time must be tentative, not exact midnight"
        );
        assert!(
            !timing.is_exact(),
            "date-only event must not fabricate an exact midnight timestamp"
        );
        assert_eq!(
            timing,
            EventTiming::TentativeDate(chrono::NaiveDate::from_ymd_opt(2026, 8, 15).unwrap())
        );
    }

    #[test]
    fn test_date_with_embedded_time_and_empty_time_field() {
        let event = RawCalendarEvent {
            title: "Embedded Datetime".into(),
            country: "USD".into(),
            date: "2026-08-15 14:30".into(),
            time: "".into(),
            impact: "High".into(),
        };

        let timing = parse_event_timing(&event, None).expect("should parse embedded datetime");
        assert!(
            timing.is_exact(),
            "date containing explicit time must be parsed as exact"
        );
        let dt = timing.exact_time().unwrap();
        assert_eq!(
            dt.format("%Y-%m-%d %H:%M UTC").to_string(),
            "2026-08-15 14:30 UTC"
        );
    }

    #[test]
    fn test_default_cache_dir() {
        let dir = CalendarClient::default_cache_dir();
        assert!(!dir.as_os_str().is_empty());
        assert!(dir.to_string_lossy().contains("redfolder"));
    }

    #[test]
    fn test_cache_sha256_checksum_and_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let client = CalendarClient::new(Some(dir.path().to_path_buf()));

        let events = vec![RawCalendarEvent {
            title: "US CPI Release".into(),
            country: "USD".into(),
            date: "2026-06-05".into(),
            time: "12:30".into(),
            impact: "High".into(),
        }];

        // Save cache
        client.save_cache(&events).expect("cache save must succeed");

        // Verify valid load with sha256
        let loaded = client.load_cache_data().expect("cache should load cleanly");
        assert_eq!(loaded.events.len(), 1);
        assert!(loaded.metadata.sha256.is_some());

        // Tamper with cache file content while keeping valid JSON structure
        let cache_file = dir.path().join(DEFAULT_CACHE_FILENAME);
        let content = std::fs::read_to_string(&cache_file).unwrap();
        // Replace "US CPI Release" with "Tampered Event"
        let tampered = content.replace("US CPI Release", "Tampered Event");
        std::fs::write(&cache_file, tampered).unwrap();

        // Loading tampered cache must detect checksum mismatch and return None
        let result = client.load_cache_data();
        assert!(
            result.is_none(),
            "load_cache_data must reject cache with modified content due to sha256 checksum mismatch"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_cache_file_permissions_unix() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let client = CalendarClient::new(Some(dir.path().to_path_buf()));

        let events = vec![RawCalendarEvent {
            title: "Permission Test Event".into(),
            country: "USD".into(),
            date: "2026-06-05".into(),
            time: "12:30".into(),
            impact: "High".into(),
        }];

        client.save_cache(&events).unwrap();
        let cache_file = dir.path().join(DEFAULT_CACHE_FILENAME);
        let metadata = std::fs::metadata(&cache_file).unwrap();
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "cache file must have restrictive 0600 permissions on Unix"
        );
    }

    #[test]
    fn test_client_builder_options() {
        let client = CalendarClient::without_cache()
            .with_fallback_url("https://fallback1.internal.org/calendar.json")
            .with_fallback_urls(vec!["https://fallback2.internal.org/calendar.json"])
            .with_max_response_bytes(5 * 1024 * 1024)
            .with_max_event_count(10_000)
            .with_max_retries(4)
            .with_backoff(Duration::from_millis(200), Duration::from_secs(5))
            .with_overall_timeout(Duration::from_secs(15));

        assert_eq!(client.fallback_urls().len(), 2);
        assert_eq!(
            client.fallback_urls()[0],
            "https://fallback1.internal.org/calendar.json"
        );
        assert_eq!(client.max_response_bytes(), 5 * 1024 * 1024);
        assert_eq!(client.max_event_count(), 10_000);
        assert_eq!(client.max_retries(), 4);
        assert_eq!(client.backoff_initial_delay(), Duration::from_millis(200));
        assert_eq!(client.max_retry_after(), Duration::from_secs(5));
        assert_eq!(client.overall_timeout(), Some(Duration::from_secs(15)));
    }
}
