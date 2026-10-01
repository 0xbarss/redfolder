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

/// Default overall wall-clock deadline spanning all candidate URLs, attempts, and backoff delays (60 seconds).
pub const DEFAULT_OVERALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Default maximum backoff delay duration (10 seconds).
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Maximum permissible HTTP retry attempts for transient errors.
pub const MAX_RETRIES_LIMIT: usize = 10;

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

/// Computes a deterministic SHA-256 hex string over cache schema version, fetched_at timestamp,
/// event count, and serialized raw calendar events for cache v2 integrity verification.
fn compute_cache_digest(
    version: u32,
    fetched_at: DateTime<Utc>,
    events: &[RawCalendarEvent],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"redfolder-cache\0");
    hasher.update(version.to_le_bytes());
    hasher.update(
        fetched_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
            .as_bytes(),
    );
    hasher.update((events.len() as u64).to_le_bytes());
    if let Ok(bytes) = serde_json::to_vec(events) {
        hasher.update(&bytes);
    }
    let result = hasher.finalize();
    format!("{result:x}")
}

/// Computes a deterministic SHA-256 hex string over serialized raw calendar events for legacy cache v1 integrity verification.
fn compute_events_sha256(events: &[RawCalendarEvent]) -> String {
    let mut hasher = Sha256::new();
    if let Ok(bytes) = serde_json::to_vec(events) {
        hasher.update(&bytes);
    }
    let result = hasher.finalize();
    format!("{result:x}")
}

#[cfg(unix)]
fn verify_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let md = std::fs::symlink_metadata(dir)?;
    if md.file_type().is_symlink() || !md.is_dir() {
        return Err(crate::error::RedFolderError::Config(format!(
            "cache dir {} is not a plain directory",
            dir.display()
        )));
    }
    // Ownership without unsafe/libc: a file we create is owned by our effective uid.
    let probe = dir.join(format!(".rf_probe_{}", std::process::id()));
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)?;
    let my_uid = f.metadata()?.uid();
    drop(f);
    let _ = std::fs::remove_file(&probe);
    if md.uid() != my_uid {
        return Err(crate::error::RedFolderError::Config(format!(
            "cache dir {} is owned by another user",
            dir.display()
        )));
    }
    if md.permissions().mode() & 0o022 != 0 {
        return Err(crate::error::RedFolderError::Config(format!(
            "cache dir {} is accessible by group/others (mode {:o})",
            dir.display(),
            md.permissions().mode() & 0o777
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_private_dir(dir: &Path) -> Result<()> {
    let md = std::fs::symlink_metadata(dir)?;
    if !md.is_dir() {
        return Err(crate::error::RedFolderError::Config(format!(
            "cache dir {} is not a directory",
            dir.display()
        )));
    }
    Ok(())
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

/// Where a calendar snapshot came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotSource {
    /// Downloaded from a remote endpoint during this call.
    Remote,
    /// Served from disk because the configured cache TTL is still valid.
    TtlCache,
    /// Remote failed; an acceptable (age-bounded) cache was served instead.
    FallbackCache,
    /// Remote answered with zero events; an acceptable cache was served instead.
    EmptyFeedCache,
}

impl SnapshotSource {
    /// `true` when the data can be treated as a successful synchronization.
    #[must_use]
    pub fn is_fresh(self) -> bool {
        matches!(self, Self::Remote | Self::TtlCache)
    }
}

/// Counters describing how much of an upstream payload was usable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestStats {
    /// Items present in the upstream JSON array.
    pub received: usize,
    /// Items rejected at ingestion (bad JSON shape, empty date, over-long fields).
    pub malformed: usize,
    /// Items kept after ingestion validation.
    pub kept: usize,
    /// Kept items whose date/time could not be parsed into a timestamp (filled in by the service).
    pub unparseable: usize,
}

/// Calendar data together with its provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarSnapshot {
    pub events: Vec<RawCalendarEvent>,
    pub source: SnapshotSource,
    /// When the *data* was downloaded (not when it was returned to the caller).
    pub data_fetched_at: DateTime<Utc>,
    /// Why remote data was not used (set for `FallbackCache` / `EmptyFeedCache`).
    pub remote_error: Option<String>,
    pub stats: IngestStats,
}

impl CalendarSnapshot {
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        !self.source.is_fresh()
    }

    /// Age of the underlying data at `now`.
    #[must_use]
    pub fn age_at(&self, now: DateTime<Utc>) -> chrono::Duration {
        now - self.data_fetched_at
    }
}

const MAX_CLOCK_SKEW: chrono::Duration = chrono::Duration::minutes(5);

/// Security policy governing calendar endpoint URLs, transport schemes, and HTTP redirects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UrlPolicy {
    /// When true, only HTTPS endpoints are permitted. Plaintext HTTP endpoints are rejected.
    pub https_only: bool,
    /// Optional allow-list of permissible hostnames (case-insensitive exact match).
    /// If `None`, any hostname complying with other policy constraints is permitted.
    pub allowed_hosts: Option<Vec<String>>,
    /// When true, private, loopback, link-local, broadcast, unspecified, and cloud metadata
    /// IP addresses are strictly rejected to mitigate SSRF and internal scanning.
    pub block_private_ips: bool,
    /// Maximum number of consecutive HTTP redirects allowed before terminating.
    pub max_redirects: usize,
}

impl Default for UrlPolicy {
    fn default() -> Self {
        Self {
            https_only: true,
            allowed_hosts: None,
            block_private_ips: true,
            max_redirects: 3,
        }
    }
}

impl UrlPolicy {
    /// Creates a new `UrlPolicy` with default secure settings:
    /// HTTPS only, private IPs blocked, no host restrictions, max 3 redirects.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enforces or relaxes the HTTPS requirement.
    #[must_use]
    pub fn with_https_only(mut self, https_only: bool) -> Self {
        self.https_only = https_only;
        self
    }

    /// Restricts permissible endpoints to an explicit allow-list of hostnames.
    #[must_use]
    pub fn with_allowed_hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_hosts = Some(hosts.into_iter().map(Into::into).collect());
        self
    }

    /// Enables or disables blocking of private, loopback, and internal IP addresses.
    #[must_use]
    pub fn with_block_private_ips(mut self, block: bool) -> Self {
        self.block_private_ips = block;
        self
    }

    /// Configures the maximum allowable HTTP redirects.
    #[must_use]
    pub fn with_max_redirects(mut self, max: usize) -> Self {
        self.max_redirects = max;
        self
    }

    /// Validates a parsed [`reqwest::Url`] against this policy.
    ///
    /// # Returns
    /// - `Ok(())` if the URL complies with all active policy constraints.
    /// - `Err(String)` describing the specific policy violation if rejected.
    ///
    /// # Limitations & SSRF Defense-in-Depth
    ///
    /// Note that IP-based filtering evaluates literal IP addresses in URLs. It does not perform
    /// synchronous DNS resolution at check time and therefore cannot prevent Time-of-Check to
    /// Time-of-Use (TOCTOU) DNS rebinding attacks where a hostile public domain name resolves to
    /// an internal IP. For zero-trust environments, combine `UrlPolicy` with network-level
    /// firewall/proxy egress filtering.
    pub fn check(&self, url: &reqwest::Url) -> std::result::Result<(), String> {
        if self.https_only && url.scheme() != "https" {
            return Err(format!("{url}: https required"));
        }

        if let Some(allowed) = &self.allowed_hosts {
            let host = url.host_str().unwrap_or_default();
            if !allowed.iter().any(|a| a.eq_ignore_ascii_case(host)) {
                return Err(format!("host {host} is not allow-listed"));
            }
        }

        if self.block_private_ips {
            if let Some(host_str) = url.host_str() {
                let clean_host = host_str.trim_start_matches('[').trim_end_matches(']');
                if clean_host.eq_ignore_ascii_case("localhost") {
                    return Err(format!("{clean_host} is a non-public address"));
                }
                if let Ok(ip) = clean_host.parse::<std::net::IpAddr>() {
                    match ip {
                        std::net::IpAddr::V4(ipv4) => {
                            if ipv4.is_loopback()
                                || ipv4.is_private()
                                || ipv4.is_link_local()
                                || ipv4.is_unspecified()
                                || ipv4.is_broadcast()
                            {
                                return Err(format!("{ipv4} is a non-public address"));
                            }
                        }
                        std::net::IpAddr::V6(ipv6) => {
                            if let Some(ipv4) = ipv6.to_ipv4_mapped() {
                                if ipv4.is_loopback()
                                    || ipv4.is_private()
                                    || ipv4.is_link_local()
                                    || ipv4.is_unspecified()
                                    || ipv4.is_broadcast()
                                {
                                    return Err(format!("{ipv6} is a non-public address"));
                                }
                            }
                            let seg0 = ipv6.segments()[0];
                            let is_ula = (seg0 & 0xfe00) == 0xfc00;
                            let is_link_local = (seg0 & 0xffc0) == 0xfe80;
                            if ipv6.is_loopback()
                                || ipv6.is_unspecified()
                                || is_ula
                                || is_link_local
                            {
                                return Err(format!("{ipv6} is a non-public address"));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Helper to parse and check a URL string against this policy.
    pub fn check_url_str(&self, url_str: &str) -> std::result::Result<(), String> {
        let parsed =
            reqwest::Url::parse(url_str).map_err(|e| format!("invalid URL {url_str:?}: {e}"))?;
        self.check(&parsed)
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
    min_expected_events: usize,
    max_retries: usize,
    backoff_initial_delay: Duration,
    max_backoff: Duration,
    max_retry_after: Duration,
    overall_timeout: Option<Duration>,
    integrity_validator: Option<CalendarIntegrityValidator>,
    user_agent: String,
    url_policy: Option<UrlPolicy>,
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
            .field("min_expected_events", &self.min_expected_events)
            .field("max_retries", &self.max_retries)
            .field("backoff_initial_delay", &self.backoff_initial_delay)
            .field("max_backoff", &self.max_backoff)
            .field("max_retry_after", &self.max_retry_after)
            .field("overall_timeout", &self.overall_timeout)
            .field(
                "integrity_validator",
                &self
                    .integrity_validator
                    .as_ref()
                    .map(|_| "<custom_validator>"),
            )
            .field("user_agent", &self.user_agent)
            .field("url_policy", &self.url_policy)
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
    /// Returns the default platform cache directory for redfolder, derived safely from the environment.
    ///
    /// Refuses to fall back to a shared system temporary directory to prevent cache poisoning
    /// and local privilege escalation vulnerabilities on multi-user hosts.
    pub fn try_default_cache_dir() -> Result<PathBuf> {
        #[cfg(windows)]
        {
            if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
                return Ok(PathBuf::from(local_app_data)
                    .join("redfolder")
                    .join("cache"));
            }
            if let Ok(app_data) = std::env::var("APPDATA") {
                return Ok(PathBuf::from(app_data).join("redfolder").join("cache"));
            }
            if let Ok(user_profile) = std::env::var("USERPROFILE") {
                return Ok(PathBuf::from(user_profile).join(".cache").join("redfolder"));
            }
        }

        #[cfg(not(windows))]
        {
            if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
                return Ok(PathBuf::from(xdg).join("redfolder"));
            }
            if let Ok(home) = std::env::var("HOME") {
                return Ok(PathBuf::from(home).join(".cache").join("redfolder"));
            }
        }

        Err(crate::error::RedFolderError::Config(
            "no standard user cache directory found in environment (XDG_CACHE_HOME, HOME, LOCALAPPDATA); refusing to fall back to shared temp directory".into(),
        ))
    }

    /// Returns the default platform cache directory for redfolder.
    ///
    /// Deprecated: may fall back to a shared temporary directory if environment variables are missing.
    /// Prefer [`try_default_cache_dir`](Self::try_default_cache_dir).
    #[deprecated(note = "may fall back to a shared temp directory; use try_default_cache_dir()")]
    #[must_use]
    pub fn default_cache_dir() -> PathBuf {
        Self::try_default_cache_dir().unwrap_or_else(|_| {
            warn!(
                "falling back to system temp directory for calendar cache. On shared/multi-user systems, consider configuring an explicit cache path or XDG_CACHE_HOME/LOCALAPPDATA to prevent cache tampering"
            );
            std::env::temp_dir().join("redfolder_cache")
        })
    }

    /// Create a new `CalendarClient` with an optional cache directory fallibly.
    pub fn try_new(cache_dir: Option<PathBuf>) -> Result<Self> {
        let dir = match cache_dir {
            Some(d) => Some(d),
            None => Self::try_default_cache_dir().ok(),
        };
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
    /// If `None`, attempts to safely derive platform default user cache or runs without cache.
    #[must_use]
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        Self::try_new(cache_dir.clone()).unwrap_or_else(|err| {
            error!(err=%err, "failed to build configured HTTP client; falling back to default with User-Agent");
            let dir = match cache_dir {
                Some(d) => Some(d),
                None => Self::try_default_cache_dir().ok(),
            };
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
        let dir = match cache_dir {
            Some(d) => Some(d),
            None => Self::try_default_cache_dir().ok(),
        };
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(user_agent)
            .build()?;
        let mut client_obj = Self::with_options(client, CALENDAR_URL, dir, Duration::from_secs(30));
        client_obj.user_agent = user_agent.to_string();
        Ok(client_obj)
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
            min_expected_events: 1,
            max_retries: 2,
            backoff_initial_delay: Duration::from_millis(500),
            max_backoff: DEFAULT_MAX_BACKOFF,
            max_retry_after: Duration::from_secs(10),
            overall_timeout: Some(DEFAULT_OVERALL_TIMEOUT),
            integrity_validator: None,
            user_agent: DEFAULT_USER_AGENT.to_string(),
            url_policy: None,
        }
    }

    /// Returns the active [`UrlPolicy`], if configured.
    #[must_use]
    pub fn url_policy(&self) -> Option<&UrlPolicy> {
        self.url_policy.as_ref()
    }

    /// Returns the configured User-Agent string.
    #[must_use]
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// Configures a [`UrlPolicy`] for URL validation and redirect handling.
    ///
    /// Validates the current primary calendar URL and any configured fallback URLs against the policy,
    /// and configures the underlying HTTP client with custom redirect handling enforcing `max_redirects`
    /// and redirect target policy compliance.
    pub fn with_url_policy(mut self, policy: UrlPolicy) -> Result<Self> {
        policy
            .check_url_str(&self.calendar_url)
            .map_err(crate::error::RedFolderError::Calendar)?;
        for fallback in &self.fallback_urls {
            policy
                .check_url_str(fallback)
                .map_err(crate::error::RedFolderError::Calendar)?;
        }

        let p = policy.clone();
        self.client = Client::builder()
            .user_agent(&self.user_agent)
            .timeout(self.request_timeout)
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= p.max_redirects {
                    return attempt.error("too many redirects");
                }
                match p.check(attempt.url()) {
                    Ok(()) => attempt.follow(),
                    Err(why) => attempt.error(why),
                }
            }))
            .build()?;
        self.url_policy = Some(policy);
        Ok(self)
    }

    /// Configures a custom User-Agent string and rebuilds the HTTP client preserving active timeout and redirect policy.
    pub fn with_user_agent_str(mut self, user_agent: impl Into<String>) -> Result<Self> {
        self.user_agent = user_agent.into();
        let mut builder = Client::builder()
            .user_agent(&self.user_agent)
            .timeout(self.request_timeout);
        if let Some(ref policy) = self.url_policy {
            let p = policy.clone();
            builder = builder.redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= p.max_redirects {
                    return attempt.error("too many redirects");
                }
                match p.check(attempt.url()) {
                    Ok(()) => attempt.follow(),
                    Err(why) => attempt.error(why),
                }
            }));
        }
        self.client = builder.build()?;
        Ok(self)
    }

    /// Adds a fallback calendar URL after validating it against the configured [`UrlPolicy`] (if any).
    pub fn try_with_fallback_url(mut self, url: impl Into<String>) -> Result<Self> {
        let u = url.into();
        if let Some(ref policy) = self.url_policy {
            policy
                .check_url_str(&u)
                .map_err(crate::error::RedFolderError::Calendar)?;
        }
        self.fallback_urls.push(u);
        Ok(self)
    }

    /// Add a fallback calendar endpoint URL used if the primary URL fails.
    #[must_use]
    pub fn with_fallback_url(mut self, url: impl Into<String>) -> Self {
        let u = url.into();
        if let Some(ref policy) = self.url_policy {
            if let Err(why) = policy.check_url_str(&u) {
                warn!(url = %u, err = %why, "fallback URL violates active UrlPolicy");
            }
        }
        self.fallback_urls.push(u);
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
        let u = url.into();
        if let Some(ref policy) = self.url_policy {
            if let Err(why) = policy.check_url_str(&u) {
                warn!(url = %u, err = %why, "primary calendar URL violates active UrlPolicy");
            }
        }
        self.calendar_url = u;
    }

    /// Add a fallback calendar URL.
    pub fn add_fallback_url(&mut self, url: impl Into<String>) {
        let u = url.into();
        if let Some(ref policy) = self.url_policy {
            if let Err(why) = policy.check_url_str(&u) {
                warn!(url = %u, err = %why, "fallback URL violates active UrlPolicy");
            }
        }
        self.fallback_urls.push(u);
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

    /// Configure the plausibility floor for raw calendar events (defaults to 1).
    /// Responses with fewer events than this threshold are treated as empty feeds.
    #[must_use]
    pub fn with_min_expected_events(mut self, min_events: usize) -> Self {
        self.min_expected_events = min_events;
        self
    }

    /// Set minimum expected events plausibility floor.
    pub fn set_min_expected_events(&mut self, min_events: usize) {
        self.min_expected_events = min_events;
    }

    /// Minimum expected events plausibility floor.
    #[must_use]
    pub fn min_expected_events(&self) -> usize {
        self.min_expected_events
    }

    /// Configure maximum HTTP retry attempts for transient errors (429, 408, 5xx), capped at [`MAX_RETRIES_LIMIT`].
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries.min(MAX_RETRIES_LIMIT);
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

    /// Configure the maximum backoff delay duration.
    #[must_use]
    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Configured maximum backoff delay duration.
    #[must_use]
    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
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

    /// Explicitly remove any overall wall-clock timeout ceiling.
    #[must_use]
    pub fn without_overall_timeout(mut self) -> Self {
        self.overall_timeout = None;
        self
    }

    /// Overall wall-clock timeout ceiling if set.
    #[must_use]
    pub fn overall_timeout(&self) -> Option<Duration> {
        self.overall_timeout
    }

    /// Calculate exponential backoff delay for the given retry attempt, capped at [`max_backoff`](Self::max_backoff).
    #[must_use]
    pub fn backoff_delay(&self, attempt: usize) -> Duration {
        let factor = 1u32 << attempt.min(16);
        self.backoff_initial_delay
            .saturating_mul(factor)
            .min(self.max_backoff)
    }

    /// Returns the candidate URLs to try in priority order (primary URL followed by fallback URLs),
    /// with duplicates removed.
    #[must_use]
    pub fn candidate_urls(&self) -> Vec<&str> {
        let mut seen = std::collections::HashSet::new();
        std::iter::once(self.calendar_url.as_str())
            .chain(self.fallback_urls.iter().map(String::as_str))
            .filter(|u| seen.insert(*u))
            .collect()
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
    /// Worst-case wall-clock latency per URL candidate is bounded by:
    /// `Σ (request_timeout) + Σ backoff_delay(attempt)`.
    /// With default settings (30s request timeout, 2 retries, 0.5s/1s backoff, 10s max backoff),
    /// worst-case latency under network timeouts is ~91.5s per URL candidate.
    /// Under repeated 429 status responses with `Retry-After >= 10s`, latency is ~21s per URL candidate.
    ///
    /// An [`overall_timeout`](Self::with_overall_timeout) (default 60s) caps the total elapsed time across
    /// all URL candidates and retries.
    pub async fn fetch_remote_with_stats(&self) -> Result<(Vec<RawCalendarEvent>, IngestStats)> {
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

    /// Fetch fresh calendar events directly from the remote API with bounded retry, response size checks,
    /// and fallback URL failover.
    ///
    /// # Latency Ceiling Guarantee
    ///
    /// Worst-case wall-clock latency per URL candidate is bounded by:
    /// `Σ (request_timeout) + Σ backoff_delay(attempt)`.
    /// With default settings (30s request timeout, 2 retries, 0.5s/1s backoff, 10s max backoff),
    /// worst-case latency under network timeouts is ~91.5s per URL candidate.
    /// Under repeated 429 status responses with `Retry-After >= 10s`, latency is ~21s per URL candidate.
    ///
    /// An [`overall_timeout`](Self::with_overall_timeout) (default 60s) caps the total elapsed time across
    /// all URL candidates and retries.
    pub async fn fetch_remote(&self) -> Result<Vec<RawCalendarEvent>> {
        Ok(self.fetch_remote_with_stats().await?.0)
    }

    async fn fetch_remote_candidates(&self) -> Result<(Vec<RawCalendarEvent>, IngestStats)> {
        let candidate_urls = self.candidate_urls();
        let deadline =
            tokio::time::Instant::now() + self.overall_timeout.unwrap_or(Duration::from_secs(3600));

        let mut candidate_errors: Vec<(String, String)> = Vec::new();

        'candidates: for (url_idx, &url) in candidate_urls.iter().enumerate() {
            #[cfg(test)]
            {
                if std::env::var("REDFOLDER_TEST_OFFLINE").as_deref() == Ok("1") {
                    assert!(
                        !url.contains("faireconomy"),
                        "Hermetic CI violation: attempt to fetch remote live feed at {url} while REDFOLDER_TEST_OFFLINE=1"
                    );
                }
            }
            debug!(url=%url, url_idx, "fetching economic calendar");
            let mut url_error = None;

            if let Some(ref policy) = self.url_policy {
                match reqwest::Url::parse(url) {
                    Ok(parsed_url) => {
                        if let Err(why) = policy.check(&parsed_url) {
                            warn!(url = %url, err = %why, "calendar endpoint rejected by URL policy");
                            candidate_errors.push((url.to_string(), why));
                            continue 'candidates;
                        }
                    }
                    Err(e) => {
                        let why = format!("invalid URL {url:?}: {e}");
                        warn!(url = %url, err = %why, "calendar endpoint URL cannot be parsed");
                        candidate_errors.push((url.to_string(), why));
                        continue 'candidates;
                    }
                }
            }

            for attempt in 0..=self.max_retries {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    let err = url_error.unwrap_or_else(|| {
                        crate::error::RedFolderError::Calendar(
                            "overall timeout deadline expired".into(),
                        )
                    });
                    candidate_errors.push((url.to_string(), err.to_string()));
                    break 'candidates;
                }

                match self
                    .client
                    .get(url)
                    .timeout(self.request_timeout.min(remaining))
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
                                    let delay = self.backoff_delay(attempt);
                                    let remaining = deadline
                                        .saturating_duration_since(tokio::time::Instant::now());
                                    tokio::time::sleep(delay.min(remaining)).await;
                                    continue;
                                }
                                break;
                            }

                            match serde_json::from_slice::<Vec<serde_json::Value>>(&body_bytes) {
                                Ok(raw_items) => {
                                    let total_count = raw_items.len();
                                    if total_count > self.max_event_count {
                                        let msg = format!(
                                            "upstream response contained {total_count} events, exceeding limit of {}",
                                            self.max_event_count
                                        );
                                        url_error = Some(crate::error::RedFolderError::Calendar(
                                            format!("{msg} (url: {url})"),
                                        ));
                                        break;
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
                                        let msg = "all upstream calendar events were malformed";
                                        url_error = Some(crate::error::RedFolderError::Calendar(
                                            format!("{msg} (url: {url})"),
                                        ));
                                        break;
                                    }

                                    if malformed_count > 0 {
                                        warn!(
                                            malformed = %malformed_count,
                                            total = %total_count,
                                            "some upstream events failed validation"
                                        );
                                        if malformed_count * 5 > total_count {
                                            let msg = format!(
                                                "upstream response corrupted: {malformed_count}/{total_count} events malformed"
                                            );
                                            url_error =
                                                Some(crate::error::RedFolderError::Calendar(
                                                    format!("{msg} (url: {url})"),
                                                ));
                                            break;
                                        }
                                    }

                                    // Run pluggable integrity validator if configured
                                    if let Some(ref validator) = self.integrity_validator {
                                        if let Err(e) = validator(&events) {
                                            warn!(url = %url, err = %e, "integrity validator rejected payload");
                                            url_error = Some(e);
                                            break;
                                        }
                                    }

                                    let stats = IngestStats {
                                        received: total_count,
                                        malformed: malformed_count,
                                        kept: events.len(),
                                        unparseable: 0,
                                    };
                                    info!(url=%url, ?stats, "downloaded calendar events successfully");
                                    return Ok((events, stats));
                                }
                                Err(e) => {
                                    warn!(err = %e, attempt, url = %url, "failed to parse calendar response JSON");
                                    url_error = Some(crate::error::RedFolderError::Json(e));
                                    if attempt < self.max_retries {
                                        let delay = self.backoff_delay(attempt);
                                        let remaining = deadline
                                            .saturating_duration_since(tokio::time::Instant::now());
                                        tokio::time::sleep(delay.min(remaining)).await;
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

                            let delay =
                                retry_after_duration.unwrap_or_else(|| self.backoff_delay(attempt));
                            debug!(delay = ?delay, "sleeping before retrying rate-limited or transient failure");
                            let remaining =
                                deadline.saturating_duration_since(tokio::time::Instant::now());
                            tokio::time::sleep(delay.min(remaining)).await;
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!(err = %e, attempt, url = %url, "calendar HTTP transport request failed");
                        url_error = Some(crate::error::RedFolderError::Http(e));
                        if attempt < self.max_retries {
                            let delay = self.backoff_delay(attempt);
                            let remaining =
                                deadline.saturating_duration_since(tokio::time::Instant::now());
                            tokio::time::sleep(delay.min(remaining)).await;
                            continue;
                        }
                    }
                }
            }

            if let Some(err) = url_error {
                warn!(url=%url, err=%err, "calendar candidate endpoint failed; trying next fallback if available");
                candidate_errors.push((url.to_string(), err.to_string()));
            }
        }

        if candidate_errors.is_empty() {
            Err(crate::error::RedFolderError::Calendar(
                "fetch failed after retries on all candidate endpoints".into(),
            ))
        } else {
            let details: Vec<String> = candidate_errors
                .into_iter()
                .map(|(u, e)| format!("{u}: {e}"))
                .collect();
            Err(crate::error::RedFolderError::Calendar(format!(
                "all calendar endpoints failed: [{}]",
                details.join("; ")
            )))
        }
    }

    /// Save raw calendar events to the local disk cache atomically (if cache path is set).
    ///
    /// On Unix systems, applies restrictive file permissions (0600) and directory permissions (0700)
    /// to prevent unauthorized access or tampering on shared multi-user hosts.
    pub fn save_cache(&self, events: &[RawCalendarEvent]) -> Result<()> {
        self.save_cache_at(events, Utc::now())
    }

    /// Like `save_cache` but records an explicit `fetched_at`. Intended for tests and migrations.
    pub fn save_cache_at(
        &self,
        events: &[RawCalendarEvent],
        fetched_at: DateTime<Utc>,
    ) -> Result<()> {
        let Some(path) = &self.cache_path else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if parent.file_name() == Some(std::ffi::OsStr::new("redfolder")) {
                        let _ = std::fs::set_permissions(
                            parent,
                            std::fs::Permissions::from_mode(0o700),
                        );
                    }
                }
            }
            verify_private_dir(parent)?;
        }

        let now = fetched_at;
        let expires_at = self
            .cache_ttl
            .and_then(|ttl| chrono::Duration::from_std(ttl).ok().map(|d| now + d));

        let cache_digest = compute_cache_digest(2, now, events);

        let cached_data = CachedCalendarData {
            metadata: CacheMetadata {
                version: 2,
                fetched_at: now,
                expires_at,
                event_count: events.len(),
                sha256: Some(cache_digest),
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
                if let Some(p) = path.parent() {
                    let _ = std::fs::File::open(p).and_then(|d| d.sync_all());
                }
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            }

            Ok(())
        })();

        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(crate::error::RedFolderError::Io(e));
        }

        debug!(path=%path.display(), count=%events.len(), "saved calendar cache atomically with v2 metadata and digest");
        Ok(())
    }

    /// Load full cached calendar data including metadata, with recovery from legacy and corrupted caches.
    pub fn load_cache_data(&self) -> Option<CachedCalendarData> {
        let path = self.cache_path.as_ref()?;
        if !path.exists() {
            return None;
        }

        if let Some(parent) = path.parent() {
            if let Err(e) = verify_private_dir(parent) {
                error!(path=%path.display(), err=%e, "cache directory security verification failed; refusing to read cache");
                return None;
            }
        }

        if let Ok(meta) = std::fs::metadata(path) {
            if meta.len() > (self.max_response_bytes as u64).saturating_mul(2) {
                warn!(
                    path = %path.display(),
                    size = meta.len(),
                    limit = self.max_response_bytes * 2,
                    "cache file exceeds maximum size limit; rejecting corrupted cache"
                );
                return None;
            }
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
            if cached.metadata.version == 2 {
                if cached.metadata.event_count != cached.events.len() {
                    warn!(
                        path = %path.display(),
                        expected = cached.metadata.event_count,
                        actual = cached.events.len(),
                        "calendar cache event count mismatch; rejecting corrupted cache"
                    );
                    return None;
                }

                let Some(ref expected_sha) = cached.metadata.sha256 else {
                    warn!(
                        path = %path.display(),
                        "cache v2 missing mandatory sha256 digest; rejecting"
                    );
                    return None;
                };

                let actual_sha =
                    compute_cache_digest(2, cached.metadata.fetched_at, &cached.events);
                if actual_sha != *expected_sha {
                    warn!(
                        path = %path.display(),
                        expected = %expected_sha,
                        actual = %actual_sha,
                        "calendar cache v2 digest mismatch; rejecting tampered or corrupted cache"
                    );
                    return None;
                }

                if cached.events.len() > self.max_event_count {
                    warn!(
                        path = %path.display(),
                        count = cached.events.len(),
                        max = self.max_event_count,
                        "cache contains more events than allowed by max_event_count; rejecting"
                    );
                    return None;
                }

                for ev in &cached.events {
                    if ev.title.len() > 500 || ev.country.len() > 10 || ev.date.trim().is_empty() {
                        warn!(
                            path = %path.display(),
                            title_len = ev.title.len(),
                            country_len = ev.country.len(),
                            "cache contains malformed events violating ingestion bounds; rejecting"
                        );
                        return None;
                    }
                }

                if let Some(ref validator) = self.integrity_validator {
                    if let Err(e) = validator(&cached.events) {
                        warn!(path = %path.display(), err = %e, "integrity validator rejected cached payload");
                        return None;
                    }
                }

                debug!(path=%path.display(), count=%cached.events.len(), "loaded structured calendar cache v2");
                return Some(cached);
            } else if cached.metadata.version == 1 {
                if cached.metadata.event_count != cached.events.len() {
                    warn!(
                        path = %path.display(),
                        expected = cached.metadata.event_count,
                        actual = cached.events.len(),
                        "calendar cache event count mismatch; rejecting corrupted cache"
                    );
                    return None;
                }

                let Some(ref expected_sha) = cached.metadata.sha256 else {
                    warn!(
                        path = %path.display(),
                        "legacy cache v1 missing sha256 checksum; rejecting"
                    );
                    return None;
                };

                let actual_sha = compute_events_sha256(&cached.events);
                if actual_sha != *expected_sha {
                    warn!(
                        path = %path.display(),
                        expected = %expected_sha,
                        actual = %actual_sha,
                        "calendar cache v1 sha256 checksum mismatch; rejecting tampered cache"
                    );
                    return None;
                }

                if cached.events.len() > self.max_event_count {
                    warn!(
                        path = %path.display(),
                        count = cached.events.len(),
                        max = self.max_event_count,
                        "cache contains more events than allowed by max_event_count; rejecting"
                    );
                    return None;
                }

                for ev in &cached.events {
                    if ev.title.len() > 500 || ev.country.len() > 10 || ev.date.trim().is_empty() {
                        warn!(
                            path = %path.display(),
                            title_len = ev.title.len(),
                            country_len = ev.country.len(),
                            "cache contains malformed events violating ingestion bounds; rejecting"
                        );
                        return None;
                    }
                }

                if let Some(ref validator) = self.integrity_validator {
                    if let Err(e) = validator(&cached.events) {
                        warn!(path = %path.display(), err = %e, "integrity validator rejected cached payload");
                        return None;
                    }
                }

                debug!(path=%path.display(), count=%cached.events.len(), "loaded legacy calendar cache v1; will upgrade to v2 on next save");
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

    /// Check whether cached calendar data is acceptable as a degraded fallback.
    ///
    /// Returns `None` if acceptable, or `Some(reason)` explaining rejection.
    #[must_use]
    pub fn cache_rejection(&self, data: &CachedCalendarData, now: DateTime<Utc>) -> Option<String> {
        if data.events.is_empty() {
            return Some("cache contains no events".into());
        }
        if data.metadata.version == 0 {
            return Some("legacy cache has no fetch timestamp".into());
        }
        if data.metadata.version == 1 && data.metadata.sha256.is_none() {
            return Some("legacy cache v1 has no integrity checksum".into());
        }
        let Some(max_stale) = self.max_stale_cache_age else {
            return Some("stale-cache fallback is disabled".into());
        };
        let max =
            chrono::Duration::from_std(max_stale).unwrap_or_else(|_| chrono::Duration::hours(36));
        let age = now - data.metadata.fetched_at;
        if age < -MAX_CLOCK_SKEW {
            return Some(format!(
                "cache fetched_at {} is in the future",
                data.metadata.fetched_at
            ));
        }
        if age > max {
            return Some(format!(
                "cache is {}h old (limit {}h)",
                age.num_hours(),
                max.num_hours()
            ));
        }
        None
    }

    fn fallback_snapshot(
        &self,
        source: SnapshotSource,
        reason: String,
        stats: IngestStats,
    ) -> Option<CalendarSnapshot> {
        let data = self.load_cache_data()?;
        if let Some(why) = self.cache_rejection(&data, Utc::now()) {
            warn!(%why, %reason, "cache not acceptable as fallback");
            return None;
        }
        warn!(
            fetched_at = %data.metadata.fetched_at,
            %reason,
            ?source,
            "serving degraded calendar snapshot from cache"
        );
        Some(CalendarSnapshot {
            data_fetched_at: data.metadata.fetched_at,
            events: data.events,
            source,
            remote_error: Some(reason),
            stats,
        })
    }

    /// Fetch calendar snapshot with provenance metadata (source, fetched_at, stats).
    pub async fn fetch_snapshot(&self) -> Result<CalendarSnapshot> {
        self.fetch_snapshot_inner(true, true).await
    }

    /// Bypasses TTL cache and fallback: the caller explicitly wants remote data.
    pub async fn force_fetch_snapshot(&self) -> Result<CalendarSnapshot> {
        self.fetch_snapshot_inner(false, false).await
    }

    async fn fetch_snapshot_inner(
        &self,
        use_ttl: bool,
        allow_fallback: bool,
    ) -> Result<CalendarSnapshot> {
        // 1. TTL cache
        if use_ttl && self.cache_ttl.is_some() && self.is_cache_valid() {
            if let Some(c) = self.load_cache_data() {
                if !c.events.is_empty() {
                    return Ok(CalendarSnapshot {
                        stats: IngestStats {
                            received: c.metadata.event_count,
                            kept: c.events.len(),
                            ..Default::default()
                        },
                        data_fetched_at: c.metadata.fetched_at,
                        events: c.events,
                        source: SnapshotSource::TtlCache,
                        remote_error: None,
                    });
                }
            }
        }

        // 2. Remote
        match self.fetch_remote_with_stats().await {
            Ok((events, stats)) if events.len() >= self.min_expected_events => {
                let fetched_at = Utc::now();
                if let Err(e) = self.save_cache_at(&events, fetched_at) {
                    warn!(err = %e, "failed to persist calendar cache");
                }
                Ok(CalendarSnapshot {
                    events,
                    source: SnapshotSource::Remote,
                    data_fetched_at: fetched_at,
                    remote_error: None,
                    stats,
                })
            }
            Ok((events, stats)) => {
                let reason = if events.is_empty() {
                    "remote returned 0 events".to_string()
                } else {
                    format!(
                        "remote returned {} events (below plausibility floor of {})",
                        events.len(),
                        self.min_expected_events
                    )
                };
                if allow_fallback {
                    if let Some(s) = self.fallback_snapshot(
                        SnapshotSource::EmptyFeedCache,
                        reason.clone(),
                        stats,
                    ) {
                        return Ok(s);
                    }
                }
                Err(crate::error::RedFolderError::Calendar(format!(
                    "{reason} and no acceptable cache is available"
                )))
            }
            Err(e) => {
                if allow_fallback {
                    if let Some(s) = self.fallback_snapshot(
                        SnapshotSource::FallbackCache,
                        e.to_string(),
                        IngestStats::default(),
                    ) {
                        return Ok(s);
                    }
                }
                Err(e)
            }
        }
    }

    /// Fetch fresh calendar events directly from the remote API, bypassing cache.
    ///
    /// Discards provenance. Prefer [`fetch_snapshot`][Self::fetch_snapshot].
    pub async fn force_fetch(&self) -> Result<Vec<RawCalendarEvent>> {
        Ok(self.force_fetch_snapshot().await?.events)
    }

    /// Fetch fresh events from the remote API, automatically saving to cache on success,
    /// or falling back to local cache if the remote request fails.
    ///
    /// Discards provenance. Prefer [`fetch_snapshot`][Self::fetch_snapshot]; stale-cache
    /// fallback is indistinguishable from a fresh download here.
    pub async fn fetch_or_cached(&self) -> Result<Vec<RawCalendarEvent>> {
        Ok(self.fetch_snapshot().await?.events)
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
        assert_eq!(structured.metadata.version, 2);
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
    #[allow(deprecated)]
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
            .with_max_backoff(Duration::from_secs(8))
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
        assert_eq!(client.max_backoff(), Duration::from_secs(8));
        assert_eq!(client.overall_timeout(), Some(Duration::from_secs(15)));

        let client_no_timeout = client.without_overall_timeout();
        assert_eq!(client_no_timeout.overall_timeout(), None);
    }

    #[test]
    fn test_backoff_is_capped_and_never_overflows() {
        let client = CalendarClient::without_cache()
            .with_max_retries(1000)
            .with_max_backoff(Duration::from_secs(10));
        assert_eq!(client.max_retries(), MAX_RETRIES_LIMIT);
        assert_eq!(client.backoff_delay(0), Duration::from_millis(500));
        assert_eq!(client.backoff_delay(1), Duration::from_millis(1000));
        assert_eq!(client.backoff_delay(2), Duration::from_millis(2000));
        assert_eq!(client.backoff_delay(3), Duration::from_millis(4000));
        assert_eq!(client.backoff_delay(4), Duration::from_millis(8000));
        assert_eq!(client.backoff_delay(5), Duration::from_secs(10));
        assert_eq!(client.backoff_delay(999), Duration::from_secs(10));
    }

    #[test]
    fn test_duplicate_fallbacks_are_ignored() {
        let client = CalendarClient::without_cache()
            .with_fallback_url(CALENDAR_URL)
            .with_fallback_url("https://mirror1.internal/cal.json")
            .with_fallback_url("https://mirror1.internal/cal.json");
        let candidates = client.candidate_urls();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], CALENDAR_URL);
        assert_eq!(candidates[1], "https://mirror1.internal/cal.json");
    }

    #[tokio::test]
    async fn test_default_client_has_overall_timeout() {
        assert_eq!(
            CalendarClient::without_cache().overall_timeout(),
            Some(DEFAULT_OVERALL_TIMEOUT)
        );
    }

    #[test]
    fn test_url_policy_defaults_and_builder() {
        let policy = UrlPolicy::new();
        assert!(policy.https_only);
        assert_eq!(policy.allowed_hosts, None);
        assert!(policy.block_private_ips);
        assert_eq!(policy.max_redirects, 3);

        let customized = policy
            .with_https_only(false)
            .with_allowed_hosts(vec!["example.com", "api.internal.com"])
            .with_block_private_ips(false)
            .with_max_redirects(5);

        assert!(!customized.https_only);
        assert_eq!(
            customized.allowed_hosts,
            Some(vec![
                "example.com".to_string(),
                "api.internal.com".to_string()
            ])
        );
        assert!(!customized.block_private_ips);
        assert_eq!(customized.max_redirects, 5);
    }

    #[test]
    fn test_url_policy_https_enforcement() {
        let policy = UrlPolicy::default();
        let http_url = reqwest::Url::parse("http://example.com/calendar.json").unwrap();
        let https_url = reqwest::Url::parse("https://example.com/calendar.json").unwrap();

        assert!(policy.check(&http_url).is_err());
        assert!(policy.check(&https_url).is_ok());

        let relaxed = policy.with_https_only(false);
        assert!(relaxed.check(&http_url).is_ok());
    }

    #[test]
    fn test_url_policy_allowed_hosts_case_insensitive() {
        let policy =
            UrlPolicy::default().with_allowed_hosts(vec!["nfs.faireconomy.media", "Mirror.Org"]);

        let allowed1 = reqwest::Url::parse("https://NFS.FairEconomy.Media/calendar.json").unwrap();
        let allowed2 = reqwest::Url::parse("https://mirror.org/calendar.json").unwrap();
        let forbidden = reqwest::Url::parse("https://evil.attacker.com/calendar.json").unwrap();

        assert!(policy.check(&allowed1).is_ok());
        assert!(policy.check(&allowed2).is_ok());
        let err = policy.check(&forbidden).unwrap_err();
        assert!(err.contains("host evil.attacker.com is not allow-listed"));
    }

    #[test]
    fn test_url_policy_private_ip_blocking_ipv4() {
        let policy = UrlPolicy::default().with_https_only(false);

        // Loopback, private RFC1918, link-local, unspecified, broadcast, and localhost
        let blocked = [
            "http://127.0.0.1/cal.json",
            "http://127.0.1.1/cal.json",
            "http://10.0.0.1/cal.json",
            "http://172.16.0.1/cal.json",
            "http://192.168.1.1/cal.json",
            "http://169.254.169.254/latest/meta-data",
            "http://0.0.0.0/cal.json",
            "http://255.255.255.255/cal.json",
            "http://localhost/cal.json",
            "http://localhost:8080/cal.json",
        ];

        for url_str in blocked {
            let url = reqwest::Url::parse(url_str).unwrap();
            assert!(
                policy.check(&url).is_err(),
                "expected {url_str} to be rejected as private IP"
            );
        }

        // Public IP should pass
        let public_url = reqwest::Url::parse("http://8.8.8.8/cal.json").unwrap();
        assert!(policy.check(&public_url).is_ok());
    }

    #[test]
    fn test_url_policy_private_ip_blocking_ipv6() {
        let policy = UrlPolicy::default().with_https_only(false);

        let blocked = [
            "http://[::1]/cal.json",                    // loopback
            "http://[::]/cal.json",                     // unspecified
            "http://[fe80::1]/cal.json",                // link-local
            "http://[fc00::1]/cal.json",                // ULA
            "http://[fd12:3456:789a::1]/cal.json",      // ULA
            "http://[::ffff:127.0.0.1]/cal.json",       // IPv4-mapped loopback
            "http://[::ffff:10.0.0.1]/cal.json",        // IPv4-mapped private
            "http://[::ffff:169.254.169.254]/cal.json", // IPv4-mapped link-local
        ];

        for url_str in blocked {
            let url = reqwest::Url::parse(url_str).unwrap();
            assert!(
                policy.check(&url).is_err(),
                "expected IPv6 {url_str} to be rejected as private IP"
            );
        }

        // Public IPv6
        let public_v6 = reqwest::Url::parse("http://[2606:4700:4700::1111]/cal.json").unwrap();
        assert!(policy.check(&public_v6).is_ok());
    }

    #[test]
    fn test_url_policy_eager_client_validation() {
        let client_http = CalendarClient::without_cache();
        let mut client_http = client_http;
        client_http.set_calendar_url("http://insecure.internal/cal.json");

        // with_url_policy should reject the client because calendar_url is HTTP
        let res = client_http.with_url_policy(UrlPolicy::default());
        assert!(res.is_err());

        // Valid HTTPS client with policy
        let client_https = CalendarClient::without_cache()
            .with_url_policy(UrlPolicy::default())
            .unwrap();

        // try_with_fallback_url should reject HTTP fallback
        let fallback_err = client_https
            .clone()
            .try_with_fallback_url("http://insecure.fallback/cal.json");
        assert!(fallback_err.is_err());

        // try_with_fallback_url should accept valid HTTPS fallback
        let fallback_ok = client_https.try_with_fallback_url("https://secure.fallback/cal.json");
        assert!(fallback_ok.is_ok());
    }
}
