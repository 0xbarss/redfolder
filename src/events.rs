use crate::types::BlackoutWindow;
use serde::{Deserialize, Serialize};

/// High-level domain events emitted by the `redfolder` event bus.
///
/// Designed to be consumed asynchronously by trading bots, MT5 execution bridges,
/// risk management subsystems, or notification webhooks (Telegram/Discord).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RedFolderEvent {
    /// Heads-up warning alert emitted $N$ minutes before a blackout window begins.
    ///
    /// Useful for cancelling pending limit orders, closing open scalps,
    /// or tightening stops before volatility spikes and spreads widen.
    BlackoutWarning {
        window: BlackoutWindow,
        minutes_until_start: i64,
        worker_id: Option<String>,
    },

    /// Emitted the exact moment a blackout window begins.
    ///
    /// Automated trading strategies should halt execution and reject new orders.
    BlackoutStarted {
        window: BlackoutWindow,
        worker_id: Option<String>,
    },

    /// Emitted the moment a blackout window expires and clears.
    ///
    /// Strategies can safely resume normal automated execution.
    BlackoutEnded {
        window: BlackoutWindow,
        worker_id: Option<String>,
    },

    /// Emitted when fresh economic calendar releases have been downloaded and synchronized.
    CalendarUpdated {
        total_events: usize,
        total_windows: usize,
    },

    /// Emitted when background or scheduled economic calendar synchronization fails.
    CalendarSyncFailed { error: String },
}

impl RedFolderEvent {
    /// Whether this event signals entering a blackout window.
    #[must_use]
    pub fn is_blackout_started(&self) -> bool {
        matches!(self, RedFolderEvent::BlackoutStarted { .. })
    }

    /// Whether this event signals the clearance of a blackout window.
    #[must_use]
    pub fn is_blackout_ended(&self) -> bool {
        matches!(self, RedFolderEvent::BlackoutEnded { .. })
    }

    /// Whether this event is a pre-blackout heads-up warning.
    #[must_use]
    pub fn is_warning(&self) -> bool {
        matches!(self, RedFolderEvent::BlackoutWarning { .. })
    }

    /// Whether this event signals a failed calendar synchronization.
    #[must_use]
    pub fn is_sync_failed(&self) -> bool {
        matches!(self, RedFolderEvent::CalendarSyncFailed { .. })
    }

    /// Returns the associated `BlackoutWindow` if applicable.
    #[must_use]
    pub fn window(&self) -> Option<&BlackoutWindow> {
        match self {
            RedFolderEvent::BlackoutWarning { window, .. } => Some(window),
            RedFolderEvent::BlackoutStarted { window, .. } => Some(window),
            RedFolderEvent::BlackoutEnded { window, .. } => Some(window),
            RedFolderEvent::CalendarUpdated { .. } | RedFolderEvent::CalendarSyncFailed { .. } => {
                None
            }
        }
    }

    /// Target worker ID if event is scoped to a specific worker.
    #[must_use]
    pub fn worker_id(&self) -> Option<&str> {
        match self {
            RedFolderEvent::BlackoutWarning { worker_id, .. } => worker_id.as_deref(),
            RedFolderEvent::BlackoutStarted { worker_id, .. } => worker_id.as_deref(),
            RedFolderEvent::BlackoutEnded { worker_id, .. } => worker_id.as_deref(),
            RedFolderEvent::CalendarUpdated { .. } | RedFolderEvent::CalendarSyncFailed { .. } => {
                None
            }
        }
    }
}

/// Asynchronous event listener trait for handling `RedFolderEvent` occurrences.
#[async_trait::async_trait]
pub trait EventListener: Send + Sync {
    /// Handle an incoming domain event.
    async fn on_event(&self, event: &RedFolderEvent);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    #[test]
    fn test_event_methods() {
        let now = Utc::now();
        let window = BlackoutWindow {
            start: now,
            end: now + Duration::minutes(30),
            events: vec![],
        };

        let started = RedFolderEvent::BlackoutStarted {
            window: window.clone(),
            worker_id: Some("eurusd".into()),
        };

        assert!(started.is_blackout_started());
        assert!(!started.is_blackout_ended());
        assert_eq!(started.worker_id(), Some("eurusd"));
        assert!(started.window().is_some());

        let ended = RedFolderEvent::BlackoutEnded {
            window: window.clone(),
            worker_id: None,
        };
        assert!(ended.is_blackout_ended());

        let warning = RedFolderEvent::BlackoutWarning {
            window,
            minutes_until_start: 5,
            worker_id: None,
        };
        assert!(warning.is_warning());

        let sync_failed = RedFolderEvent::CalendarSyncFailed {
            error: "connection timeout".into(),
        };
        assert!(sync_failed.is_sync_failed());
        assert!(!sync_failed.is_warning());
        assert_eq!(sync_failed.worker_id(), None);
        assert_eq!(sync_failed.window(), None);
    }
}
