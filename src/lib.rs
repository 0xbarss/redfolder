//! # redfolder
//!
//! Async economic calendar client and automated trading blackout engine for algorithmic traders and prop firms.
//!
//! `redfolder` monitors high-impact macroeconomic releases (ForexFactory / FairEconomy calendar)
//! and calculates dynamic trading blackout windows, protecting algorithmic bots and prop firm accounts
//! against sudden spread spikes and slippage.

pub mod calendar;
pub mod config;
pub mod curfew;
pub mod error;
pub mod types;

pub use calendar::{CalendarClient, RawCalendarEvent, CALENDAR_URL, DEFAULT_CACHE_FILENAME};
pub use config::{NewsConfig, RedFolderConfig, RedFolderConfigBuilder};
pub use curfew::{next_weekend_window, parse_time, weekend_window_title, WeekendMode};
pub use error::{RedFolderError, Result};
pub use types::{
    BlackoutNotification, BlackoutWindow, EconomicEvent, Impact, NewsEvent, WindowEvent,
};
