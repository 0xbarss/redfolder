//! Quickstart example for `redfolder`.
//!
//! Fetches the current week's economic calendar from ForexFactory
//! and checks if there is an active trading blackout for USD.

use redfolder::calendar::CalendarClient;
use redfolder::config::RedFolderConfig;
use redfolder::engine::BlackoutEngine;
use redfolder::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize client with a local disk cache
    let cache_dir = std::env::temp_dir().join("redfolder_example");
    let client = CalendarClient::new(Some(cache_dir));

    println!("Fetching economic calendar events...");
    let raw_events = client.fetch_or_cached().await?;
    println!("Loaded {} events from calendar.", raw_events.len());

    // 2. Configure blackout rules (High impact USD releases, 30 min before/after)
    let config = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(30, 30)
        .build();

    // 3. Compile blackout engine
    let engine = BlackoutEngine::compile(&raw_events, &[&config], chrono::Utc::now());

    // 4. Inspect current trading status
    if engine.is_blackout(&config) {
        let window = engine.current_window(&config).unwrap();
        println!("🔴 TRADING BLACKOUT ACTIVE!");
        println!("   Event: {}", window.summary_title());
        println!("   Remaining: {} minutes", window.remaining_minutes());
    } else {
        println!("🟢 TRADING ALLOWED: No active blackout for USD.");
    }

    // 5. Query upcoming blackout windows for the next 24 hours
    let upcoming = engine.upcoming_blackouts(&config, 24);
    println!("\nUpcoming Blackout Windows (Next 24h):");
    for (i, w) in upcoming.iter().enumerate() {
        println!(
            "  {}. {} ({} -> {} UTC, duration: {}m)",
            i + 1,
            w.summary_title(),
            w.start.format("%Y-%m-%d %H:%M"),
            w.end.format("%H:%M"),
            w.duration_minutes()
        );
    }

    Ok(())
}
