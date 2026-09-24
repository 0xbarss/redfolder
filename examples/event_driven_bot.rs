//! Event-driven algorithmic trading bot integration example.
//!
//! Demonstrates how an automated trading engine can subscribe to `RedFolderEvent`s
//! on a non-blocking asynchronous event loop, responding to pre-news warnings,
//! blackout starts, and blackout clearances in real time.

use redfolder::config::RedFolderConfig;
use redfolder::events::RedFolderEvent;
use redfolder::service::RedFolderService;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RedFolder Event-Driven Trading Bot Example ===");

    // 1. Initialize the background RedFolder service
    let service = RedFolderService::new(None);

    // 2. Configure strict news guard rules for EUR/USD:
    //    - Monitor USD and EUR releases
    //    - High impact events only
    //    - 15-minute pre/post buffer
    //    - 5-minute advance warning before the blackout window begins
    let config = RedFolderConfig::builder()
        .currencies(vec!["USD", "EUR"])
        .impacts(vec!["High"])
        .buffer_minutes(15, 15)
        .warning_minutes(5)
        .build();

    // 3. Register our worker bot and receive its typed event stream
    let mut event_rx = service.register_worker_events("eurusd_scalper", config).await;

    // 4. Also demonstrate subscribing a global listener (e.g. for Telegram/Discord alerts or logging)
    let mut global_bus = service.subscribe();

    // Spawn an asynchronous listener task on the global broadcast bus
    tokio::spawn(async move {
        while let Ok(event) = global_bus.recv().await {
            match &event {
                RedFolderEvent::CalendarUpdated { total_events, total_windows } => {
                    println!("[Audit Log] Economic calendar refreshed: {} events, {} active windows.", total_events, total_windows);
                }
                RedFolderEvent::BlackoutWarning { window, minutes_until_start, worker_id } => {
                    println!("[Risk Alert] Worker {:?}: Window '{}' begins in {} minutes!", worker_id, window.summary_title(), minutes_until_start);
                }
                RedFolderEvent::BlackoutStarted { window, worker_id } => {
                    println!("[Risk Alert] Worker {:?}: ENTERED blackout '{}'.", worker_id, window.summary_title());
                }
                RedFolderEvent::BlackoutEnded { window, worker_id } => {
                    println!("[Risk Alert] Worker {:?}: EXITED blackout '{}'.", worker_id, window.summary_title());
                }
            }
        }
    });

    println!("Bot registered. Listening for real-time blackout events...");

    // Simulated event processing loop in the trading bot
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                RedFolderEvent::BlackoutWarning { window, minutes_until_start, .. } => {
                    println!("\n⚠️  [BOT ACTION] ADVANCE WARNING: Blackout starts in {} minutes!", minutes_until_start);
                    println!("    Window: {}", window.summary_title());
                    println!("    -> Action: Cancelling pending limit orders...");
                    println!("    -> Action: Tightening stop-loss on open positions...");
                }
                RedFolderEvent::BlackoutStarted { window, .. } => {
                    println!("\n🚨 [BOT ACTION] HARD BLACKOUT ACTIVE: {}", window.summary_title());
                    println!("    Remaining: {} minutes", window.remaining_minutes());
                    println!("    -> Action: HALTING strategy execution.");
                    println!("    -> Action: Rejecting all new buy/sell signals.");
                }
                RedFolderEvent::BlackoutEnded { .. } => {
                    println!("\n🟢 [BOT ACTION] BLACKOUT CLEARED.");
                    println!("    -> Action: Resuming normal automated execution.");
                }
                _ => {}
            }
        }
    });

    // Start background syncing and checking
    service.start().await?;

    // Keep running for demonstration
    sleep(Duration::from_secs(2)).await;
    service.stop().await;

    println!("\nService stopped gracefully.");
    Ok(())
}
