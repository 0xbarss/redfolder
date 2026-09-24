//! Custom Event Listener Callback Example.
//!
//! Demonstrates how to implement the `EventListener` trait to hook a custom
//! risk manager or logging sink into `RedFolderService`.

use redfolder::events::{EventListener, RedFolderEvent};
use redfolder::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// A custom risk auditor that handles events asynchronously
struct RiskAuditor {
    system_name: String,
}

#[async_trait::async_trait]
impl EventListener for RiskAuditor {
    async fn on_event(&self, event: &RedFolderEvent) {
        match event {
            RedFolderEvent::BlackoutWarning {
                window,
                minutes_until_start,
                worker_id,
            } => {
                println!(
                    "[{}] ⚠️ WARNING for {:?}: Event '{}' starts in {} mins!",
                    self.system_name,
                    worker_id,
                    window.summary_title(),
                    minutes_until_start
                );
            }
            RedFolderEvent::BlackoutStarted { window, worker_id } => {
                println!(
                    "[{}] 🚨 BLACKOUT STARTED for {:?}: '{}' active until {}.",
                    self.system_name,
                    worker_id,
                    window.summary_title(),
                    window.end.format("%H:%M UTC")
                );
            }
            RedFolderEvent::BlackoutEnded { worker_id, .. } => {
                println!(
                    "[{}] 🟢 BLACKOUT CLEARED for {:?}: Trading window reopened.",
                    self.system_name, worker_id
                );
            }
            RedFolderEvent::CalendarUpdated {
                total_events,
                total_windows,
            } => {
                println!(
                    "[{}] 📡 Calendar updated: {} events, {} blackout windows.",
                    self.system_name, total_events, total_windows
                );
            }
            RedFolderEvent::CalendarSyncFailed { error } => {
                println!("[{}] ❌ Calendar sync failed: {}", self.system_name, error);
            }
        }
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let service = RedFolderService::new(None);

    // Register our custom trait-based listener
    let auditor = Arc::new(RiskAuditor {
        system_name: "AuditLogger".into(),
    });
    service.add_listener(auditor).await;

    // Register a worker bot
    let config = RedFolderConfig::builder()
        .currencies(vec!["USD", "GBP"])
        .impacts(vec!["High"])
        .warning_minutes(10)
        .build();

    let _rx = service.register_worker_events("gbpusd_bot", config).await?;

    println!("Starting service with custom listener...");
    service.start().await?;

    sleep(Duration::from_millis(500)).await;
    service.stop().await;

    println!("Demo completed.");
    Ok(())
}
