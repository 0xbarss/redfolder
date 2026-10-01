//! Prop Firm Challenge Guard Example.
//!
//! Demonstrates how to configure and enforce strict prop firm news trading rules
//! (such as FTMO, FundedNext, and The5ers rules):
//! - No trading 5 minutes before and after High-Impact ("Red Folder") events.
//! - Curfew during weekend market close.

use redfolder::calendar::CalendarClient;
use redfolder::config::RedFolderConfig;
use redfolder::engine::BlackoutEngine;
use redfolder::error::Result;
use redfolder::types::Currency;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize client
    let client = CalendarClient::new(None);
    let snap = client.fetch_snapshot().await?;

    // 2. Use the built-in prop firm preset:
    //    - 8 major currencies (USD, EUR, GBP, JPY, CAD, AUD, NZD, CHF)
    //    - High-impact events only
    //    - 5-minute pre/post event blackout windows
    //    - Weekend market close curfew until Monday 00:00 UTC
    //    - FailClosed safety policy
    let prop = RedFolderConfig::prop_firm_strict();

    println!("--- RedFolder Prop Firm Risk Guard ---");
    println!("Currencies monitored: {:?}", prop.currencies);
    println!(
        "Buffer: {}m before, {}m after",
        prop.before_min, prop.after_min
    );
    println!("Weekend Curfew Mode: {}", prop.weekend_mode);
    println!("Fail-Safe Mode: {:?}", prop.fail_safe_mode);

    let max_age = chrono::Duration::hours(36);
    let engine =
        BlackoutEngine::compile_snapshot(&snap, &[&prop], chrono::Utc::now(), None, max_age);

    // 3. Simulated order placement check
    let test_symbols = [("EURUSD", "EUR"), ("GBPUSD", "GBP"), ("USDJPY", "JPY")];

    for (symbol, quote_ccy) in test_symbols {
        let pair_cfg = RedFolderConfig {
            currencies: vec![Currency::from(quote_ccy), Currency::USD],
            ..prop.clone()
        };
        pair_cfg.validate()?;

        if let Some(win) = engine.status(&pair_cfg) {
            println!(
                "⛔ REJECT ORDER on {}: Active Red Folder news blackout ({}). Ends in {}m.",
                symbol,
                win.summary_title(),
                win.remaining_minutes()
            );
        } else {
            println!("✅ ALLOW ORDER on {}: Clear to execute.", symbol);
        }
    }

    if snap.is_degraded() {
        println!(
            "⚠ calendar served from {:?} ({})",
            snap.source, snap.data_fetched_at
        );
    }

    Ok(())
}
