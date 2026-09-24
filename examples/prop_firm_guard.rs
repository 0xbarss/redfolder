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

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize client
    let client = CalendarClient::new(None);
    let events = client.fetch_or_cached().await?;

    // 2. Use the built-in prop firm preset:
    //    - 8 major currencies (USD, EUR, GBP, JPY, CAD, AUD, NZD, CHF)
    //    - High-impact events only
    //    - 5-minute pre/post event blackout windows
    //    - Weekend market close curfew until Monday 00:00 UTC
    let prop_config = RedFolderConfig::prop_firm_strict();

    println!("--- RedFolder Prop Firm Risk Guard ---");
    println!("Currencies monitored: {:?}", prop_config.currencies);
    println!(
        "Buffer: {}m before, {}m after",
        prop_config.before_min, prop_config.after_min
    );
    println!("Weekend Curfew Mode: {}", prop_config.weekend_mode);

    let engine = BlackoutEngine::compile(&events, &[&prop_config], chrono::Utc::now());

    // 3. Simulated order placement check
    let test_symbols = [("EURUSD", "EUR"), ("GBPUSD", "GBP"), ("USDJPY", "JPY")];

    for (symbol, currency) in test_symbols {
        let pair_cfg = RedFolderConfig::builder()
            .currencies(vec![currency.to_string(), "USD".to_string()])
            .impacts(vec!["High"])
            .buffer_minutes(5, 5)
            .build();

        if engine.is_blackout(&pair_cfg) {
            let win = engine.current_window(&pair_cfg).unwrap();
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

    Ok(())
}
