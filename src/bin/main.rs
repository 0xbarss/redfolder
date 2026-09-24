use clap::{Parser, Subcommand};
use colored::Colorize;
use redfolder::calendar::CalendarClient;
use redfolder::config::RedFolderConfig;
use redfolder::engine::BlackoutEngine;
use redfolder::error::Result;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "redfolder",
    author = "0xbarss",
    version,
    about = "Economic calendar client and trading blackout engine for algorithmic traders & prop firms",
    long_about = "Monitors high-impact macroeconomic releases (ForexFactory / FairEconomy calendar)\nand manages dynamic blackout windows, protecting algorithmic bots and prop firm accounts\nagainst spread spikes and slippage."
)]
struct Cli {
    /// Custom cache directory for economic calendar data
    #[arg(long, global = true)]
    cache_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Check whether trading is currently allowed or blocked by a news blackout
    Status {
        /// Filter by currency (e.g. USD, EUR, GBP, or All)
        #[arg(short, long, default_value = "USD")]
        currency: String,

        /// Minimum impact level to consider (High, Medium, Low)
        #[arg(short, long, default_value = "High")]
        impact: String,

        /// Output results as JSON
        #[arg(long)]
        json: bool,
    },

    /// List upcoming economic blackout windows within a given time horizon
    Upcoming {
        /// Hours to look ahead
        #[arg(short = 'H', long, default_value_t = 24)]
        hours: u32,

        /// Filter by currency (e.g. USD, EUR, GBP, or All)
        #[arg(short, long, default_value = "USD")]
        currency: String,

        /// Minimum impact level to consider (High, Medium, Low)
        #[arg(short, long, default_value = "High")]
        impact: String,

        /// Output results as JSON
        #[arg(long)]
        json: bool,
    },

    /// Force a fresh download and cache synchronization from ForexFactory
    Sync,

    /// Watch blackout status in real-time with automatic countdowns
    Watch {
        /// Refresh interval in seconds
        #[arg(short, long, default_value_t = 5)]
        interval: u64,

        /// Filter by currency
        #[arg(short, long, default_value = "USD")]
        currency: String,

        /// Minimum impact level
        #[arg(short, long, default_value = "High")]
        impact: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cache_dir = cli.cache_dir.or_else(|| {
        std::env::var("HOME")
            .ok()
            .map(|h| PathBuf::from(h).join(".cache").join("redfolder"))
            .or_else(|| Some(std::env::temp_dir().join("redfolder_cache")))
    });

    let client = CalendarClient::new(cache_dir.clone());

    match cli.command {
        Commands::Status {
            currency,
            impact,
            json,
        } => {
            let events = client.fetch_or_cached().await?;
            let config = RedFolderConfig::builder()
                .currencies(vec![currency.clone()])
                .impacts(vec![impact])
                .build();

            let engine = BlackoutEngine::compile(&events, &[&config], chrono::Utc::now());
            let current = engine.current_window(&config);

            if json {
                let status_json = serde_json::json!({
                    "currency": currency,
                    "in_blackout": current.is_some(),
                    "active_window": current,
                });
                println!("{}", serde_json::to_string_pretty(&status_json)?);
            } else {
                match current {
                    Some(window) => {
                        println!(
                            "\n{}",
                            "========================================================"
                                .red()
                                .bold()
                        );
                        println!(
                            " {} {}",
                            "🔴 STATUS:".bold(),
                            "TRADING BLACKOUT ACTIVE".red().bold()
                        );
                        println!(
                            "{}",
                            "========================================================"
                                .red()
                                .bold()
                        );
                        println!(
                            " {} {} minutes",
                            "Remaining Time:".bold(),
                            window.remaining_minutes().to_string().yellow().bold()
                        );
                        println!(
                            " {} until {}",
                            window.start.format("%Y-%m-%d %H:%M UTC"),
                            window.end.format("%H:%M UTC").to_string().cyan()
                        );
                        println!("\n {}", "Triggered Events:".bold());
                        for ev in &window.events {
                            println!(
                                "  - [{}] {} ({}) @ {}",
                                ev.country.yellow(),
                                ev.title.white().bold(),
                                ev.impact.red(),
                                ev.event_time.format("%H:%M UTC")
                            );
                        }
                        println!();
                    }
                    None => {
                        println!(
                            "\n{}",
                            "========================================================"
                                .green()
                                .bold()
                        );
                        println!(
                            " {} {}",
                            "🟢 STATUS:".bold(),
                            "TRADING PERMITTED (NO ACTIVE BLACKOUT)".green().bold()
                        );
                        println!(
                            "{}",
                            "========================================================"
                                .green()
                                .bold()
                        );
                        println!(" Currency: {}", currency.cyan());
                        let upcoming = engine.upcoming_blackouts(&config, 12);
                        if let Some(next) = upcoming.first() {
                            let mins_until = (next.start - chrono::Utc::now()).num_minutes();
                            println!(
                                " Next Blackout in: {} mins ({})",
                                mins_until.to_string().yellow().bold(),
                                next.summary_title()
                            );
                        } else {
                            println!(" No blackouts scheduled within next 12 hours.");
                        }
                        println!();
                    }
                }
            }
        }

        Commands::Upcoming {
            hours,
            currency,
            impact,
            json,
        } => {
            let events = client.fetch_or_cached().await?;
            let config = RedFolderConfig::builder()
                .currencies(vec![currency.clone()])
                .impacts(vec![impact])
                .build();

            let engine = BlackoutEngine::compile(&events, &[&config], chrono::Utc::now());
            let upcoming = engine.upcoming_blackouts(&config, hours);

            if json {
                println!("{}", serde_json::to_string_pretty(&upcoming)?);
            } else {
                println!(
                    "\n{}",
                    format!("── Upcoming Blackout Windows (Next {hours} Hours, {currency}) ──")
                        .cyan()
                        .bold()
                );
                if upcoming.is_empty() {
                    println!(
                        " {}",
                        "No upcoming blackout windows in this time window.".green()
                    );
                } else {
                    for (i, w) in upcoming.iter().enumerate() {
                        let mins_until = (w.start - chrono::Utc::now()).num_minutes();
                        println!(
                            "\n{}. {} (in {} mins, duration: {} mins)",
                            i + 1,
                            w.summary_title().white().bold(),
                            mins_until.to_string().yellow(),
                            w.duration_minutes().to_string().cyan()
                        );
                        println!(
                            "   Window: {} -> {}",
                            w.start.format("%Y-%m-%d %H:%M UTC"),
                            w.end.format("%H:%M UTC")
                        );
                        for ev in &w.events {
                            println!(
                                "    • [{}] {} [{}]",
                                ev.country.yellow(),
                                ev.title,
                                ev.impact.red()
                            );
                        }
                    }
                }
                println!();
            }
        }

        Commands::Sync => {
            println!(
                "{}",
                "Syncing economic calendar from ForexFactory...".cyan()
            );
            let events = client.force_fetch().await?;
            if events.is_empty() {
                println!(
                    "{} {}",
                    "⚠".yellow().bold(),
                    "Remote calendar returned 0 events; preserved existing cache if present."
                        .yellow()
                );
            } else {
                println!(
                    "{} Downloaded and cached {} economic events.",
                    "✔".green().bold(),
                    events.len().to_string().yellow().bold()
                );
            }
            if let Some(path) = client.cache_path() {
                println!("  Cache file: {}", path.display().to_string().dimmed());
            }
        }

        Commands::Watch {
            interval,
            currency,
            impact,
        } => {
            println!(
                "{}",
                "Starting live RedFolder watcher (press Ctrl+C to exit)...".cyan()
            );
            let config = RedFolderConfig::builder()
                .currencies(vec![currency.clone()])
                .impacts(vec![impact])
                .build();

            let mut last_status = false;
            loop {
                let events = client.fetch_or_cached().await?;
                let engine = BlackoutEngine::compile(&events, &[&config], chrono::Utc::now());
                let current = engine.current_window(&config);
                let in_blackout = current.is_some();

                let now_str = chrono::Utc::now().format("%H:%M:%S UTC").to_string();

                if in_blackout != last_status {
                    last_status = in_blackout;
                    if in_blackout {
                        println!(
                            "\n[{}] 🚨 {}",
                            now_str,
                            "ENTERING BLACKOUT WINDOW!".red().bold()
                        );
                    } else {
                        println!(
                            "\n[{}] 🟢 {}",
                            now_str,
                            "BLACKOUT CLEARED. TRADING PERMITTED.".green().bold()
                        );
                    }
                }

                if let Some(w) = current {
                    print!(
                        "\r[{}] 🔴 IN BLACKOUT: {} (remaining: {}m)   ",
                        now_str,
                        w.summary_title().red().bold(),
                        w.remaining_minutes().to_string().yellow().bold()
                    );
                } else {
                    let upcoming = engine.upcoming_blackouts(&config, 12);
                    if let Some(next) = upcoming.first() {
                        let mins_until = (next.start - chrono::Utc::now()).num_minutes();
                        print!(
                            "\r[{}] 🟢 CLEAR: next blackout in {}m ({})   ",
                            now_str,
                            mins_until.to_string().yellow(),
                            next.summary_title().dimmed()
                        );
                    } else {
                        print!(
                            "\r[{}] 🟢 CLEAR: no blackout scheduled in next 12h   ",
                            now_str
                        );
                    }
                }
                std::io::Write::flush(&mut std::io::stdout()).ok();
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        }
    }

    Ok(())
}
