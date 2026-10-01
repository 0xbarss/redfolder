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

        /// Impact tier(s) to include, comma-separated (e.g. High,Medium). Exact match: `Medium` does NOT include `High`.
        #[arg(short, long, value_delimiter = ',', default_value = "High")]
        impact: Vec<String>,

        /// Include this impact tier and everything above it (e.g. `Medium` = Medium + High). Overrides --impact.
        #[arg(long, conflicts_with = "impact")]
        min_impact: Option<String>,

        /// Enforce fail-closed safety policy (halts trading when calendar data is unavailable, empty, or stale)
        #[arg(long)]
        fail_closed: bool,

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

        /// Impact tier(s) to include, comma-separated (e.g. High,Medium). Exact match: `Medium` does NOT include `High`.
        #[arg(short, long, value_delimiter = ',', default_value = "High")]
        impact: Vec<String>,

        /// Include this impact tier and everything above it (e.g. `Medium` = Medium + High). Overrides --impact.
        #[arg(long, conflicts_with = "impact")]
        min_impact: Option<String>,

        /// Enforce fail-closed safety policy (halts trading when calendar data is unavailable, empty, or stale)
        #[arg(long)]
        fail_closed: bool,

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

        /// Impact tier(s) to include, comma-separated (e.g. High,Medium). Exact match: `Medium` does NOT include `High`.
        #[arg(short, long, value_delimiter = ',', default_value = "High")]
        impact: Vec<String>,

        /// Include this impact tier and everything above it (e.g. `Medium` = Medium + High). Overrides --impact.
        #[arg(long, conflicts_with = "impact")]
        min_impact: Option<String>,

        /// Enforce fail-closed safety policy (halts trading when calendar data is unavailable, empty, or stale)
        #[arg(long)]
        fail_closed: bool,
    },
}

fn build_cli_config(
    currency: String,
    impacts: Vec<String>,
    min_impact: Option<String>,
    fail_closed: bool,
) -> Result<RedFolderConfig> {
    let mut b = RedFolderConfig::builder().currencies(vec![currency]);
    b = match min_impact {
        Some(m) => b.min_impact(m.as_str()),
        None => b.impacts(impacts),
    };
    if fail_closed {
        b = b.fail_safe_mode(redfolder::types::FailSafeMode::FailClosed);
    }
    let cfg = b.try_build_strict().map_err(|e| {
        eprintln!("{}: {}", "Invalid configuration".red().bold(), e);
        e
    })?;
    for w in cfg.lint() {
        eprintln!("{}: {}", "Warning".yellow().bold(), w);
    }
    Ok(cfg)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cache_dir = cli
        .cache_dir
        .or_else(|| CalendarClient::try_default_cache_dir().ok());

    // Configure a 15-minute default cache TTL to prevent rate limit hammering
    let client = CalendarClient::new(cache_dir).with_ttl(Duration::from_secs(900));

    match cli.command {
        Commands::Status {
            currency,
            impact,
            min_impact,
            fail_closed,
            json,
        } => {
            let config = build_cli_config(currency.clone(), impact, min_impact, fail_closed)?;
            let snap = match client.fetch_snapshot().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!(
                        "{}: Failed to fetch calendar snapshot: {e}",
                        "Error".red().bold()
                    );
                    if fail_closed {
                        std::process::exit(2);
                    } else {
                        return Err(e);
                    }
                }
            };

            let max_age = chrono::Duration::hours(36);
            let engine = BlackoutEngine::compile_snapshot(
                &snap,
                &[&config],
                chrono::Utc::now(),
                None,
                max_age,
            );
            let current = engine.status(&config);
            let stale = engine.is_stale_at(chrono::Utc::now());

            if json {
                let status_json = serde_json::json!({
                    "currency": currency,
                    "in_blackout": current.is_some(),
                    "active_window": current,
                    "data": {
                        "source": snap.source,
                        "fetched_at": snap.data_fetched_at,
                        "age_minutes": snap.age_at(chrono::Utc::now()).num_minutes(),
                        "stale": stale,
                        "degraded": snap.is_degraded(),
                        "remote_error": snap.remote_error,
                        "ingest": snap.stats,
                    }
                });
                println!("{}", serde_json::to_string_pretty(&status_json)?);
            } else {
                if snap.is_degraded() {
                    eprintln!(
                        "{} using cached data from {} ({})",
                        "⚠".yellow().bold(),
                        snap.data_fetched_at.format("%Y-%m-%d %H:%M:%S UTC"),
                        snap.remote_error.as_deref().unwrap_or("degraded source")
                    );
                }
                match current.as_ref() {
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

            if fail_closed && (stale || snap.is_degraded()) {
                std::process::exit(2);
            } else if current.is_some() {
                std::process::exit(1);
            } else {
                std::process::exit(0);
            }
        }

        Commands::Upcoming {
            hours,
            currency,
            impact,
            min_impact,
            fail_closed,
            json,
        } => {
            let config = build_cli_config(currency.clone(), impact, min_impact, fail_closed)?;
            let snap = client.fetch_snapshot().await?;
            let max_age = chrono::Duration::hours(36);
            let engine = BlackoutEngine::compile_snapshot(
                &snap,
                &[&config],
                chrono::Utc::now(),
                None,
                max_age,
            );
            let upcoming = engine.upcoming_blackouts(&config, hours);

            if json {
                println!("{}", serde_json::to_string_pretty(&upcoming)?);
            } else {
                if snap.is_degraded() {
                    eprintln!(
                        "{} using cached data from {} ({})",
                        "⚠".yellow().bold(),
                        snap.data_fetched_at.format("%Y-%m-%d %H:%M:%S UTC"),
                        snap.remote_error.as_deref().unwrap_or("degraded source")
                    );
                }
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
            min_impact,
            fail_closed,
        } => {
            println!(
                "{}",
                "Starting live RedFolder watcher (press Ctrl+C to exit)...".cyan()
            );
            let config = build_cli_config(currency.clone(), impact, min_impact, fail_closed)?;
            let max_age = chrono::Duration::hours(36);

            let mut snap = client.fetch_snapshot().await?;
            let mut engine = BlackoutEngine::compile_snapshot(
                &snap,
                &[&config],
                chrono::Utc::now(),
                None,
                max_age,
            );
            let mut last_fetch = std::time::Instant::now();
            let mut last_status = false;

            loop {
                // Re-fetch calendar only when cache TTL (15 minutes) expires
                if last_fetch.elapsed() >= Duration::from_secs(900) {
                    match client.fetch_snapshot().await {
                        Ok(new_snap) => {
                            snap = new_snap;
                            engine = BlackoutEngine::compile_snapshot(
                                &snap,
                                &[&config],
                                chrono::Utc::now(),
                                None,
                                max_age,
                            );
                            last_fetch = std::time::Instant::now();
                        }
                        Err(e) => {
                            eprintln!(
                                "\n{}: Failed to refresh calendar: {e}",
                                "Warning".yellow().bold()
                            );
                        }
                    }
                }

                let current = engine.status(&config);
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
