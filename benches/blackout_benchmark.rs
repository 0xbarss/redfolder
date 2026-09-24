use chrono::{Duration, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redfolder::calendar::RawCalendarEvent;
use redfolder::config::RedFolderConfig;
use redfolder::engine::BlackoutEngine;

fn generate_mock_events(count: usize) -> Vec<RawCalendarEvent> {
    let now = Utc::now();
    (0..count)
        .map(|i| {
            let offset_min = (i as i64) * 60;
            let date = (now + Duration::minutes(offset_min)).to_rfc3339();
            let country = match i % 4 {
                0 => "USD",
                1 => "EUR",
                2 => "GBP",
                _ => "JPY",
            };
            RawCalendarEvent {
                title: format!("Macro Release {i}"),
                country: country.to_string(),
                date,
                time: "".to_string(),
                impact: "High".to_string(),
            }
        })
        .collect()
}

fn bench_blackout_engine(c: &mut Criterion) {
    let events_50 = generate_mock_events(50);
    let config = RedFolderConfig::prop_firm_strict();
    let now = Utc::now();

    // 1. Benchmark compilation of 50 weekly events into blackout windows
    c.bench_function("engine_compile_50_events", |b| {
        b.iter(|| {
            BlackoutEngine::compile(black_box(&events_50), black_box(&[&config]), black_box(now))
        })
    });

    // 2. Benchmark runtime blackout query (is_blackout)
    let engine = BlackoutEngine::compile(&events_50, &[&config], now);
    c.bench_function("is_blackout_query", |b| {
        b.iter(|| engine.is_blackout(black_box(&config)))
    });

    // 3. Benchmark deterministic historical / future timestamp query (is_blackout_at)
    let target_time = now + Duration::hours(12);
    c.bench_function("is_blackout_at_query", |b| {
        b.iter(|| engine.is_blackout_at(black_box(&config), black_box(target_time)))
    });

    // 4. Benchmark per-worker dynamic window derivation
    c.bench_function("windows_for_config", |b| {
        b.iter(|| engine.windows_for_config(black_box(&config), black_box(now)))
    });
}

criterion_group!(benches, bench_blackout_engine);
criterion_main!(benches);
