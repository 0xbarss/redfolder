use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const DEAD_URL: &str = "http://127.0.0.1:9/calendar.json";

#[derive(Clone)]
pub enum Reply {
    Json(String),
    Status(u16),
    Hang,
}

pub struct MockServer {
    pub url: String,
    pub hits: Arc<AtomicUsize>,
}

/// Replies are consumed in order; the last one repeats.
pub async fn spawn_mock(script: Vec<Reply>) -> MockServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/calendar.json", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let n = h.fetch_add(1, Ordering::SeqCst);
            let reply = script[n.min(script.len() - 1)].clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let (status, body) = match reply {
                    Reply::Json(b) => (200, b),
                    Reply::Status(s) => (s, String::new()),
                    Reply::Hang => {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        return;
                    }
                };
                let resp = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    MockServer { url, hits }
}

pub fn usd_high_in(d: chrono::Duration) -> redfolder::RawCalendarEvent {
    redfolder::RawCalendarEvent {
        title: "US CPI".into(),
        country: "USD".into(),
        time: String::new(),
        impact: "High".into(),
        date: (chrono::Utc::now() + d).to_rfc3339(),
    }
}

pub fn feed_json(n: usize, date: impl Fn(usize) -> String) -> String {
    let items: Vec<_> = (0..n)
        .map(|i| {
            serde_json::json!({
                "title": format!("Event {i}"),
                "country": "USD",
                "date": date(i),
                "time": "",
                "impact": "High"
            })
        })
        .collect();
    serde_json::to_string(&items).unwrap()
}

pub fn offline_client(cache_dir: Option<std::path::PathBuf>) -> redfolder::CalendarClient {
    redfolder::CalendarClient::with_options(
        reqwest::Client::new(),
        DEAD_URL,
        cache_dir,
        Duration::from_secs(1),
    )
    .with_max_retries(0)
}

pub fn offline_service(cache_dir: Option<std::path::PathBuf>) -> redfolder::RedFolderService {
    redfolder::RedFolderService::with_client(offline_client(cache_dir))
}
