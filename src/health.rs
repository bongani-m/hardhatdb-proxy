use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Two successes in a row mark a node up. Two failures in a row mark it down.
/// A node starts down.
pub struct Gate {
    healthy: AtomicBool,
    ok_streak: AtomicU32,
    fail_streak: AtomicU32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mark {
    None,
    Up,
    Down,
}

impl Gate {
    pub fn new() -> Self {
        Self {
            healthy: AtomicBool::new(false),
            ok_streak: AtomicU32::new(0),
            fail_streak: AtomicU32::new(0),
        }
    }

    pub fn is_up(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn observe(&self, ok: bool) -> Mark {
        if ok {
            self.fail_streak.store(0, Ordering::Relaxed);
            let n = self.ok_streak.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= 2 {
                self.ok_streak.store(2, Ordering::Relaxed);
                if !self.healthy.swap(true, Ordering::AcqRel) {
                    return Mark::Up;
                }
            }
            return Mark::None;
        }
        self.ok_streak.store(0, Ordering::Relaxed);
        let n = self.fail_streak.fetch_add(1, Ordering::Relaxed) + 1;
        if n >= 2 {
            self.fail_streak.store(2, Ordering::Relaxed);
            if self.healthy.swap(false, Ordering::AcqRel) {
                return Mark::Down;
            }
        }
        Mark::None
    }
}

/// HTTP 200 from the status line. Any other response, including a short read, is a failure.
pub fn http_status_ok(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    let line = text.split(['\r', '\n']).next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let Some(version) = parts.next() else {
        return false;
    };
    let Some(code) = parts.next() else {
        return false;
    };
    version.starts_with("HTTP/") && code == "200"
}

pub async fn check_ready(addr: &str) -> bool {
    match tokio::time::timeout(Duration::from_secs(1), read_status(addr)).await {
        Ok(Ok(ok)) => ok,
        _ => false,
    }
}

async fn read_status(addr: &str) -> std::io::Result<bool> {
    let mut stream = TcpStream::connect(addr).await?;
    let host = addr.rsplit_once(':').map(|(host, _)| host).unwrap_or(addr);
    let req = format!("GET /readyz HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(128);
    let mut tmp = [0u8; 128];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(2).any(|w| w == b"\r\n") || buf.len() >= 512 {
            break;
        }
    }
    Ok(http_status_ok(&buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_line_accepts_only_200() {
        assert!(http_status_ok(
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nok\n"
        ));
        assert!(http_status_ok(b"HTTP/1.0 200 OK\r\n"));
        assert!(!http_status_ok(b"HTTP/1.1 503 Service Unavailable\r\n\r\n"));
        assert!(!http_status_ok(b""));
        assert!(!http_status_ok(b"not http"));
    }

    #[test]
    fn two_results_change_the_gate() {
        let gate = Gate::new();
        assert_eq!(gate.observe(true), Mark::None);
        assert!(!gate.is_up());
        assert_eq!(gate.observe(true), Mark::Up);
        assert!(gate.is_up());
        assert_eq!(gate.observe(true), Mark::None);

        assert_eq!(gate.observe(false), Mark::None);
        assert!(gate.is_up());
        assert_eq!(gate.observe(false), Mark::Down);
        assert!(!gate.is_up());
        assert_eq!(gate.observe(false), Mark::None);

        assert_eq!(gate.observe(true), Mark::None);
        assert_eq!(gate.observe(false), Mark::None);
        assert!(!gate.is_up());
        assert_eq!(gate.observe(true), Mark::None);
        assert_eq!(gate.observe(true), Mark::Up);
    }
}
