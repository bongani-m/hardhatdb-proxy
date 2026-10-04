//! TCP proxy for one HardhatDB Raft group.
//!
//! Clients dial one address. Each new connection is copied to a node whose
//! `/readyz` has returned 200 twice in a row. That connection stays on the
//! node until the proxy closes it.

mod config;
mod health;
mod pick;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{AbortHandle, Id, JoinHandle};

use crate::config::Config;
use crate::health::{check_ready, Gate, Mark};
use crate::pick::{pick, Balance, Ready};

struct Node {
    id: String,
    mysql_addr: String,
    health_addr: String,
    gate: Gate,
    conns: Arc<AtomicUsize>,
    tasks: Mutex<HashMap<Id, AbortHandle>>,
}

struct Proxy {
    nodes: Vec<Node>,
    balance: Balance,
    interval: Duration,
    pending: Mutex<HashMap<Id, AbortHandle>>,
}

/// Holds one open-splice count until the connection ends, including cancellation.
struct Reserve(Arc<AtomicUsize>);

impl Reserve {
    fn new(conns: &Arc<AtomicUsize>) -> Self {
        conns.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(conns))
    }
}

impl Drop for Reserve {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Running {
    addr: std::net::SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Running {
    async fn stop(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

impl Proxy {
    fn new(cfg: Config) -> Self {
        let nodes = cfg
            .backends
            .into_iter()
            .map(|backend| Node {
                id: backend.id,
                mysql_addr: backend.mysql_addr,
                health_addr: backend.health_addr,
                gate: Gate::new(),
                conns: Arc::new(AtomicUsize::new(0)),
                tasks: Mutex::new(HashMap::new()),
            })
            .collect();
        Self {
            nodes,
            balance: cfg.balance,
            interval: cfg.check_interval,
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn choose(&self, exclude: Option<usize>) -> Option<usize> {
        let ready: Vec<Ready> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(index, node)| Some(*index) != exclude && node.gate.is_up())
            .map(|(index, node)| Ready {
                index,
                conns: node.conns.load(Ordering::Relaxed),
            })
            .collect();
        pick(self.balance, &ready)
    }

    fn take_pending(&self, id: Id) -> Option<AbortHandle> {
        self.pending.lock().unwrap().remove(&id)
    }

    fn abort_node(&self, index: usize) {
        let tasks = std::mem::take(&mut *self.nodes[index].tasks.lock().unwrap());
        for (_, handle) in tasks {
            handle.abort();
        }
    }

    fn abort_all(&self) {
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        for (_, handle) in pending {
            handle.abort();
        }
        for index in 0..self.nodes.len() {
            self.abort_node(index);
        }
    }
}

#[tokio::main]
async fn main() {
    let cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };
    if let Err(err) = serve(cfg).await {
        eprintln!("hardhatdb-proxy: {err}");
        std::process::exit(1);
    }
}

async fn serve(cfg: Config) -> std::io::Result<()> {
    let running = start(cfg).await?;
    eprintln!("hardhatdb-proxy: listening on {}", running.addr);
    shutdown_signal().await;
    running.stop().await;
    Ok(())
}

async fn start(cfg: Config) -> std::io::Result<Running> {
    let listener = TcpListener::bind(&cfg.listen).await?;
    let addr = listener.local_addr()?;
    let (shutdown, rx) = watch::channel(false);
    let proxy = Arc::new(Proxy::new(cfg));
    let task = tokio::spawn(run(listener, proxy, rx));
    Ok(Running {
        addr,
        shutdown,
        task,
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

async fn run(listener: TcpListener, proxy: Arc<Proxy>, mut shutdown: watch::Receiver<bool>) {
    let health_proxy = Arc::clone(&proxy);
    let mut health_shutdown = shutdown.clone();
    let health = tokio::spawn(async move {
        health_loop(health_proxy, &mut health_shutdown).await;
    });
    loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((sock, _)) => spawn_client(sock, Arc::clone(&proxy)),
                    Err(err) => {
                        eprintln!("hardhatdb-proxy: accept: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
    drop(listener);
    proxy.abort_all();
    let _ = health.await;
}

async fn health_loop(proxy: Arc<Proxy>, shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        check_all(&proxy).await;
        tokio::select! {
            _ = tokio::time::sleep(proxy.interval) => {}
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn check_all(proxy: &Proxy) {
    let mut set = tokio::task::JoinSet::new();
    for (index, node) in proxy.nodes.iter().enumerate() {
        let addr = node.health_addr.clone();
        set.spawn(async move { (index, check_ready(&addr).await) });
    }
    while let Some(joined) = set.join_next().await {
        let Ok((index, ok)) = joined else {
            continue;
        };
        match proxy.nodes[index].gate.observe(ok) {
            Mark::Up => eprintln!("hardhatdb-proxy: {} up", proxy.nodes[index].id),
            Mark::Down => {
                eprintln!("hardhatdb-proxy: {} down", proxy.nodes[index].id);
                proxy.abort_node(index);
            }
            Mark::None => {}
        }
    }
}

fn spawn_client(client: TcpStream, proxy: Arc<Proxy>) {
    let _ = client.set_nodelay(true);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task_proxy = Arc::clone(&proxy);
    let task = tokio::spawn(async move {
        let Ok(id) = rx.await else {
            return;
        };
        run_client(client, task_proxy, id).await;
    });
    let id = task.id();
    proxy
        .pending
        .lock()
        .unwrap()
        .insert(id, task.abort_handle());
    let _ = tx.send(id);
}

async fn run_client(mut client: TcpStream, proxy: Arc<Proxy>, id: Id) {
    let Some(abort) = proxy.take_pending(id) else {
        return;
    };
    let Some((idx, mut server, reserved)) = open_backend(&proxy).await else {
        return;
    };
    if !proxy.nodes[idx].gate.is_up() {
        return;
    }
    let _ = server.set_nodelay(true);
    proxy.nodes[idx].tasks.lock().unwrap().insert(id, abort);
    let _reserved = reserved;
    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
    proxy.nodes[idx].tasks.lock().unwrap().remove(&id);
}

/// Dials the chosen node. One dial failure tries a different ready node once.
async fn open_backend(proxy: &Proxy) -> Option<(usize, TcpStream, Reserve)> {
    let idx = proxy.choose(None)?;
    let reserved = Reserve::new(&proxy.nodes[idx].conns);
    match TcpStream::connect(&proxy.nodes[idx].mysql_addr).await {
        Ok(server) => Some((idx, server, reserved)),
        Err(_) => {
            drop(reserved);
            let idx2 = proxy.choose(Some(idx))?;
            let reserved = Reserve::new(&proxy.nodes[idx2].conns);
            match TcpStream::connect(&proxy.nodes[idx2].mysql_addr).await {
                Ok(server) => Some((idx2, server, reserved)),
                Err(_) => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct Peer {
        id: String,
        echo_addr: std::net::SocketAddr,
        health_addr: std::net::SocketAddr,
        hits: Arc<AtomicUsize>,
        ok: Arc<AtomicBool>,
        stop_echo: watch::Sender<bool>,
    }

    impl Peer {
        async fn start(id: &str, ok: bool) -> Self {
            let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let health = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let echo_addr = echo.local_addr().unwrap();
            let health_addr = health.local_addr().unwrap();
            let hits = Arc::new(AtomicUsize::new(0));
            let ok = Arc::new(AtomicBool::new(ok));
            let (stop_echo, stop_rx) = watch::channel(false);
            tokio::spawn(echo_loop(echo, Arc::clone(&hits), stop_rx));
            tokio::spawn(health_server(health, Arc::clone(&ok)));
            Self {
                id: id.to_string(),
                echo_addr,
                health_addr,
                hits,
                ok,
                stop_echo,
            }
        }

        fn kill_echo(&self) {
            let _ = self.stop_echo.send(true);
        }
    }

    async fn echo_loop(
        listener: TcpListener,
        hits: Arc<AtomicUsize>,
        mut stop: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                biased;
                result = stop.changed() => {
                    if result.is_err() || *stop.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let Ok((sock, _)) = accepted else { break };
                    let hits = Arc::clone(&hits);
                    tokio::spawn(async move {
                        hits.fetch_add(1, Ordering::Relaxed);
                        let (mut read, mut write) = sock.into_split();
                        let _ = tokio::io::copy(&mut read, &mut write).await;
                    });
                }
            }
        }
    }

    async fn health_server(listener: TcpListener, ok: Arc<AtomicBool>) {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let ok = Arc::clone(&ok);
            tokio::spawn(async move {
                let mut buf = [0u8; 256];
                let _ = sock.read(&mut buf).await;
                let body = if ok.load(Ordering::Relaxed) {
                    "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n"
                } else {
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                };
                let _ = sock.write_all(body.as_bytes()).await;
            });
        }
    }

    fn config_for(balance: Balance, peers: &[&Peer]) -> Config {
        Config {
            listen: "127.0.0.1:0".to_string(),
            check_interval: Duration::from_millis(50),
            balance,
            backends: peers
                .iter()
                .map(|peer| crate::config::Backend {
                    id: peer.id.clone(),
                    mysql_addr: peer.echo_addr.to_string(),
                    health_addr: peer.health_addr.to_string(),
                })
                .collect(),
        }
    }

    async fn launch(cfg: Config) -> Running {
        start(cfg).await.unwrap()
    }

    async fn wait_echo(addr: std::net::SocketAddr) -> TcpStream {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("proxy did not become ready");
            }
            if let Ok(mut stream) = TcpStream::connect(addr).await {
                if stream.write_all(b"ping").await.is_ok() {
                    let mut buf = [0u8; 4];
                    if tokio::time::timeout(Duration::from_millis(500), stream.read_exact(&mut buf))
                        .await
                        .ok()
                        .and_then(|result| result.ok())
                        .is_some()
                        && &buf == b"ping"
                    {
                        return stream;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until a round trip works, then until every peer has had time to pass two checks.
    async fn settle(addr: std::net::SocketAddr) {
        drop(wait_echo(addr).await);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    async fn hold_until_hit(addr: std::net::SocketAddr, peers: &[&Peer]) -> TcpStream {
        let before: usize = peers
            .iter()
            .map(|peer| peer.hits.load(Ordering::Relaxed))
            .sum();
        let stream = TcpStream::connect(addr).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while peers
            .iter()
            .map(|peer| peer.hits.load(Ordering::Relaxed))
            .sum::<usize>()
            == before
        {
            if tokio::time::Instant::now() > deadline {
                panic!("backend did not accept");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        stream
    }

    #[tokio::test]
    async fn killing_a_peer_moves_the_next_connection() {
        let a = Peer::start("n1", true).await;
        let b = Peer::start("n2", true).await;
        let running = launch(config_for(Balance::First, &[&a, &b])).await;
        settle(running.addr).await;

        let a_before = a.hits.load(Ordering::Relaxed);
        let b_before = b.hits.load(Ordering::Relaxed);
        let mut held = wait_echo(running.addr).await;
        assert_eq!(a.hits.load(Ordering::Relaxed), a_before + 1);
        assert_eq!(b.hits.load(Ordering::Relaxed), b_before);

        a.ok.store(false, Ordering::Relaxed);
        a.kill_echo();
        let mut buf = [0u8; 8];
        let closed = tokio::time::timeout(Duration::from_secs(2), held.read(&mut buf)).await;
        assert!(closed.is_ok(), "splice stayed open after the peer died");

        let next = wait_echo(running.addr).await;
        drop(next);
        assert_eq!(a.hits.load(Ordering::Relaxed), a_before + 1);
        assert!(b.hits.load(Ordering::Relaxed) >= b_before + 1);
        running.stop().await;
    }

    #[tokio::test]
    async fn marked_down_closes_the_splice() {
        let a = Peer::start("n1", true).await;
        let running = launch(config_for(Balance::First, &[&a])).await;
        let mut held = wait_echo(running.addr).await;
        a.ok.store(false, Ordering::Relaxed);
        let mut buf = [0u8; 8];
        let closed = tokio::time::timeout(Duration::from_secs(2), held.read(&mut buf)).await;
        assert!(closed.is_ok(), "splice stayed open after /readyz failed");
        running.stop().await;
    }

    #[tokio::test]
    async fn leastconn_spreads_new_connections() {
        let a = Peer::start("n1", true).await;
        let b = Peer::start("n2", true).await;
        let running = launch(config_for(Balance::LeastConn, &[&a, &b])).await;
        settle(running.addr).await;

        let peers = [&a, &b];
        let a0 = a.hits.load(Ordering::Relaxed);
        let b0 = b.hits.load(Ordering::Relaxed);
        let _c1 = hold_until_hit(running.addr, &peers).await;
        let _c2 = hold_until_hit(running.addr, &peers).await;
        let _c3 = hold_until_hit(running.addr, &peers).await;
        assert_eq!(a.hits.load(Ordering::Relaxed), a0 + 2);
        assert_eq!(b.hits.load(Ordering::Relaxed), b0 + 1);
        running.stop().await;
    }

    #[tokio::test]
    async fn no_ready_node_closes_the_client() {
        let a = Peer::start("n1", false).await;
        let b = Peer::start("n2", false).await;
        let running = launch(config_for(Balance::First, &[&a, &b])).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut stream = TcpStream::connect(running.addr).await.unwrap();
        let mut buf = [0u8; 4];
        let n = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(a.hits.load(Ordering::Relaxed), 0);
        assert_eq!(b.hits.load(Ordering::Relaxed), 0);
        running.stop().await;
    }
}
