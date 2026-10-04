use std::env;
use std::time::Duration;

use crate::pick::Balance;

/// One HardhatDB process. `mysql_addr` receives client bytes. `health_addr`
/// is `GET /readyz` on the same host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backend {
    pub id: String,
    pub mysql_addr: String,
    pub health_addr: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub listen: String,
    pub backends: Vec<Backend>,
    pub check_interval: Duration,
    pub balance: Balance,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let listen = env_or("HARDHATDB_PROXY_LISTEN", "0.0.0.0:3306");
        let raw = env::var("HARDHATDB_PROXY_BACKENDS")
            .map_err(|_| "hardhatdb-proxy: HARDHATDB_PROXY_BACKENDS is unset".to_string())?;
        let health_port = parse_port(&env_or("HARDHATDB_PROXY_HEALTH_PORT", "9090"))?;
        let check_interval = parse_duration(&env_or("HARDHATDB_PROXY_CHECK_INTERVAL", "2s"))?;
        let balance = parse_balance(&env_or("HARDHATDB_PROXY_BALANCE", "first"))?;
        let backends = parse_backends(&raw, health_port)?;
        Ok(Config {
            listen,
            backends,
            check_interval,
            balance,
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => default.to_string(),
    }
}

pub fn parse_port(raw: &str) -> Result<u16, String> {
    let port: u16 = raw
        .trim()
        .parse()
        .map_err(|_| format!("hardhatdb-proxy: bad port {raw}"))?;
    if port == 0 {
        return Err("hardhatdb-proxy: port is 0".to_string());
    }
    Ok(port)
}

/// `2s`, `50ms`, or a bare number of seconds. Zero is refused.
pub fn parse_duration(raw: &str) -> Result<Duration, String> {
    let raw = raw.trim();
    let (num, unit) = if let Some(num) = raw.strip_suffix("ms") {
        (num, "ms")
    } else if let Some(num) = raw.strip_suffix('s') {
        (num, "s")
    } else {
        (raw, "s")
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("hardhatdb-proxy: bad duration {raw}"))?;
    if n == 0 {
        return Err("hardhatdb-proxy: duration is 0".to_string());
    }
    Ok(match unit {
        "ms" => Duration::from_millis(n),
        _ => Duration::from_secs(n),
    })
}

pub fn parse_balance(raw: &str) -> Result<Balance, String> {
    match raw.trim() {
        "first" => Ok(Balance::First),
        "leastconn" => Ok(Balance::LeastConn),
        other => Err(format!(
            "hardhatdb-proxy: HARDHATDB_PROXY_BALANCE {other} is unknown"
        )),
    }
}

/// `id=host:port` entries, comma-separated. The port is MySQL. Health checks
/// use `health_port` on the same host.
pub fn parse_backends(raw: &str, health_port: u16) -> Result<Vec<Backend>, String> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (id, addr) = part
            .split_once('=')
            .ok_or_else(|| format!("hardhatdb-proxy: backend {part} needs id=host:port"))?;
        let id = id.trim();
        let addr = addr.trim();
        if id.is_empty() || addr.is_empty() {
            return Err(format!(
                "hardhatdb-proxy: backend {part} needs id=host:port"
            ));
        }
        if out.iter().any(|b: &Backend| b.id == id) {
            return Err(format!("hardhatdb-proxy: duplicate backend {id}"));
        }
        let (host, port) = split_host_port(addr)?;
        out.push(Backend {
            id: id.to_string(),
            mysql_addr: format!("{host}:{port}"),
            health_addr: format!("{host}:{health_port}"),
        });
    }
    if out.is_empty() {
        return Err("hardhatdb-proxy: HARDHATDB_PROXY_BACKENDS is empty".to_string());
    }
    Ok(out)
}

fn split_host_port(addr: &str) -> Result<(&str, u16), String> {
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| format!("hardhatdb-proxy: {addr} needs host:port"))?;
    if host.is_empty() {
        return Err(format!("hardhatdb-proxy: {addr} needs host:port"));
    }
    let port = parse_port(port)?;
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn parses_a_peer_list() {
        let backends = parse_backends(
            "n1=10.116.0.2:3306, n2=10.116.0.3:3306,n3=10.116.0.4:3306",
            9090,
        )
        .unwrap();
        assert_eq!(backends.len(), 3);
        assert_eq!(backends[0].id, "n1");
        assert_eq!(backends[0].mysql_addr, "10.116.0.2:3306");
        assert_eq!(backends[0].health_addr, "10.116.0.2:9090");
        assert_eq!(backends[2].health_addr, "10.116.0.4:9090");
    }

    #[test]
    fn rejects_bad_backends() {
        assert!(parse_backends("", 9090).is_err());
        assert!(parse_backends("n1", 9090).is_err());
        assert!(parse_backends("n1=10.116.0.2", 9090).is_err());
        assert!(parse_backends("n1=10.116.0.2:3306,n1=10.116.0.3:3306", 9090).is_err());
        assert!(parse_port("0").is_err());
    }

    #[test]
    fn parses_balance_and_duration() {
        assert_eq!(parse_balance("first").unwrap(), Balance::First);
        assert_eq!(parse_balance("leastconn").unwrap(), Balance::LeastConn);
        assert!(parse_balance("roundrobin").is_err());
        assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
        assert_eq!(parse_duration("50ms").unwrap(), Duration::from_millis(50));
        assert_eq!(parse_duration("2").unwrap(), Duration::from_secs(2));
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("50m").is_err());
    }

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct ClearEnv;
    impl Drop for ClearEnv {
        fn drop(&mut self) {
            for key in [
                "HARDHATDB_PROXY_LISTEN",
                "HARDHATDB_PROXY_BACKENDS",
                "HARDHATDB_PROXY_HEALTH_PORT",
                "HARDHATDB_PROXY_CHECK_INTERVAL",
                "HARDHATDB_PROXY_BALANCE",
            ] {
                env::remove_var(key);
            }
        }
    }

    #[test]
    fn from_env_applies_defaults() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _clear = ClearEnv;
        env::remove_var("HARDHATDB_PROXY_LISTEN");
        env::remove_var("HARDHATDB_PROXY_HEALTH_PORT");
        env::remove_var("HARDHATDB_PROXY_CHECK_INTERVAL");
        env::remove_var("HARDHATDB_PROXY_BALANCE");
        env::set_var(
            "HARDHATDB_PROXY_BACKENDS",
            "n1=10.116.0.2:3306,n2=10.116.0.3:3306",
        );
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:3306");
        assert_eq!(cfg.balance, Balance::First);
        assert_eq!(cfg.check_interval, Duration::from_secs(2));
        assert_eq!(cfg.backends[1].health_addr, "10.116.0.3:9090");

        env::set_var("HARDHATDB_PROXY_BALANCE", "leastconn");
        env::set_var("HARDHATDB_PROXY_CHECK_INTERVAL", "50ms");
        env::set_var("HARDHATDB_PROXY_LISTEN", "127.0.0.1:3390");
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.balance, Balance::LeastConn);
        assert_eq!(cfg.check_interval, Duration::from_millis(50));
        assert_eq!(cfg.listen, "127.0.0.1:3390");
    }
}
