# hardhatdb-proxy

A TCP proxy for one [HardhatDB](https://github.com/bongani-m/hardhatdb) Raft group. Clients that store one `host:port` dial this process. It copies each connection to a node whose `/readyz` returns 200. MySQL and TLS stay between the client and that node.

One process fronts one Raft group. A ranged deployment runs one proxy per group, each with that group's backend list.

## Run

This directory sits next to a HardhatDB checkout. Create that cluster's certificates once, from the HardhatDB repo: `examples/gencerts.sh`. The compose file here is an override. It publishes host port 3390 and sets `HARDHATDB_METRICS_ADDR` on each node. Port 9090 stays on the `vpc` network.

```bash
HARDHATDB_BOOTSTRAP_PASSWORD=secret docker compose \
  -f ../hardhatdb/compose.yaml -f compose.yaml up --build
```

From the host:

```bash
mysql --host=127.0.0.1 --port=3390 --user=root --password=secret \
  --ssl-mode=REQUIRED --ssl-ca=../hardhatdb/examples/certs/ca.crt \
  mydb
```

On the `vpc` network the address is `proxy:3306`.

A connection stays on the node it opened. When that node is marked down, the proxy closes the connection. The client opens a new one to the same proxy address. While every `/readyz` returns 503, new connections are closed. That is the Raft election window.

Writes still commit on the Raft leader. A connection on a follower uses HardhatDB's write-forward path. `leastconn` spreads reads and connection count across ready nodes. It does not add write throughput. With `HARDHATDB_READ_FROM=local`, another connection can still see an older copy.

`--ssl-mode=REQUIRED` checks the CA. The proxy name is not on the certificate. `--ssl-mode=VERIFY_IDENTITY` requires that name on the certificate.

## Balance

`HARDHATDB_PROXY_BALANCE=first` sends every new connection to the first ready node. That node keeps taking connections until it fails two health checks.

`leastconn` sends a new connection to the ready node with the fewest open connections. A tie goes to the earlier node in `HARDHATDB_PROXY_BACKENDS`.

## Environment

| Variable | Role |
|----------|------|
| `HARDHATDB_PROXY_LISTEN` | Address the proxy binds. Default `0.0.0.0:3306`. |
| `HARDHATDB_PROXY_BACKENDS` | `id=host:port` list, comma-separated. The port is the MySQL port. |
| `HARDHATDB_PROXY_HEALTH_PORT` | Port for `GET /readyz` on each backend host. Default `9090`. |
| `HARDHATDB_PROXY_CHECK_INTERVAL` | Time between checks. Default `2s`. A bare number is seconds. `50ms` is milliseconds. |
| `HARDHATDB_PROXY_BALANCE` | `first` (default) or `leastconn`. |

Two successful checks in a row mark a node up. Two failed checks in a row mark it down. A node starts down. Each check waits at most one second.

`SIGTERM` and `SIGINT` stop the listener and close open connections.
