FROM rust:1-bookworm AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

COPY --from=build /src/target/release/hardhatdb-proxy /usr/local/bin/hardhatdb-proxy
COPY LICENSE /usr/share/doc/hardhatdb-proxy/LICENSE
COPY NOTICE /usr/share/doc/hardhatdb-proxy/NOTICE

EXPOSE 3306
ENTRYPOINT ["hardhatdb-proxy"]
