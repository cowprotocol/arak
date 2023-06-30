FROM docker.io/rust:1-slim-bookworm as cargo-build
WORKDIR /src

RUN apt-get update && apt-get install -y git libsqlite3-dev libssl-dev pkg-config

COPY . .
RUN cargo build --release

FROM docker.io/debian:bookworm-slim
WORKDIR /opt/arak

RUN apt-get update && apt-get install -y ca-certificates libsqlite3-0 tini && apt-get clean
COPY --from=cargo-build /src/target/release/arak arak
COPY --from=cargo-build /src/arak.example.toml arak.toml
COPY --from=cargo-build /src/LICENSE LICENSE

ENV RUST_LOG=info,arak=debug
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/arak/arak"]
