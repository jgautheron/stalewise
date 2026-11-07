# syntax=docker/dockerfile:1
FROM rust:1.99.1-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS builder
ARG TARGETPLATFORM
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-$TARGETPLATFORM,sharing=locked \
    --mount=type=cache,target=/src/target,id=cargo-target-$TARGETPLATFORM,sharing=locked \
    cargo build --release --locked && cp target/release/stalewise /stalewise

FROM varnish:9.1.0@sha256:f97e894d90bccc585e5847e766696b7785c98046c29f76007f06d2e2c3228a4e
USER root
RUN test "$(id -u varnish)" = 1000 && test "$(id -g varnish)" = 1000 \
    && install -d -o varnish -g varnish -m 0750 /var/lib/stalewise
COPY --from=builder /stalewise /usr/local/bin/stalewise
USER 1000:1000
EXPOSE 8080 8081 9090
ENTRYPOINT ["/usr/local/bin/stalewise"]
