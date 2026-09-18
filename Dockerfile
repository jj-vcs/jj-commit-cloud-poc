# Multi-stage Dockerfile for deploying jj-cc-server on Google Cloud Run
# Build stage compiles the Rust server binary
FROM rust:bookworm AS builder

# Install protoc (required by tonic-build in cc-common) and build dependencies
RUN apt-get update && apt-get install -y protobuf-compiler libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build the release binary for the Commit Cloud server
RUN cargo build --release --bin jj-cc-server

# Runtime stage uses a minimal Debian image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy compiled binary from builder
COPY --from=builder /app/target/release/jj-cc-server /usr/local/bin/jj-cc-server

ENV PORT=8080
ENV SPANNER_DATABASE=projects/your-project-id/instances/your-spanner-instance/databases/commit_cloud
EXPOSE 8080

# Bind to 0.0.0.0 and listen on Cloud Run's injected PORT using the Spanner database backend
CMD ["sh", "-c", "jj-cc-server --host 0.0.0.0 --port ${PORT:-8080} --store-type spanner --spanner-db ${SPANNER_DATABASE}"]
