# -------------------------------------------------------------
# Stage 1: Build stage
# -------------------------------------------------------------
FROM rust:latest as builder

# Install protoc (required by tonic-build in cc-proto)
RUN apt-get update && apt-get install -y protobuf-compiler libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build the release binary for the server
RUN cargo build --release --bin jj-cc-server

# -------------------------------------------------------------
# Stage 2: Runtime stage
# -------------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy compiled binary from builder
COPY --from=builder /app/target/release/jj-cc-server /usr/local/bin/jj-cc-server

ENV PORT=8080
EXPOSE 8080

# Bind to 0.0.0.0 and listen on port 8080
CMD ["sh", "-c", "jj-cc-server --host 0.0.0.0 --port ${PORT:-8080}"]
