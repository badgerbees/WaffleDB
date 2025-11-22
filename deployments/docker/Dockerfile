FROM rust:latest AS builder

WORKDIR /app

# Copy the entire project
COPY . .

# Build the project in release mode
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/waffledb-server /usr/local/bin/waffledb-server

# Create data directory
RUN mkdir -p /data

# Expose ports
EXPOSE 8080 9090

# Set environment variables
ENV WAFFLEDB_HOST=0.0.0.0
ENV WAFFLEDB_HTTP_PORT=8080
ENV WAFFLEDB_GRPC_PORT=9090
ENV WAFFLEDB_STORAGE_PATH=/data
ENV RUST_LOG=info

# Run the server
CMD ["waffledb-server"]

