<<<<<<< HEAD
# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

ARG RUST_VERSION=1.76.0
ARG APP_NAME=datastore-server

################################################################################
# xx is a helper for cross-compilation.
# See https://github.com/tonistiigi/xx/ for more information.
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.3.0 AS xx

################################################################################
# Create a stage for building the application.
FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-alpine AS build
ARG APP_NAME
WORKDIR /app

# Copy /.cargo/config.toml to the container
COPY .cargo /root/.cargo

# Copy SSH key to the container
COPY .ssh /root/.ssh

# Set permissions for the SSH key
RUN chmod 600 /root/.ssh/id_rsa && \
    echo "[git.anvere.net]:2222 ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBBmggbX7wR8OMqmRGQLvtVR57Me87silLwyK2QYnScq6RDn1V1RxIXtEgdfbDPm+f0+myVs6k9ORRU58XriPpPc=" >> /root/.ssh/known_hosts


# Copy cross compilation utilities from the xx stage.
COPY --from=xx / /
COPY . .

# Install cross-compilation build dependencies
RUN apk add --no-cache clang lld musl-dev git file openssh

# Build the application
RUN set -e && \
    xx-cargo build --release --target-dir ./target && \
    cp ./target/$(xx-cargo --print-target-triple)/release/$APP_NAME /bin/server && \
    xx-verify /bin/server
=======
FROM docker.io/rust:1.76-bullseye AS builder


WORKDIR /app

COPY ./ ./

# RUN     RUSTFLAGS="-C target-cpu=native" cargo build --release
RUN cargo build --release --bin datastore-server
>>>>>>> named_tables

################################################################################
# Stage 2: Create the final image for running the application
################################################################################
<<<<<<< HEAD
FROM alpine:3.18 AS final

# Create a non-privileged user that the app will run under
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser
=======
FROM docker.io/rust:1.76-bullseye AS final
>>>>>>> named_tables

# Set working directory
WORKDIR /app

# Copy the executable from the build stage
<<<<<<< HEAD
COPY --from=build /bin/server /bin/

# Set ownership and permissions for the executable
RUN chown appuser:appuser /bin/server && \
    chmod +x /bin/server

# Expose the port that the application listens on
EXPOSE 5555

# Specify the command to run the application
CMD ["/bin/server", "--listen-addr", "0.0.0.0:5555", "Belochat123"]
=======
COPY --from=builder /app/target/release/datastore-server /app/
COPY --from=builder /app/service.yaml /app/

# # Set ownership and permissions for the executable
# RUN chown appuser:appuser /bin/server && \
#     chmod +x /bin/server
RUN chmod +x /app/datastore-server

# Expose the port that the application listens on
EXPOSE 7777

# Specify the command to run the application
CMD ["/app/datastore-server"]
>>>>>>> named_tables
