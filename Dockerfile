# Build stage
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /validator-health-collector .

# Runtime stage
FROM alpine:3.20
RUN apk --no-cache add ca-certificates

# Runs unprivileged. The collector only makes outbound HTTPS calls and serves
# /metrics, so it needs no write access to its own filesystem.
RUN adduser -D -u 10001 collector

COPY --from=builder /validator-health-collector /usr/local/bin/

# A default entity map so the image is usable without a mounted config. Deployments
# override it with a ConfigMap.
#
# Two permission details, both of which silently break entity grouping if missed.
# COPY preserves the source file's mode, and a 0640 root-owned file is unreadable
# to the unprivileged user below. COPY --chmod also applies to any parent directory
# it creates, so letting it create this one leaves a 0644 directory with no execute
# bit and nothing can traverse it. Create the directory first, then set only the
# file's mode.
RUN mkdir -p /etc/validator-health
COPY --chmod=0644 entity.yaml /etc/validator-health/entity.yaml

USER 10001
EXPOSE 9090
ENTRYPOINT ["validator-health-collector"]

# No endpoints are baked in: the collector discovers them from the chain registry
# and health checks them at runtime. Pass -rest or -rpc only to pin one.
CMD ["-listen=:9090", "-entity-map=/etc/validator-health/entity.yaml", "-interval=1h", "-chain=cosmoshub"]
