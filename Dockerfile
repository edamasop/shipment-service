# syntax=docker/dockerfile:1

# The build context is the workspace root (the directory containing go.work),
# not this service directory:
#
#   docker build -f shipment-service/Dockerfile .
#
# go.work is what makes the sibling modules (events, messaging) resolve from
# source. A standalone build must drop go.work and pin those modules in go.mod.
FROM golang:1.26-alpine AS build

WORKDIR /src

# A static binary; no cgo runtime needed in the final image.
# GOFLAGS=-mod=mod is deliberately NOT set: workspace mode rejects it.
ENV CGO_ENABLED=0

COPY events/ ./events/
COPY messaging/ ./messaging/
COPY shipment-service/ ./shipment-service/

# Rebuild the workspace with just the modules this image needs: the go.work in
# the repository lists every service, and go refuses to start when a listed
# directory is absent from the build context.
RUN rm -f go.work go.work.sum \
    && go work init ./events ./messaging ./shipment-service \
    && go build -trimpath -ldflags="-s -w" -o /out/shipment-service ./shipment-service

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 -H app

USER app
WORKDIR /app

COPY --from=build /out/shipment-service /app/shipment-service

EXPOSE 8081
ENTRYPOINT ["/app/shipment-service"]
