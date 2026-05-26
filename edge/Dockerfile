# Multi-stage build for lark-edge.
#
# Stage 1 builds the dashboard SPA with Vite. Stage 2 builds the Go
# binary (which embeds the SPA from dashboard/dist via go:embed). Final
# stage is a tiny alpine image with just the static binary.
#
# Build context is the repo root so the Dockerfile can reach into
# `edge/` for source and `edge/dashboard/` for the SPA.

# --- Stage 1: dashboard SPA --------------------------------------------------
FROM node:22-alpine AS spa
WORKDIR /dashboard

COPY edge/dashboard/package.json edge/dashboard/package-lock.json* ./
RUN npm ci --no-audit --no-fund

COPY edge/dashboard/ ./
RUN npm run build

# --- Stage 2: Go binary ------------------------------------------------------
FROM golang:1.25-alpine AS gobuild
RUN apk add --no-cache git
WORKDIR /src/edge

COPY edge/ ./
COPY --from=spa /dashboard/dist ./dashboard/dist

RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/lark-edge .

# --- Stage 3: runtime --------------------------------------------------------
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app

COPY --from=gobuild /out/lark-edge /app/lark-edge

# Main HTTP listener (admin SPA + admin API + wire-protocol REST/WS).
EXPOSE 8080
# Internal HTTP listener (server-to-server registration, metrics ingest).
EXPOSE 8081

# Persistent SQLite database and any future on-disk state lives here.
VOLUME ["/data"]

ENTRYPOINT ["/app/lark-edge"]
