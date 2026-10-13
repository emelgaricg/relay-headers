# syntax=docker/dockerfile:1
#
# Build multi-stage:
#   Etapa 1 "build": imagen con el compilador de Go. Compila el binario.
#   Etapa 2 final  : FROM scratch (imagen vacía). Solo lleva el binario y los
#                    certificados para TLS. Pesa ~7 MB y no tiene shell.
#
# No necesita Go instalado en su máquina: el compilador vive dentro de la etapa 1.
#   docker build -t relay-headers:dev .
#   docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/relay-headers:v1.0.0 --push .

# ---------- Etapa 1: compilar ----------
# --platform=$BUILDPLATFORM compila en la arquitectura nativa del runner y hace
# cross-compile hacia la arquitectura destino (mucho más rápido que emular con QEMU).
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

RUN apk add --no-cache ca-certificates
WORKDIR /src

# Primero solo las dependencias: esta capa queda en caché mientras go.mod no cambie.
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.Version=${VERSION}" -o /out/relay-headers .

# ---------- Etapa 2: imagen final ----------
FROM scratch

LABEL org.opencontainers.image.title="relay-headers" \
      org.opencontainers.image.description="Servicio didáctico: cola RabbitMQ -> log -> exchange headers (+estado=procesado)"

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/relay-headers /relay-headers

# Usuario sin privilegios (no root). En scratch no existe /etc/passwd, se usa el UID.
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/relay-headers"]
