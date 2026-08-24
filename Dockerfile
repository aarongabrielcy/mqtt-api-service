
# ---- Build stage ----
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Cachear dependencias antes de copiar el resto del código
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

# Directorio donde docker-compose monta certs/ en tiempo de ejecución (ver
# docker-compose.yml). Debe preexistir en la imagen para que el bind mount
# de un directorio funcione bajo el USER nonroot. La configuración ya no usa
# archivos (100% variables de entorno vía env_file), por lo que no se crea
# /app/config.
RUN mkdir -p /out/certs

# ---- Final stage ----
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/api /app/api
COPY --from=builder /out/certs /app/certs

# Puerto reservado para un futuro health/readiness HTTP endpoint.
# TODO: no implementado todavía — no existe servidor HTTP en el proceso.
EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/app/api"]