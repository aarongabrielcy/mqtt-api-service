
# ---- Build stage ----
FROM golang:1.24-alpine AS builder
 
WORKDIR /src
 
# Cachear dependencias antes de copiar el resto del código
COPY go.mod go.sum ./
RUN go mod download
 
COPY . .
 
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api
 
# ---- Final stage ----
FROM gcr.io/distroless/static-debian12:nonroot
 
WORKDIR /app
 
COPY --from=builder /out/api /app/api
 
EXPOSE 8080
 
USER nonroot:nonroot
 
ENTRYPOINT ["/app/api"]