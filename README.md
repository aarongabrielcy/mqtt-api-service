# mqtt-api-service

Servicio hermano de `saas-system-iot` que integra marcas IoT vía API/MQTT
propietario. **Por ahora integra únicamente LG ThinQ**; Samsung, Daewoo y
Alexa quedan fuera de esta fase. Normaliza los eventos LG a un payload JSON
compatible con Payload Profiles y los reenvía por gRPC a `ingestion-service`.

For permanent repository operating rules for AI-assisted changes, see [CLAUDE.md](CLAUDE.md).
For deep, evidence-based repository context (architecture, LG/vendor integration flow, Kafka
command bridge, confirmation/synthetic ACK, Redis, gRPC boundary, known risks), see
[docs/sdo/PROJECT_CONTEXT.md](docs/sdo/PROJECT_CONTEXT.md).

```
LG ThinQ API / MQTT
        │
        ▼
mqtt-api-service
  ├─ Se autentica y consulta/consume LG ThinQ API + MQTT (AWS IoT)
  ├─ Cachea estado por dispositivo en Redis (merge de reports parciales)
  ├─ Guarda el mensaje crudo LG en MongoDB (forense, no leído por tracking-platform)
  └─ Normaliza a JSON directo y lo envía por gRPC (TrackingService.IngestRaw)
        │
        ▼
ingestion-service (tracking-platform)
        │
        ▼
Payload Profiles → Realtime / Telemetry / Automation / Alerts / Reports
```

## Cómo corre dentro de saas-system-iot

Corre como servicio hermano, usando infraestructura ya existente en la red
externa `saas-network` — no declara su propio Mongo, Redis ni red:

- Mongo existente: `mongo:27017`
- Redis existente: `redis:6379`
- gRPC de ingestion-service: `ingestion-service:50051`

```bash
cd ../config/mqtt-api-service   # fuera de este repo, ver sección siguiente
docker compose -f ../../saas-system-iot/mqtt-api-service/docker-compose.yml up -d --build
docker logs -f mqtt-api-service
```

## Configuración: 100% variables de entorno

**`config/config.yaml` ya no existe ni se usa.** Desde FASE LG-1A la
configuración se lee únicamente de variables de entorno
(`internal/infrastructure/config/config.go`, sin YAML ni Viper). El
contrato completo de variables está en [.env.example](.env.example).

La configuración runtime real vive **fuera de este repo**, en
`../config/mqtt-api-service/` (hermano de `saas-system-iot/mqtt-api-service/`):

```
../config/mqtt-api-service/
├── mqtt-api-service.env   ← variables reales (copiar desde .env.example)
└── certs/
    ├── AmazonRootCA1.pem
    ├── lg-client.crt
    └── lg-client.key
```

`docker-compose.yml` monta ese `.env` vía `env_file` y ese `certs/` vía bind
mount de solo lectura en `/app/certs`. No monta ningún archivo de
configuración — no hay `/app/config` en la imagen.

## Qué NO debe commitearse

- `../config/mqtt-api-service/mqtt-api-service.env` (secretos reales de LG).
- `../config/mqtt-api-service/certs/*` (certificados TLS reales de AWS IoT/LG).
- Cualquier `.env` o `certs/` dentro de este repo — cubiertos por
  `.gitignore` (`*.pem`, `*.crt`, `*.key`, `certs/`, `.env`).
- IDs de dispositivo o client IDs reales hardcodeados en código.

## Estado actual

### Implementado
- Autenticación y llamadas a LG ThinQ API (`internal/adapters/api/lg/`):
  dispositivos, registro, push subscriptions, event subscriptions, control
  de estado (power/temperatura/airflow/modo/oscilación/power save).
- Cliente MQTT hacia el broker LG en AWS IoT (`internal/adapters/mqtt/client.go`),
  con TLS mutuo (CA + cert + key), auto-reconnect y suscripción a
  `app/clients/{clientId}/push` e `.../inbox`.
- Merge de estado por dispositivo en Redis
  (`internal/adapters/cache/device_state_store.go`), usado tanto por eventos
  push como por polling periódico de estado.
- Persistencia cruda en MongoDB del mensaje LG original
  (`internal/adapters/mongo/repository.go`).
- Normalización a un payload JSON directo (sin wrapper) y envío por gRPC a
  `ingestion-service` vía `TrackingService.IngestRaw`
  (`internal/application/normalizers/lg_normalizer.go`,
  `internal/adapters/grpc/client/tracking.go`).
- Configuración 100% por variables de entorno, centralizada en
  `internal/infrastructure/config/config.go` (sin `os.Getenv` sueltos en
  otros archivos).
- Tests unitarios reales para la normalización LG y para los defaults de
  configuración (ver sección Tests).

### No implementado / fuera de esta fase
- Samsung, Daewoo, Alexa.
- Device Model Sensor Codes Catalog.
- Health/readiness HTTP endpoint — no hay servidor HTTP en el proceso.
  `EXPOSE 8080` en el Dockerfile es solo un puerto reservado, comentado como
  no implementado.
- `internal/adapters/parser/lg_message_parser.go` y
  `internal/application/normalizers/message_normalizer.go` son código
  especulativo previo a LG-1, no usado por el flujo activo (que usa
  `lg_push_parser.go` / `lg_state_parser.go` / `lg_normalizer.go`).

## Payload final enviado a tracking-platform

El contrato gRPC (`internal/adapters/grpc/proto/tracking/tracking.proto`,
`TrackingService.IngestRaw`) no cambia: `topic, payload (bytes), qos,
retain, received_at`. El `topic` va en su campo dedicado, nunca dentro del
payload; el payload es JSON directo compatible con Payload Profiles, sin
wrapper y sin el raw completo de LG (eso se conserva aparte en Mongo):

```
Topic = devices/<deviceID>/telemetry
```

```json
{
  "vendor": "lg",
  "integration": "lg-thinq",
  "event": 0,
  "dt": 1739200000,
  "device": { "externalId": "<LG device id>", "type": "DEVICE_AIR_CONDITIONER" },
  "state": { "power": true, "mode": "COOL", "operationMode": "POWER_ON" },
  "climate": {
    "temperature": { "current": 22, "target": 24, "unit": "C" },
    "humidity": null
  }
}
```

`climate.humidity` siempre es `null`: LG no expone humedad en el estado de
aire acondicionado que consume este servicio.

## Operational behavior (FASE LG-1B)

Mejoras de resiliencia de arranque y diagnóstico agregadas tras observar
comportamiento real en pruebas (DNS de Docker no resuelto todavía en el
primer arranque, ingestion-service tardando en estar listo, LG API
reportando dispositivos desconectados).

### Mongo startup retry
`internal/adapters/mongo/repository.go` (`NewMongoClient`) ya no falla
`fatal` en el primer intento. Reintenta connect+ping con backoff 1s→2s→4s→8s
(tope 10s) durante hasta 90s antes de rendirse. Cada intento fallido se
loguea como `warn` ("mongo connection retry"); solo si se agota el tiempo
máximo se loguea `error` ("failed to connect to mongo after retries") y el
proceso termina (igual que antes, pero ahora tolera el DNS/startup timing
típico de arrancar junto con el contenedor de Mongo). Al conectar, loguea
"connected to mongo" con la URI **sin credenciales** (usuario/password se
quitan antes de loguear), db name y collection name.

### Tracking gRPC: timeout + retry corto
`internal/adapters/grpc/client/tracking.go` (`IngestRaw`) ya no usa
`context.Background()` sin deadline: cada intento tiene un timeout explícito
(`TRACKING_GRPC_REQUEST_TIMEOUT_SECONDS`, default 10s). Ante errores
transitorios (`Unavailable`, `DeadlineExceeded` — típicos de DNS Docker o
ingestion-service arrancando) reintenta hasta `TRACKING_GRPC_MAX_ATTEMPTS`
veces (default 3) con backoff `TRACKING_GRPC_RETRY_INITIAL_BACKOFF_MS`→
`TRACKING_GRPC_RETRY_MAX_BACKOFF_MS` (default 1000ms→4000ms). Errores no
transitorios (payload inválido, etc.) no se reintentan. Logs a buscar:
- `tracking gRPC retry` — reintento en curso (incluye attempt/maxAttempts/topic/grpc_state).
- `tracking event ingested` — publicación exitosa (con o sin retries).
- `tracking publish failed after retries` — falló tras agotar los intentos.

### LG "Not connected device" (416 / código 1222)
`internal/adapters/api/lg/client.go` parsea el body de error de la LG API
(`error.code`/`message`, tolerando forma anidada o plana) y
`APIError.IsDeviceNotConnected()` clasifica status=416 + code=1222 como una
condición operativa esperada (dispositivo offline en la app LG), no un
error crítico. Se loguea una sola vez como `warn` ("device disconnected"
con deviceID/lgErrorCode/httpStatus), sin repetir el error completo, y no
interrumpe el polling de los demás dispositivos. Cualquier otro status/código
sigue tratándose como error real (`stateReadFailed`).

### Status online/offline por gRPC — no implementado (TODO)
Se evaluó reenviar el cambio de estado (online/offline) como un
`RawMessage` adicional a `devices/<deviceID>/status` vía el mismo
`TrackingClient`. **No se implementó**: requeriría asumir cómo
ingestion-service/Payload Profiles interpretan un topic `status` para un
dispositivo LG, algo que esta fase no puede verificar sin tocar
tracking-platform (regla explícita de esta fase). Inventar ese flujo sin
validarlo del lado de tracking-platform arriesga un payload que se ingiere
pero no se interpreta. TODO explícito en
`internal/application/use_case/lg/state_polling.go`
(`recordDeviceStatus`). El estado queda disponible localmente vía Redis y
logs mientras tanto (ver siguiente sección).

### Contadores de polling separados
`refreshDeviceStates` (`internal/application/use_case/lg/state_polling.go`)
ya no resume todo bajo un único `failed`. Log final `Device states
refreshed` con:
```json
{
  "devices": 1,
  "telemetryPublished": 1,
  "stateReadFailed": 0,
  "telemetryPublishFailed": 0,
  "disconnected": 0,
  "skipped": 0
}
```
Un fallo real de `publishTracking` incrementa `telemetryPublishFailed`; un
416/1222 incrementa `disconnected` (no `stateReadFailed`); cualquier otro
error de lectura de estado incrementa `stateReadFailed`.

### Estado operativo en Redis (best-effort)
`internal/adapters/cache/device_state_store.go` guarda un resumen mínimo en
`lg:device:<deviceID>:status` (TTL 24h, igual que el resto del estado LG en
Redis): `{status, lastSeenAt, lastErrorCode, updatedAt}`. Se actualiza a
`"online"` tras una publicación exitosa y a `"offline"` ante un 416/1222. Es
puramente diagnóstico: si Redis falla, se loguea `warn` y el flujo de
polling/telemetry continúa sin interrupción.

### Comandos LG — canal único: Kafka (FASE LG-CMD-1/2, endurecido en 2D/2E/2G)
El `DeviceControlService` gRPC dev/test descrito en fases anteriores de este
README fue eliminado (FASE LG-CMD-2G): ya no existe ningún servidor gRPC en
este proceso. Los comandos LG entran únicamente por Kafka
(`device.command.requested`, consumidos por
`internal/adapters/kafka/command_consumer.go` +
`internal/application/commands/dispatcher.go`), que ejecuta el comando
contra la LG API y publica `device.command.sent` /
`device.command.publish_failed`. La confirmación final llega por estado
(polling/push) vía un ACK sintético publicado por gRPC `IngestRaw` hacia
`ingestion-service` — ver `internal/application/commands/confirmation_manager.go`.

gRPC en este servicio es hoy **exclusivamente cliente**: `TrackingService.
IngestRaw` (`internal/adapters/grpc/client/tracking.go`) para telemetría y
ACKs sintéticos. No hay ningún gRPC server expuesto por mqtt-api-service.

## Requisitos

- Go 1.24.x (`go.mod` fija `go 1.24.1`; no usar 1.25/1.26)
- Docker / docker-compose
- Acceso a la red externa `saas-network` de saas-system-iot

## Desarrollo local

```bash
go mod download
cp .env.example .env   # completar con credenciales LG reales, no commitear
go run ./cmd/api
```

## Tests

```bash
go test ./...
```

Cubre: normalización LG (`internal/application/normalizers/lg_normalizer_test.go`
— forma del topic, ausencia de wrapper, todos los campos del payload),
defaults/lectura de configuración incluyendo los env vars de timeout/retry
gRPC (`internal/infrastructure/config/config_test.go`), retry/timeout de
`IngestRaw` con un fake `TrackingServiceClient` (`internal/adapters/grpc/client/tracking_test.go`
— reintenta ante `Unavailable`, no reintenta errores no transitorios, falla
tras agotar los intentos), clasificación de errores LG 416/1222
(`internal/adapters/api/lg/client_test.go`), redacción de credenciales en
`MONGO_URI` (`internal/adapters/mongo/repository_test.go`) y forma/naming
del estado en Redis (`internal/adapters/cache/device_state_store_test.go`).

No se agregó un test de integración para `refreshDeviceStates` en sí: está
fuertemente acoplado a tipos concretos (`*lg.DeviceService`,
`*parser.LGStateParser`, etc., no interfaces), así que mockearlo
requeriría un refactor más amplio fuera del alcance de esta fase. En su
lugar se testean las piezas puras que sí puede aislar
(`APIError.IsDeviceNotConnected`, `isTransientGRPCError`), que son
justamente lo que decide el conteo.

## Validación

```bash
go vet ./...
go test ./...
go build ./cmd/api
docker build .
```
