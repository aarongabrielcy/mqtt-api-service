# mqtt-api-service

Servicio hermano de `saas-system-iot` que integra marcas IoT vía API/MQTT
propietario. **Por ahora integra únicamente LG ThinQ**; Samsung, Daewoo y
Alexa quedan fuera de esta fase. Normaliza los eventos LG a un payload JSON
compatible con Payload Profiles y los reenvía por gRPC a `ingestion-service`.

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
- Flujo final de comandos LG vía Kafka (`device.command.requested` /
  `device.command.sent` / `device.command.publish_failed`) — el
  `DeviceControlService` gRPC actual es solo un mecanismo dev/test, ver TODO
  en `internal/adapters/grpc/server/device_control_server.go`. No conectar
  ese servidor a producción.
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

## Comandos LG — pendiente

`DeviceControlService` (gRPC local) sigue siendo solo un mecanismo de
desarrollo/pruebas. La integración final de comandos LG debe consumir Kafka
`device.command.requested` y publicar `device.command.sent` /
`device.command.publish_failed`, igual que `mqtt-adapter-service`. No
implementado en esta fase.

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
— forma del topic, ausencia de wrapper, todos los campos del payload) y
defaults/lectura de configuración (`internal/infrastructure/config/config_test.go`).

## Validación

```bash
go vet ./...
go test ./...
go build ./cmd/api
docker build .
```
