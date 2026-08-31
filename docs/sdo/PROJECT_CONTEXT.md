# Project Context — mqtt-api-service

Deep, repository-specific current context for AI/SDO/onboarding use. This document distinguishes
**CONFIRMED** (directly verified against current repository files at the time of writing) from
**UNKNOWN** (unverifiable, stale, or deferred to a future decision). Repository implementation
evidence outranks historical audit/chat/task assumptions for repository behavior; the authoritative
Notion `Project Context — saas-system-iot` governs project-level/SDO policy facts (cited explicitly
in §12 below).

## 1. Purpose

CONFIRMED: `mqtt-api-service` is the Go LG ThinQ integration bridge for the `saas-system-iot`
platform. It is one independent repository in a polyrepo (see the platform-level Project Context);
it does not duplicate platform-wide documentation. It currently integrates only LG ThinQ air
conditioners — Samsung, Daewoo, and Alexa are explicitly out of scope for this phase.

CONFIRMED — responsibilities, from `cmd/api/main.go` and the wiring it performs:
1. Authenticate against and poll the LG ThinQ HTTP API for device state, and connect over MQTT
   (mutual TLS) to LG's AWS IoT broker to receive push notifications for the same devices.
2. Cache per-device state in Redis, merging partial push reports onto the last known snapshot.
3. Persist every raw LG message (push and polled state) to MongoDB, store-first and independent of
   the gRPC publish outcome.
4. Normalize LG state into a direct JSON payload (no wrapper) and forward it to
   `tracking-platform`'s `ingestion-service` over gRPC (`TrackingService.IngestRaw`).
5. Consume `device.command.requested` from Kafka for LG-addressed commands, execute them against
   the LG ThinQ HTTP API, and report delivery outcome back to Kafka
   (`device.command.sent` / `device.command.publish_failed`).
6. Confirm command execution by comparing subsequent LG state (push or poll) against the expected
   post-command state, and publish a synthetic command ACK over the same gRPC `IngestRaw` channel
   used for telemetry — LG does not expose a native command ACK.

## 2. Go structure (layered: adapters / application / domain / infrastructure)

CONFIRMED, from the current tree:

```
cmd/api/main.go                          — composition root: wires config, Mongo, Redis, MQTT,
                                            gRPC, LGService, Kafka command bridge, graceful shutdown
internal/domain/
  models.go                                domain types shared across the service
  commands/events.go                       Kafka wire shapes: DeviceCommandEvent,
                                            CommandSentEvent, CommandPublishFailedEvent
  interfaces/tracking_client.go            TrackingClient port (gRPC IngestRaw abstraction)
internal/application/
  use_case/lg/                             LGService: device sync, subscriptions, push handling,
                                            state polling, command execution entry points
  commands/                                CommandDispatcher, ConfirmationManager, AckPublisher,
                                            SyntheticAck, ExpectedState, Payload parsing/validation,
                                            ResolveCommandKey/LooksLikeLGCommand resolver
  normalizers/lg_normalizer.go              LGStateNormalizer: LG state -> LGTelemetryEnvelope JSON
internal/adapters/
  api/lg/                                   LGAPIClient (HTTP) + DeviceService, DeviceRegistryService,
                                            PushService, EventService, error classification
                                            (APIError.IsDeviceNotConnected / IsDeviceTimeout)
  mqtt/client.go                            paho-mqtt client, mutual TLS to LG's AWS IoT broker
  parser/                                   LGStateParser, LGPushParser (active); lg_message_parser.go
                                            (unused, see §13)
  cache/device_state_store.go               Redis-backed per-device state snapshot/merge + status
  mongo/repository.go                       raw LG message persistence + Mongo connect-with-retry
  grpc/client/                              gRPC client to tracking-platform's TrackingService
  kafka/                                    CommandConsumer (device.command.requested) +
                                            CommandStatusPublisher (sent/publish_failed)
internal/infrastructure/
  config/config.go                          100% env-var-driven Config (no YAML/Viper)
  cache/redis_client.go                     Redis client construction (ping-on-connect)
  logger/, debuglog/                        zap logger setup; truncation helper for verbose logs
```

UNKNOWN / not part of the active flow: `internal/adapters/parser/lg_message_parser.go`
(`LGMessageParser`) and `internal/application/normalizers/message_normalizer.go` exist in the tree
but are not constructed or called from any current code path (confirmed by repository-wide
reference search — only `lg_push_parser.go`, `lg_state_parser.go`, and `lg_normalizer.go` are wired
from `LGService`/`main.go`). Treat them as pre-existing speculative code, not current behavior.
`structure.txt` is explicitly marked obsolete in its own first line and does not reflect the current
tree.

## 3. LG ThinQ / vendor control integration

CONFIRMED. `internal/adapters/api/lg.LGAPIClient` (`client.go`) is a thin HTTP client against
`LGApi.BaseURL` (`LG_API_BASE_URL`), adding `Authorization: Bearer <LG_ACCESS_TOKEN>`,
`x-message-id` (generated per request), `x-country`, `x-client-id`, `x-api-key` headers. On top of
it:
- `DeviceService` — device list/state (`devices_service.go`, `device_registry_service.go`).
- `PushService` / `EventService` — LG push and event subscription management.
- `DeviceRegistryService` — `GET/POST/DELETE /push/devices` registry subscribe/unsubscribe.

`LGService.Initialize` (`internal/application/use_case/lg/lg_service.go`) runs, at startup:
`syncDevices` → `ensureRegistrySubscription` → `ensureDeviceSubscriptions`. Two independent state
paths feed the same Redis snapshot and the same telemetry/confirmation pipeline:
- **Push (event-driven):** the MQTT client (see §4) delivers LG push messages to
  `LGService.HandlePushMessage` (`push_handler.go`), which merges the partial report into Redis,
  classifies the change into an `EventCode` (`classifyPushEvent`), and — only for changes judged
  trackable — emits telemetry.
- **Poll (interval-driven):** `LGService.StartDeviceStateMonitor` (`state_polling.go`) runs
  `refreshDeviceStates` on `LG_STATE_POLL_INTERVAL_SECONDS` (default 30s), calling
  `GET /devices/:id/state` for every managed `DEVICE_AIR_CONDITIONER`, parsing, snapshotting to
  Redis, and always emitting telemetry (`EventCodeTracking`).

Both paths call `ConfirmationManager.TryConfirm` (if the Kafka command bridge is enabled, see §5)
before publishing telemetry, and both persist the raw LG payload to MongoDB (§7) before or alongside
the gRPC publish.

LG API error classification (`APIError`, `client.go`): status 416 / code `1222` ("Not connected
device") is treated as a normal offline condition, not a service error; status 400 / code `2211`
("Device Timeout") is treated as ambiguous — the dispatcher (§5) waits for state confirmation
instead of failing immediately.

## 4. AWS IoT MQTT push path

CONFIRMED, `internal/adapters/mqtt/client.go`. The MQTT client connects to `MQTT.Endpoint`
(`LG_MQTT_ENDPOINT`, an AWS IoT broker) using mutual TLS built from `LG_MQTT_CA_CERT_PATH` /
`LG_MQTT_CLIENT_CERT_PATH` / `LG_MQTT_CLIENT_KEY_PATH` (defaults `/app/certs/AmazonRootCA1.pem`,
`/app/certs/lg-client.crt`, `/app/certs/lg-client.key`), `TLS.MinVersion = TLS 1.2`,
auto-reconnect enabled (max reconnect interval 3s). `cmd/api/main.go` subscribes to
`app/clients/<LG_CLIENT_ID>/push` (routed to `LGService.HandlePushMessage`) and
`app/clients/<LG_CLIENT_ID>/inbox` (routed to a no-op logging handler — inbox messages are received
and logged only, not otherwise processed). `LG_CLIENT_ID` here is the LG app/tenant identifier used
to build these topics, distinct from `MQTT.ClientID` (`LG_MQTT_CLIENT_ID`, the MQTT/AWS IoT session
identity) and from `LGApi.ClientID` (`LG_API_CLIENT_ID`, used only in LG HTTP API headers) — three
different identifiers that must not be confused.

## 5. Kafka `device.command.requested` consumption and the sent/publish-failed boundary

CONFIRMED. `internal/adapters/kafka.CommandConsumer` (`command_consumer.go`) reads
`KAFKA_COMMAND_TOPIC` (default `device.command.requested`) with consumer group
`KAFKA_COMMAND_CONSUMER_GROUP` (default `mqtt-api-service-lg-commands`) — a **separate** consumer
group from `mqtt-adapter-service`'s own consumer on the same topic (see §11 for the shared-topic
risk this implies). Unlike `mqtt-adapter-service`'s auto-commit pattern, this consumer uses
`FetchMessage` + explicit `CommitMessages` only after `CommandDispatcher.Dispatch` returns.

`CommandDispatcher.Dispatch` (`internal/application/commands/dispatcher.go`):
1. Drops (logs, no publish) events missing `commandId`/`imei`.
2. Resolves a `commandKey` via `ResolveCommandKey` (`resolver.go`): metadata.commandKey → root
   `commandKey` → `commandType` if it already matches a valid key → `commandCode` 201–206 fallback.
   If unresolved, `LooksLikeLGCommand` distinguishes "not addressed to LG at all" (silently ignored
   — e.g. legacy ESP32 codes 101–106 that belong to `mqtt-adapter-service`'s own consumer group on
   the same topic) from "looks LG-addressed but unresolvable" (published as
   `device.command.publish_failed` + a failure ACK, detail `unsupported_command`).
3. Enforces idempotency by `commandId` (`ConfirmationManager.MarkSeenIfNew`, Redis `SETNX` under
   `lg:command:seen:<commandId>`, TTL `LG_COMMAND_SEEN_TTL_SECONDS`, default 600s) — a duplicate
   (e.g. Kafka redelivery) is skipped without republishing.
4. Parses/validates the command payload (`ParseCommandPayload`); an invalid payload publishes
   `publish_failed` + a failure ACK (detail `invalid_payload`).
5. For `lg.oscillation` with `enabled=true` only, checks last-known power state
   (`GetLastKnownPower`) and rejects with `precondition_failed_power_off` if the device is known to
   be powered off (confirmed LG behavior: oscillation changes are not applied/reported while
   `airConOperationMode=POWER_OFF`). No other command has this precondition.
6. Registers a `PendingConfirmation` in Redis (`ConfirmationManager.SavePending`,
   `lg:command:pending:<imei>`) **before** calling the LG API — this ordering exists specifically so
   a push notification arriving immediately after the HTTP response is not missed by
   `TryConfirm` (§6).
7. Executes the command against the LG API (`SetDevicePower`/`SetDeviceTemperature`/
   `SetOperationMode`/`SetAirFlow`/`SetOscillation`/`SetPowerSave`). LG's ambiguous
   "Device Timeout" (400/2211) is treated the same as success (SENT + pending confirmation kept,
   with `Reason=device_timeout`) rather than as an immediate failure. Any other execution error
   deletes the pending confirmation and publishes `publish_failed` + a failure ACK (detail
   `device_disconnected` for 416/1222, `lg_api_error` otherwise).
8. On success or ambiguous timeout, publishes `device.command.sent`
   (`CommandStatusPublisher.PublishSent` → Kafka `KAFKA_COMMAND_SENT_TOPIC`, default
   `device.command.sent`) and schedules a delayed, best-effort state refresh
   (`triggerPostCommandRefresh`, delay `LG_COMMAND_POST_REFRESH_DELAY_MS`, default 1000ms) that
   reuses the same `refreshDeviceState` pipeline as periodic polling.

`device.command.publish_failed` is published via the same `CommandStatusPublisher`
(`KAFKA_COMMAND_PUBLISH_FAILED_TOPIC`, default `device.command.publish_failed`). This bridge only
starts if `LG_COMMANDS_ENABLED=true` (default true; an unparseable value falls back to `false`, not
to the unset-default — see `config.go`'s `getEnvBoolStrict`); when disabled, the service behaves as
telemetry-only, unchanged from before this bridge existed.

## 6. Vendor-state confirmation and synthetic ACK

CONFIRMED. LG exposes no native command ACK, so `mqtt-api-service` synthesizes one over the same
gRPC `IngestRaw` channel already used for telemetry (`AckPublisher.PublishSyntheticAck`,
`internal/application/commands/synthetic_ack.go`), publishing to pseudo-topic
`devices/<imei>/ack` — the same topic shape `mqtt-adapter-service`/ESP32 use for real device ACKs,
so `ingestion-service` requires no change to classify it.

`ConfirmationManager` (`confirmation_manager.go`) owns two Redis-backed states:
- `lg:command:seen:<commandId>` — idempotency marker (§5).
- `lg:command:pending:<imei>` — the single in-flight `PendingConfirmation` per device (TTL =
  `LG_COMMAND_ACK_TIMEOUT_SECONDS` + 60s margin; the sweep below, not Redis TTL expiry, is the
  actual timeout mechanism). Saving a new pending confirmation for a device that already has a
  different `commandId` pending first publishes a `superseded_by_new_command` ACK for the older one
  — LG does not support concurrent commands per device, so the newest one always wins.

`TryConfirm` (called from both the push path §4 and the poll path §3 after every state read) compares
the pending confirmation's `ExpectedState` against the freshly observed LG state
(`matchesExpected`); on match it publishes an `ok:true` ACK (`confirmed_by_state`, or
`confirmed_after_device_timeout` if the pending originated from a 2211) and deletes the pending
entry. `StartSweep` runs `SweepTimeouts` on `LG_COMMAND_ACK_SWEEP_SECONDS` (default 5s), publishing
an `ok:false` ACK for any pending confirmation past its `ExpiresAt` (`ack_timeout`, or
`device_timeout_unconfirmed` plus an additional `device.command.publish_failed` if it originated
from a 2211).

## 7. Redis use

CONFIRMED, two independent key families under `internal/adapters/cache` (device state) and
`internal/application/commands` (confirmation, §6):
- `device:state:<deviceID>` — full merged LG state snapshot (`SetSnapshot`/`MergePartial`/`GetState`
  in `device_state_store.go`), TTL 24h, updated by both push (partial merge) and poll (full
  overwrite).
- `lg:device:<deviceID>:status` — minimal best-effort operational summary
  (`{status, lastSeenAt, lastErrorCode, updatedAt}`), TTL 24h; a Redis failure here is logged as a
  warning and never interrupts the polling/telemetry flow.
- `lg:command:pending:<imei>` / `lg:command:seen:<commandId>` — command bridge state, §5–6.

Connection: `REDIS_ADDR` (default `redis:6379`), a plain ping-on-connect client
(`internal/infrastructure/cache/redis_client.go`), no auth/TLS options currently wired.

## 8. gRPC / ingestion boundary

CONFIRMED. `internal/adapters/grpc/client` wraps `tracking.pb.go`'s generated
`TrackingServiceClient`. This service is gRPC **client-only** — the prior in-process gRPC
`DeviceControlService` (a dev/test control surface) has been removed; there is no gRPC server in
this process. `TRACKING_PLATFORM_GRPC_ADDRESS` (default `ingestion-service:50051`) is the sole gRPC
target, used for exactly one RPC, `TrackingService.IngestRaw`, called for two distinct purposes:
telemetry (`internal/application/use_case/lg`, via `publishTracking`) and synthetic command ACKs
(§6). The contract itself (`topic`, `payload` bytes, `qos`, `retain`, `received_at`) is not modified
by this repository. Calls have a per-attempt timeout (`TRACKING_GRPC_REQUEST_TIMEOUT_SECONDS`,
default 10s) and retry transient errors (`Unavailable`, `DeadlineExceeded`) up to
`TRACKING_GRPC_MAX_ATTEMPTS` (default 3) with backoff between
`TRACKING_GRPC_RETRY_INITIAL_BACKOFF_MS` and `TRACKING_GRPC_RETRY_MAX_BACKOFF_MS` (default
1000ms–4000ms).

## 9. Runtime / environment configuration conventions

CONFIRMED, `internal/infrastructure/config/config.go`. Configuration is 100% environment-variable
driven — there is no YAML/Viper config file read at runtime (a previously planned
`config/config.yaml` does not exist; `structure.txt`, which still shows it, is explicitly marked
obsolete). `LoadConfig` optionally loads a local `.env` via `godotenv.Load()` (a no-op if absent) —
this is a local-dev convenience only.

The real runtime configuration lives **outside this repository**, confirmed present on this
machine at `../config/mqtt-api-service/` (sibling to this repo's parent, i.e.
`saas-system-iot/config/mqtt-api-service/`), containing `mqtt-api-service.env` (real values) and a
`certs/` directory (real AWS IoT/LG TLS material) — neither was opened or read by this task; their
presence and the absence of secret values in this documentation were verified by directory listing
only. `docker-compose.yml` mounts `mqtt-api-service.env` via `env_file` and `certs/` read-only at
`/app/certs`; it does not mount any config file — there is no `/app/config` in the image. The
in-repo `.env.example` is the variable-contract **template** (copy-and-fill), not real runtime
configuration, and is git-ignored alongside `.env`/`certs/*.{pem,crt,key}` (`.gitignore`).

The Kafka topic names (`KAFKA_COMMAND_TOPIC`, `KAFKA_COMMAND_SENT_TOPIC`,
`KAFKA_COMMAND_PUBLISH_FAILED_TOPIC`) are environment-configurable in this codebase but are, per the
platform-level Project Context, the shared platform contract — not independently choosable per
service in practice.

## 10. Build / test / vet commands

CONFIRMED — run in this task's session, all exit code 0:
```
go build ./...
go vet ./...
go test ./...
```
`go build ./cmd/api` (the single entrypoint) is equally valid and is what `Dockerfile` uses for the
production image; `go build ./...` was used for this task's validation since it covers every
package in one command and both succeed identically today (there is exactly one `main` package).
Existing Go test files confirming real (non-empty) coverage: `internal/adapters/api/lg/client_test.go`,
`internal/adapters/cache/device_state_store_test.go`, `internal/adapters/grpc/client/tracking_test.go`,
`internal/adapters/mongo/repository_test.go`, `internal/adapters/parser/lg_state_parser_test.go`,
`internal/application/commands/*_test.go` (confirmation_manager, dispatcher, expected_state,
payload, resolver, synthetic_ack), `internal/application/normalizers/lg_normalizer_test.go`,
`internal/infrastructure/config/config_test.go`, `internal/infrastructure/debuglog/debuglog_test.go`,
`internal/infrastructure/logger/logger_test.go`.

## 11. Constraints

- Go module `mqtt-api-service`, Go 1.24.1 (`go.mod`); avoid 1.25/1.26 per the repository's own
  documented convention.
- Direct dependencies: `eclipse/paho.mqtt.golang` (MQTT), `redis/go-redis/v9` (+ `alicebob/miniredis/v2`
  for tests), `segmentio/kafka-go`, `go.mongodb.org/mongo-driver`, `google.golang.org/grpc` +
  `google.golang.org/protobuf`, `google/uuid`, `joho/godotenv`, `go.uber.org/zap`.
- No ORM/migration framework; Mongo access is a thin hand-written repository over the official
  driver. No database migrations in this repository.
- No CI/CD workflow files were found inside this repository (`.github/` does not exist here); the
  root `saas-system-iot` repository has a `.github/agents/unittest.agent.md` but no workflow file
  targeting this repository specifically was found. UNKNOWN whether CI is defined elsewhere in the
  platform.
- No HTTP server runs in this process; `EXPOSE 8080` in `Dockerfile` is an explicitly-commented
  reserved port for a not-yet-implemented health/readiness endpoint.

## 12. Known risks / UNKNOWNs

- **CONFIRMED project-level risk (from the authoritative Notion `Project Context —
  saas-system-iot`, not independently re-derivable from this repository alone):**
  `mqtt-adapter-service` and `mqtt-api-service` both consume the shared Kafka topic
  `device.command.requested`, each with its own separate consumer group. Multi-bridge command
  ownership/routing across the two services is a known architectural risk the platform intends to
  stabilize before Voice Command Service work begins. This repository's `LooksLikeLGCommand` guard
  (§5) is the current mitigation that prevents this service from misclassifying the other bridge's
  commands as its own failures, but it does not implement a general ownership/routing solution —
  none is proposed by this task.
- **CONFIRMED, not implemented:** a generalized `confirmationStrategy` abstraction is a
  project-level future direction (per the same authoritative Project Context) and is **not**
  implemented anywhere in this repository today. This repository's confirmation mechanism (§6) is a
  concrete, LG-specific vendor-state + synthetic-ACK implementation, not an instance of that future
  abstraction — do not describe it as such.
- UNKNOWN: purpose/current relevance of `internal/adapters/parser/lg_message_parser.go` and
  `internal/application/normalizers/message_normalizer.go` beyond "pre-existing, unused code" (§2).
- UNKNOWN: whether/how an LG device online/offline status change should be forwarded to
  `tracking-platform` — evaluated but not implemented (explicit TODO in
  `internal/application/use_case/lg/state_polling.go`, `recordDeviceStatus`); status is currently
  only observable via Redis (`lg:device:<deviceID>:status`) and logs.
- UNKNOWN: whether CI/CD exists for this repository outside what is visible in this polyrepo
  checkout (§11).
