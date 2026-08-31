# CLAUDE.md — mqtt-api-service Operating Rules

Permanent repository operating policy for Claude Code sessions in this repository. This is
policy, not task history — do not append task-specific notes here; those belong in the
corresponding Notion Development Task and, for deep repository facts, in
[docs/sdo/PROJECT_CONTEXT.md](docs/sdo/PROJECT_CONTEXT.md).

## Task authority

- Every non-trivial change in this repository is driven by an SDO Development Task tracked in
  Notion. Before editing, verify the task's Task ID, Project, Repository, Status, and Prompt
  Version against the Notion Development Tasks database. Do not act on a task whose identity or
  status does not match what was handed to you.
- A task's Functional Requirements (FR-*) and Acceptance Criteria (AC-*) are immutable once
  specified. Do not renumber, merge, split, weaken, strengthen, or reinterpret them. If the task
  is ambiguous or repository evidence contradicts it, STOP and report the blocker instead of
  inventing a resolution.
- Use `.claude/skills/development-task/SKILL.md` as the reusable execution procedure for
  Development Tasks in this repository.

## Smallest-scope changes

- Implement only what the current task's Authorized Changes / Expected Result actually requires.
  Do not perform unrelated cleanup, refactors, or "while I'm here" improvements.
- Do not add dependencies, new architecture layers, or new external contracts unless the task
  explicitly authorizes it.

## Git operations

- No Git write operations (`add`, `commit`, `push`, `pull`, `merge`, `rebase`, `reset`, `clean`,
  `stash`, tag creation, branch creation/deletion/switching) without the user's explicit,
  in-conversation authorization for that specific action. Read-only Git inspection
  (`status`, `diff`, `log`, `branch --show-current`, `ls-files`) is always allowed.

## Secrets

- Never copy vendor tokens/credentials (`LG_API_KEY`, `LG_ACCESS_TOKEN`, `LG_CLIENT_ID`/
  `LG_API_CLIENT_ID` real values), MQTT/AWS IoT TLS material (CA/cert/key file *contents*), Mongo
  or Redis connection strings/credentials, Kafka broker credentials, or any other runtime secret
  value into documentation, reports, command output excerpts, commit messages, or Notion updates.
- Runtime env files and TLS certificates live outside this repository (see
  [docs/sdo/PROJECT_CONTEXT.md](docs/sdo/PROJECT_CONTEXT.md) §9). Referencing an env **variable
  name** is fine; printing its current **value** from a real (non-`.example`) env file, or the
  contents of a real cert/key file, is not. Verifying that such a file *exists* (e.g. `ls`) is
  fine; reading (`cat`) or echoing its contents is not.

## Architecture and contract preservation

- Preserve the existing layering (`internal/domain`, `internal/application`,
  `internal/adapters/*`, `internal/infrastructure/*`) and the existing external contracts unless a
  task explicitly authorizes changing them:
  - The gRPC contract consumed from `tracking-platform` (`TrackingService.IngestRaw`,
    `internal/adapters/grpc/proto/tracking`). This service is gRPC **client-only**.
  - The Kafka device-command bridge contract (`device.command.requested` consumed,
    `device.command.sent` / `device.command.publish_failed` produced) and its consumer-group
    separation from `mqtt-adapter-service`'s own consumer on the same topic.
  - The synthetic command-ACK contract (`devices/<imei>/ack` pseudo-topic via gRPC `IngestRaw`) —
    LG has no native ACK; do not invent one without task authorization.
  - The Mongo raw-persistence document shape (`internal/adapters/mongo/repository.go`).
- Do not blur the boundary between the telemetry path (LG API/MQTT push → Redis/Mongo → gRPC) and
  the command-dispatch path (Kafka → LG API → Kafka/gRPC ACK). They are intentionally independent
  flows that happen to share the LG state cache and the gRPC client.
- Do not design or implement a new multi-bridge command ownership/routing strategy, and do not
  implement the project-level `confirmationStrategy` abstraction, unless a task explicitly
  authorizes it — both are documented as current, unresolved project-level items in
  [docs/sdo/PROJECT_CONTEXT.md](docs/sdo/PROJECT_CONTEXT.md) §12, not as work for this repository
  to originate on its own.

## Configuration and runtime changes

- Verify current runtime/config evidence (`internal/infrastructure/config/config.go`, `Dockerfile`,
  `docker-compose.yml`, `.env.example`, and the referenced sibling env/certs paths' *existence*,
  never their contents) before proposing or documenting any configuration behavior. Do not assume
  a variable's default or an env file's shape from memory or from another repository's
  conventions.
- Do not modify `Dockerfile`, `docker-compose.yml`, or any env file unless a task explicitly
  authorizes runtime/config changes.

## Deterministic evidence

- A claim is not evidence. Do not report a build, test, or vet result as passing unless you
  actually ran `go build`, `go test`, or `go vet` in this session and observed exit code 0.
- Before documenting a repository behavior as current, verify it against the current source
  files, not against a task's, README's, or Notion's prior description of it. Mark anything that
  cannot be verified this way as UNKNOWN rather than asserting it.

## STOP conditions

STOP and report the blocker instead of proceeding when:
- Repository evidence materially contradicts the task's FR/AC in a way that would require
  changing them, or would require an architecture, public API/proto, database schema, dependency,
  or security-policy change beyond what the task authorizes.
- An unrelated, unauthorized working-tree change is present and cannot be safely isolated from
  the task's own scope.
- A required validation command fails for a reason the task did not authorize fixing.
- The task would require modifying another repository in the `saas-system-iot` polyrepo
  (`tracking-platform`, `mqtt-adapter-service`, `iot-frontend`, `tracking-platform`'s
  `ingestion-service`, or the root `saas-system-iot` repo).
- Completing the task's documentation goal would require exposing a secret value, redesigning LG
  confirmation/command routing, or resolving the multi-bridge command-ownership risk — these are
  explicitly out of scope for documentation-only tasks.
