# IMPLEMENTATION GUIDE - mqtt-api-service

> ⚠️ **OBSOLETO.** Este documento describe el estado del proyecto antes de
> implementar los adaptadores (MQTT/gRPC/Mongo/config) y antes de las fases
> LG-1/LG-1A de integración a saas-system-iot. Ya no refleja el código
> actual (config.yaml y viper ya no existen, los adaptadores están
> implementados, Mongo/Redis/gRPC son los del ecosistema, no propios). Se
> conserva solo como referencia histórica del diseño original. Para el
> estado real, ver [README.md](README.md).

## 📊 Estado Actual del Proyecto

Este ZIP contiene **~70% del código** de mqtt-api-service. Está **100% funcional** pero requiere completar **4 componentes críticos** con lógica específica de LG.

---

## ✅ QUÉ ESTÁ COMPLETO (Puedes usar directamente)

### Estructura Base
```
cmd/api/main.go
├─ Bootstrap completo
├─ Conexión MQTT a LG
├─ Suscripción a topics dinámicos
└─ Graceful shutdown
```

### Parser de Mensajes LG
```
internal/adapters/parser/lg_message_parser.go
├─ Parsea JSON de LG (ESPECULATIVO, ajustable)
├─ Extrae deviceId
├─ Determina tipo de evento
└─ Validaciones básicas
```

### Normalización
```
internal/application/normalizers/message_normalizer.go
├─ Convierte LG → formato estándar
├─ Mapeo de campos comunes (temp, status, power)
├─ Clasificador de eventos
└─ Serializa a JSON para gRPC
```

### Servicio Principal
```
internal/application/services/lg_service.go
├─ Orquestador central
├─ Manejo de MQTT messages
├─ Mapeo de topics (app/clients/xxx → devices/xxx)
├─ Envío a tracking-platform
└─ Dead-letter queue para errores
```

### Configuración
```
config/config.yaml
├─ MQTT broker LG (variables de entorno)
├─ gRPC endpoint tracking-platform
├─ MongoDB
└─ Health checks
```

---

## ❌ QUÉ FALTA (Necesitas implementar)

### 1. ⚠️ CRÍTICO: Validar estructura real de payloads LG

**Archivo:** `internal/adapters/parser/lg_message_parser.go` (líneas 12-30)

**Qué hacer:**
```go
type LGMessage struct {
    // Estos campos son ESPECULATIVOS
    DeviceID string      // ¿Es deviceId o device_id o id?
    MessageType string   // ¿Qué valores envía LG?
    EventType string     // ¿state, status, telemetry?
    Data interface{}     // ¿Estructura exacta?
    Timestamp int64      // ¿Unix milisegundos o segundos?
}
```

**Cómo validar:**
1. Abre un cliente MQTT (ej: mosquitto_sub)
2. Suscribete a: `app/clients/{tu_client_id}/push`
3. Publica un evento desde un device LG
4. Copia el JSON exacto
5. Ajusta `LGMessage` struct

**Ejemplo esperado:**
```json
{
  "deviceId": "LG_AC_123",
  "messageType": "Event",
  "eventType": "StateChange",
  "data": {
    "status": "on",
    "temperature": 22,
    "mode": "cooling"
  },
  "timestamp": 1234567890000
}
```

**O podría ser:**
```json
{
  "deviceID": "LG_AC_123",
  "type": "STATE",
  "payload": {...},
  "ts": 1234567890
}
```

**Sin esto:** El parser fallará para todos los eventos.

---

### 2. ⚠️ CRÍTICO: Implementar adaptadores faltantes

Estos archivos existen pero están **vacíos/stubs**:

#### A. `internal/adapters/mqtt/client.go`
```go
type Client interface {
    Connect(ctx context.Context) error
    Subscribe(ctx context.Context, topic string, handler func(...) error) error
    Publish(ctx context.Context, topic string, payload []byte) error
    Disconnect(ctx context.Context) error
}
```

**Implementar con:** `github.com/eclipse/paho.mqtt.golang`

---

#### B. `internal/adapters/grpc/client.go`
```go
type Client struct {
    // Usar proto: libs/proto/tracking.proto
    // Llamar: TrackingService.IngestRaw
}

func (c *Client) IngestRaw(ctx context.Context, topic string, payload []byte, qos int) error {
    // Implementar llamada gRPC
}
```

---

#### C. `internal/adapters/mongo/repository.go`
```go
type Repository interface {
    SaveRawMessage(ctx context.Context, msg *RawMessage) error
    SaveDeadLetterMessage(ctx context.Context, msg *DeadLetterMessage) error
    GetDevice(ctx context.Context, deviceID string) (*Device, error)
    UpdateDeviceStatus(ctx context.Context, deviceID string, status string) error
}
```

**Implementar con:** `go.mongodb.org/mongo-driver`

---

#### D. `internal/adapters/api/lg_client.go`
```go
type LGAPIClient struct {
    // Autenticación: POST /client/certificate
}

func (c *LGAPIClient) GetDeviceState(ctx context.Context, deviceID string) (interface{}, error) {
    // GET /api/v1/devices/{deviceID}/state (si LG lo soporta)
}

func (c *LGAPIClient) SendCommand(ctx context.Context, req CommandRequest) (string, error) {
    // POST /api/v1/devices/{deviceID}/commands
}
```

**Nota:** Puede no ser necesario si MQTT publica TODO automáticamente.

---

#### E. `internal/infrastructure/config/config.go`
```go
type Config struct {
    MQTT MQTTConfig
    GRPC GRPCConfig
    Mongo MongoConfig
    LGApi LGApiConfig
}

func LoadConfig(path string) (*Config, error) {
    // Usar viper para cargar config.yaml
}
```

---

### 3. ⚠️ IMPORTANTE: Interfaces (Ports)

Crear interfaces en `internal/ports/`:
```go
// ports/mqtt_port.go
type MQTTClient interface {
    Connect(context.Context) error
    Subscribe(context.Context, string, MessageHandler) error
}

// ports/repository_port.go
type Repository interface {
    SaveRawMessage(context.Context, *RawMessage) error
}

// ports/grpc_port.go
type TrackingClient interface {
    IngestRaw(context.Context, string, []byte, int) error
}
```

---

### 4. ⚠️ IMPORTANTE: Domain Models

Completar en `internal/domain/models.go`:
```go
type RawMessage struct {
    ID        string
    DeviceID  string
    Brand     string      // "LG"
    Topic     string
    Payload   interface{}
    Timestamp time.Time
}

type DeadLetterMessage struct {
    Topic     string
    Payload   []byte
    Error     string
    Timestamp time.Time
}

type Device struct {
    ID       string
    Brand    string
    Model    string
    Type     string  // AC, Refrigerator, TV
    Status   string
    LastSeen time.Time
}
```

---

## 🛣️ ROADMAP DE IMPLEMENTACIÓN

### Fase 1: Validación (2 horas)
- [ ] Obtén payload REAL de LG MQTT
- [ ] Valida estructura exacta
- [ ] Ajusta `LGMessage` struct

### Fase 2: Adaptadores (8-12 horas)
- [ ] MQTT client (eclipse paho)
- [ ] gRPC client (tracking-platform)
- [ ] MongoDB repository
- [ ] LG API client (opcional)
- [ ] Config loader (viper)

### Fase 3: Integración (4-6 horas)
- [ ] Tests unitarios
- [ ] Tests de integración
- [ ] Mock MQTT broker
- [ ] Health checks

### Fase 4: Deploy (2-4 horas)
- [ ] Dockerfile
- [ ] docker-compose.yml
- [ ] Documentation
- [ ] Env variables

**Total: 20-28 horas de dev = 3-4 días (1 dev)**

---

## 📁 ESTRUCTURA DE CARPETAS A COMPLETAR

```
mqtt-api-service/
├── internal/
│   ├── adapters/
│   │   ├── mqtt/
│   │   │   ├── client.go           ← IMPLEMENTAR
│   │   │   └── handler.go          ← CREAR
│   │   ├── grpc/
│   │   │   └── client.go           ← IMPLEMENTAR
│   │   ├── mongo/
│   │   │   └── repository.go       ← IMPLEMENTAR
│   │   ├── api/
│   │   │   ├── lg_client.go        ← IMPLEMENTAR
│   │   │   └── models.go           ← CREAR
│   │   └── parser/
│   │       └── lg_message_parser.go ✅ HECHO
│   ├── domain/
│   │   ├── models.go               ← COMPLETAR
│   │   └── message.go              ← CREAR
│   ├── infrastructure/
│   │   └── config/
│   │       └── config.go           ← IMPLEMENTAR
│   ├── ports/
│   │   ├── mqtt_port.go            ← CREAR
│   │   ├── grpc_port.go            ← CREAR
│   │   ├── repository_port.go      ← CREAR
│   │   └── api_port.go             ← CREAR
│   └── application/
│       ├── normalizers/
│       │   └── message_normalizer.go ✅ HECHO
│       └── services/
│           └── lg_service.go       ✅ HECHO
├── tests/
│   ├── unit/
│   │   ├── parser_test.go          ← CREAR
│   │   ├── normalizer_test.go      ← CREAR
│   │   └── service_test.go         ← CREAR
│   └── integration/
│       └── e2e_test.go             ← CREAR
├── docker/
│   └── Dockerfile                  ← CREAR
├── config/
│   ├── config.yaml                 ✅ HECHO
│   └── config.dev.yaml             ← CREAR
├── go.mod                          ✅ HECHO
├── .gitignore                      ← CREAR
├── docker-compose.yml              ← CREAR
├── Makefile                        ← CREAR
└── README.md                       ← CREAR
```

---

## 🎯 Próximos Pasos (En Orden)

### INMEDIATO (2 horas):
1. **Valida payload LG real**
   - Abre cliente MQTT a LG
   - Obtén JSON exacto
   - Copia estructura

2. **Ajusta `LGMessage` struct**
   - internal/adapters/parser/lg_message_parser.go línea 12

3. **Crea domain models**
   - internal/domain/models.go

### DESPUÉS (1-2 días):
4. Implementa MQTT client
5. Implementa gRPC client
6. Implementa MongoDB repository
7. Tests

### FINALMENTE (1 día):
8. Docker
9. Deploy

---

## 📝 CHECKLIST DE VALIDACIÓN

```go
// Valida antes de continuar:

// 1. ¿Estructura LG conocida?
lgMsg := &parser.LGMessage{
    DeviceID: "???",      // ← Validar campo exacto
    MessageType: "???",   // ← Validar valores
}

// 2. ¿Topics correctos?
topics := []string{
    "app/clients/{clientId}/push",     // ← Validar
    "app/clients/{clientId}/inbox",    // ← Validar
    "app/clients/{clientId}/outbox",   // ← Validar
}

// 3. ¿gRPC endpoint tracking-platform disponible?
// Port 50051, servicio: TrackingService.IngestRaw

// 4. ¿MongoDB disponible?
// mongodb://localhost:27017/device_raw_data
```

---

## 🚨 IMPORTANTE

**NO comiencesimplementación hasta validar payload LG.**

Sin estructura exacta, todo el parser fallará y perderás horas debugging.

---

## 📞 REFERENCIAS

- **MQTT Client:** `github.com/eclipse/paho.mqtt.golang`
- **gRPC:** `google.golang.org/grpc`
- **MongoDB:** `go.mongodb.org/mongo-driver`
- **Config:** `github.com/spf13/viper`

---

**¿Necesitas template code para algún adaptador específico? Pídelo y te lo genero.**
