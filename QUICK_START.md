# QUICK START - mqtt-api-service

## 📦 Lo que recibiste

Un **ZIP completo** de `mqtt-api-service` con estructura lista para desarrollar. **~70% del código** está hecho y funcional.

```
mqtt-api-service.zip (24 KB)
├── cmd/api/main.go                           ✅ COMPLETO
├── internal/
│   ├── adapters/
│   │   ├── parser/lg_message_parser.go       ✅ COMPLETO (especulativo)
│   │   ├── mqtt/client.go                    ⚠️  STUB (implementar)
│   │   ├── grpc/client.go                    ⚠️  STUB (implementar)
│   │   ├── mongo/repository.go               ⚠️  STUB (implementar)
│   │   └── api/lg_client.go                  ⚠️  STUB (opcional)
│   ├── application/
│   │   ├── normalizers/message_normalizer.go ✅ COMPLETO
│   │   └── services/lg_service.go            ✅ COMPLETO
│   ├── domain/models.go                      ✅ COMPLETO
│   └── infrastructure/
│       └── config/config.go                  ⚠️  STUB (implementar)
├── config/
│   └── config.yaml                           ✅ COMPLETO
├── docker-compose.yml                        ✅ LISTO
├── Dockerfile                                ✅ LISTO
├── go.mod                                    ✅ COMPLETO
├── IMPLEMENTATION_GUIDE.md                   📖 LEE ESTO
├── README.md                                 📖 LEE ESTO
└── .gitignore                                ✅ LISTO
```

---

## 🚀 Próximos Pasos (En Orden)

### 1️⃣ INMEDIATO (30 minutos)

**Extrae el ZIP y valida los payloads reales de LG MQTT:**

```bash
unzip mqtt-api-service.zip
cd mqtt-api-service

# Abre cliente MQTT y suscribete a topic LG
mosquitto_sub -h mqtt.lgeapi.com -p 8883 \
  -u ${LG_MQTT_USER} -P ${LG_MQTT_PASS} \
  -t 'app/clients/YOUR_CLIENT_ID/push'

# Cuando LG publique un evento, copia el JSON exacto
# y compara con internal/adapters/parser/lg_message_parser.go (línea 14)
```

**Si el payload es diferente a lo especulado, ajusta `LGMessage` struct.**

### 2️⃣ DESPUÉS (2-3 días)

Implementa 4 adaptadores:

| Archivo | Status | Tiempo | Dependencia |
|---------|--------|--------|-------------|
| `internal/adapters/mqtt/client.go` | ⚠️ Stub | 2h | eclipse/paho |
| `internal/adapters/grpc/client.go` | ⚠️ Stub | 2h | google.golang.org/grpc |
| `internal/adapters/mongo/repository.go` | ⚠️ Stub | 3h | go.mongodb.org/mongo-driver |
| `internal/infrastructure/config/config.go` | ⚠️ Stub | 1h | github.com/spf13/viper |

**Total: 8 horas máximo**

---

## 📋 Arquitectura (Revisada)

Basada en los **topics reales de LG** que compartiste:

```
┌─────────────────────────────────────────────┐
│ LG MQTT Broker                              │
│ Topics:                                     │
│  • app/clients/{id}/push    ← Eventos      │
│  • app/clients/{id}/inbox   ← Entrada     │
│  • app/clients/{id}/outbox  ← Salida      │
└────────────────────┬────────────────────────┘
                     │
                     ↓
         mqtt-api-service
         ├─ Parsea: lg_message_parser.go   ✅
         ├─ Normaliza: message_normalizer.go ✅
         ├─ Almacena: MongoDB               ⚠️
         └─ Envía: gRPC → tracking-platform ⚠️
                     ↓
         tracking-platform:50051
         (ingestion-service)
                     ↓
         Publica a Kafka
         ├─ telemetry.normalized
         ├─ device.event.generated
         ├─ device.ack.received
         └─ device.status.changed
```

---

## ⚙️ Configuración Necesaria

### .env (crear en raíz del proyecto)

```bash
# LG MQTT
LG_CLIENT_ID=aa0d9ce5-0888-4b1e-ba7e-7da932f57c6a
LG_MQTT_USER=your_mqtt_user
LG_MQTT_PASS=your_mqtt_pass

# LG API
LG_API_CLIENT_ID=your_api_client_id
LG_API_CLIENT_SECRET=your_api_secret

# tracking-platform
TRACKING_PLATFORM_HOST=localhost:50051

# MongoDB
MONGO_URI=mongodb://localhost:27017
```

### Ejecutar localmente (con Docker)

```bash
# Iniciar MongoDB
docker-compose up -d

# Ejecutar servicio (después de implementar adaptadores)
go run cmd/api/main.go
```

---

## 🎯 Checklist de Implementación

```
VALIDACIÓN (30 min):
[ ] Extrae ZIP
[ ] Obtén payload real de LG MQTT
[ ] Compara con LGMessage struct
[ ] Ajusta si es diferente

ADAPTADORES (8 horas):
[ ] MQTT client (paho)
[ ] gRPC client (tracking-platform)
[ ] MongoDB repository
[ ] Config loader (viper)

TESTS (4 horas):
[ ] Unit tests (parser, normalizer)
[ ] Integration tests (MQTT mock)
[ ] End-to-end (tracking-platform mock)

DEPLOY (2 horas):
[ ] Dockerfile (ya está)
[ ] Docker-compose (ya está)
[ ] Health checks
[ ] Logs y monitoring
```

**Total: 18-20 horas = 2-3 días (1 dev)**

---

## 📂 Dónde Seguir

1. **Leer IMPLEMENTATION_GUIDE.md** (en el ZIP)
   - Qué está completo
   - Qué falta
   - Cómo implementar cada parte

2. **Leer README.md** (en el ZIP)
   - Overview del proyecto
   - Instalación
   - Desarrollo

3. **Validar payloads LG**
   - Abre cliente MQTT
   - Copia JSON exacto
   - Ajusta parsers

4. **Implementar adaptadores**
   - Uno por uno
   - Con tests
   - Integración progresiva

---

## 💡 Decisiones Tomadas

| Decisión | Por qué |
|----------|--------|
| **gRPC a tracking-platform** | Tu backend YA tiene servidor gRPC en 50051 |
| **NO Kafka entre mqtt-api y tracking** | Desacoplamiento ya existe internamente en tracking |
| **MongoDB para raw data** | Auditoría y debugging |
| **Estructura multi-brand** | Fácil agregar Samsung, Philips, etc después |
| **Topics dinámicos** | Basados en {clientId} de LG |

---

## 🔗 Dependencias Principales

```
github.com/eclipse/paho.mqtt.golang     # MQTT client
google.golang.org/grpc                  # gRPC client
go.mongodb.org/mongo-driver             # MongoDB driver
github.com/spf13/viper                  # Config management
go.uber.org/zap                         # Logging
```

Todas están en `go.mod`. Solo necesitas `go mod download`.

---

## 📞 Soporte

Si tienes dudas:

1. **¿Qué está hecho?** → `IMPLEMENTATION_GUIDE.md`
2. **¿Cómo ejecutar?** → `README.md`
3. **¿Estructura?** → Este archivo
4. **¿Payloads LG?** → IMPLEMENTATION_GUIDE.md sección "Validación"

---

## ✅ Última Validación

Antes de comenzar la implementación, confirma:

- [ ] ¿Tienes acceso MQTT a LG? ✅ (Ya lo dijiste)
- [ ] ¿Tienes client_id y secret? ✅ (Ya lo dijiste)
- [ ] ¿Tienes tracking-platform corriendo? (Verifica puerto 50051)
- [ ] ¿Tienes MongoDB disponible? (Local o cloud)
- [ ] ¿Payload LG validado?

Sin estas, los adaptadores no funcionarán.

---

**¿Listo para comenzar? Extrae el ZIP, valida el payload LG, y comienza con el adaptador MQTT.**
