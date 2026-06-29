# mqtt-api-service

Servicio que consume eventos de múltiples APIs IoT (LG, Samsung, etc) vía MQTT y los normaliza para enviar a tracking-platform.

## Arquitectura

```
LG MQTT Broker
    ↓
mqtt-api-service
    ├─ Parsea mensajes LG
    ├─ Normaliza a formato estándar
    ├─ Guarda raw data en MongoDB
    └─ Envía por gRPC a tracking-platform
         ↓
    tracking-platform (ingestion-service)
         ↓
    Publica a Kafka → otros servicios
```

## Topics MQTT LG

```
app/clients/{clientId}/push    ← Eventos del dispositivo
app/clients/{clientId}/inbox   ← Mensajes entrantes
app/clients/{clientId}/outbox  ← Mensajes salientes (comandos)
```

## Requisitos

- Go 1.21+
- MQTT broker LG (con acceso)
- MongoDB
- tracking-platform corriendo en puerto 50051 (gRPC)

## Instalación

```bash
# Clonar repositorio
git clone <repo>
cd mqtt-api-service

# Instalar dependencias
go mod download

# Crear archivo .env
cp .env.example .env
# Editar .env con credenciales LG

# Ejecutar
go run cmd/api/main.go
```

## Configuración

Ver `config/config.yaml`

Variables de entorno requeridas:
- `LG_CLIENT_ID`
- `LG_MQTT_USER`
- `LG_MQTT_PASS`
- `LG_API_CLIENT_ID`
- `LG_API_CLIENT_SECRET`
- `MONGO_URI` (opcional, default: localhost:27017)
- `TRACKING_PLATFORM_HOST` (opcional, default: localhost:50051)

## Desarrollo

Ver `IMPLEMENTATION_GUIDE.md` para detalles de qué está hecho y qué falta.

### Estado Actual

- [x] Estructura base
- [x] Parser MQTT (especulativo, requiere validación)
- [x] Normalización
- [x] Servicio principal
- [x] Configuración
- [ ] Adaptadores (MQTT, gRPC, MongoDB, API)
- [ ] Tests
- [ ] Docker

### Próximos pasos

1. Valida payload LG real (2 horas)
2. Implementa adaptadores (12 horas)
3. Tests (6 horas)
4. Deploy (4 horas)

## Logs

Usa structured logging con zap. Verifica `logs/` para detalles.

## Contacto

Ver IMPLEMENTATION_GUIDE.md para preguntas técnicas.
