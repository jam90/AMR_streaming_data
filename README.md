# MIR250 · Monitorización en tiempo real con Kafka, InfluxDB y Grafana

Pipeline de datos para la monitorización en tiempo real de un robot móvil autónomo **MIR250**. El sistema captura datos de batería y estado de misiones a través de la API REST del robot, los distribuye mediante **Apache Kafka**, los almacena en **InfluxDB** y los visualiza en **Grafana**. También incluye flujos equivalentes en **Node-RED**.
Este proyecto ha sido desarrollado en un entorno educativo, por lo que ciertos pasos pueden ser distintos de los que se darían a nivel industrial.

---

## Arquitectura

```
MIR250 (REST API)
       │
       ▼
       └──► Python Producer  ──►  Apache Kafka  ──►  Python Consumer  ──►  InfluxDB  ──►  Grafana
       │                                                                     │
       └── Node-RED (flujos alternativos) ──────────────────────────────────►┘
```

### Topics de Kafka

| Topic | Contenido |
|-------|-----------|
| `mir250.battery` | Porcentaje de batería del robot |
| `mir250.mission.current` | Misión en ejecución en cada momento |
| `mir250.mission.completed` | Registro de misiones finalizadas (completadas o abortadas) |

### Measurements de InfluxDB

| Measurement | Datos almacenados |
|-------------|-------------------|
| `Datos_MIR` | Serie temporal de batería |
| `Misiones` | Historial de misiones (actuales y completadas) |

---

## Estructura del proyecto

```
├── Scripts/
│   ├── MIR250-battery-producer.py     # Productor solo de batería
│   ├── MIR250-battery-consumer.py     # Consumidor solo de batería → InfluxDB
│   ├── MIR250-producer.py             # Productor completo (batería + misiones)
│   └── MIR250-consumer.py             # Consumidor completo multihilo → InfluxDB
├── Node-red/
│   ├── 20260303-flow_main.json        # Flujo principal Node-RED
│   └── 20260303-flow_adicional.json   # Flujo adicional Node-RED
├── Kafka/
│   └── 1_Kafka_Inicio.pptx            # Guía de inicio con Kafka
├── Influx+Grafana/
│   └── 2025_Influxdb+Grafana.pptx     # Guía de InfluxDB y Grafana
└── MIR250/
    ├── MIR250-RestAPI.pptx            # Documentación de la API REST del robot
    └── Manual MIR250.docx             # Manual del robot
```

---

## Requisitos

### Python

- Python 3.8+
- `kafka-python`
- `influxdb-client`
- `requests`

Instalar dependencias:

```bash
pip install kafka-python influxdb-client requests
```

### Servicios externos

- **Apache Kafka** corriendo en `localhost:9092`
- **InfluxDB 2.x** corriendo en `localhost:8086`
- **MIR250** accesible por red
- **Grafana** corriendo en `localhost:3000`
---

## Configuración

Antes de ejecutar los scripts, es necesario realizar la instalación de InfluxDB, Grafana, node-red y Kafka, que se explica en los distintos documentos. Además, debes ajustar las siguientes variables en cada archivo:

```python
# IP del robot
MIR250_IP = "192.168.250.34"

# Kafka
KAFKA_BROKERS = ["localhost:9092"]

# InfluxDB
INFLUXDB_URL    = "http://localhost:8086"
INFLUXDB_TOKEN  = "<tu_token>"
INFLUXDB_ORG    = "<tu_organización>"
INFLUXDB_BUCKET = "<tu_bucket>"
```

> **Nota de seguridad:** Las credenciales y tokens del repositorio son de ejemplo. Sustitúyelos por los tuyos propios y no subas tokens reales a GitHub.

---

## Uso

### Opción A — Scripts completos (batería + misiones)

Ejecutar el **productor** (en una terminal):

```bash
python Scripts/MIR250-producer.py
```

Ejecutar el **consumidor** (en otra terminal):

```bash
python Scripts/MIR250-consumer.py
```

El consumidor lanza tres hilos en paralelo, uno por cada topic de Kafka.

### Opción B — Scripts solo de batería

```bash
python Scripts/MIR250-battery-producer.py
python Scripts/MIR250-battery-consumer.py
```

### Opción C — Node-RED

Importa los flujos desde la carpeta `Node-red/` en tu instancia de Node-RED. Consulta `Node-red/Instalación Node-red.pptx` para los pasos de instalación.

---

## Lógica de los datos

### Batería

El productor consulta el endpoint `/api/v2.0.0/status` del MIR250 cada 5 segundos y publica el `battery_percentage` en Kafka. El consumidor aplica **RBE (Report By Exception)**: solo escribe en InfluxDB cuando el valor cambia.

### Misiones

El productor detecta cambios en `mission_queue_id`:

- Si hay misión activa → publica en `mir250.mission.current` con nombre, ID y hora de inicio.
- Cuando una misión termina → publica en `mir250.mission.completed` con nombre, tiempos de inicio/fin, duración total y estado (`Done` / `Aborted`).

---

## Materiales de apoyo

Los documentos y presentaciones en las carpetas `Kafka/`, `Influx+Grafana/` y `MIR250/` incluyen guías paso a paso para poner en marcha cada componente del sistema.

---

## Licencia

Este proyecto se desarrolla con fines educativos y de investigación.
