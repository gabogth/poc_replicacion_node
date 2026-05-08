# Kafka Dashboard (Node.js)

Aplicación sencilla en Node.js que muestra:

- Conectores de Kafka Connect.
- Tópicos de Kafka.
- Consumer Group IDs por tópico.

## Requisitos

- Node.js 18+.
- Kafka CLI disponible (`kafka-topics.sh` y `kafka-consumer-groups.sh`).
- Kafka Connect con REST API habilitada.

## Variables de entorno

- `PORT` (default: `3000`)
- `KAFKA_BOOTSTRAP` (default: `localhost:9092`)
- `KAFKA_CONNECT_URL` (default: `http://localhost:8083`)
- `KAFKA_BIN` (default: `/opt/kafka/bin`)

## Uso

```bash
npm start
```

Abrir: `http://localhost:3000`
