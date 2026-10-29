# Data Aggregator Service

Recibe lecturas, las normaliza y forma lotes de ocho. Cuando el lote está completo llama al detector cuántico.

## Variables

- `DETECTOR_URL`
- `SAFE_MIN_C=37.0`
- `SAFE_MAX_C=38.0`
- `BATCH_SIZE=8`
- `SHOTS=2048`

Normalmente se ejecuta desde el repositorio `hybrid-deployment-orchestrator`.
