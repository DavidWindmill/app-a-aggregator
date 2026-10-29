"""Servicio agregador que normaliza lecturas y construye lotes para detección."""

from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from aggregator_service.observability import ObservabilityPublisher
from shared.models import AggregatedReading, QuantumBatch, QuantumDetectionResult, SensorReading

SAFE_MIN_C = float(os.getenv("SAFE_MIN_C", "37.0"))
SAFE_MAX_C = float(os.getenv("SAFE_MAX_C", "38.0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "8"))
INITIAL_DETECTOR_URL = os.getenv("DETECTOR_URL", "http://127.0.0.1:8001")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "120"))
SHOTS = int(os.getenv("SHOTS", "2048"))
SIMULATED_EXECUTION_TIME = float(os.getenv("SIMULATED_EXECUTION_TIME", "20"))
SIMULATED_RELIABILITY = float(os.getenv("SIMULATED_RELIABILITY", "0.10"))
SIMULATED_AVAILABILITY = float(os.getenv("SIMULATED_AVAILABILITY", "24"))
SIMULATED_COST = float(os.getenv("SIMULATED_COST", "0.20"))
SIMULATED_RESPONSE_TIME_MS = float(os.getenv("SIMULATED_RESPONSE_TIME_MS", "250"))
OBSERVABILITY_URL = os.getenv("OBSERVABILITY_URL", "")
APPLICATION_ID = os.getenv("APPLICATION_ID", "default")

if BATCH_SIZE <= 0 or BATCH_SIZE & (BATCH_SIZE - 1):
    raise RuntimeError("BATCH_SIZE debe ser una potencia de dos.")
if SAFE_MAX_C <= SAFE_MIN_C:
    raise RuntimeError("SAFE_MAX_C debe ser mayor que SAFE_MIN_C.")

buffer: list[AggregatedReading] = []
lock = asyncio.Lock()
observability = ObservabilityPublisher(OBSERVABILITY_URL, "data-aggregator")


class DetectorSwitchRequest(BaseModel):
    """Define la petición para cambiar el detector activo del agregador."""
    detector_url: str
    service_id: str | None = None
    reason: str | None = None


class SimulationRequest(BaseModel):
    """Define los parámetros de una simulación temporal de incumplimiento."""
    failed: bool | None = None
    execution_time: float | None = None
    reliability: float | None = None
    availability: float | None = None
    cost: float | None = None
    response_time_ms: float | None = None
    reason: str | None = None
    recover_after_seconds: float | None = None


class AggregatorState(BaseModel):
    """Conserva el estado mutable del servicio agregador durante su ejecución."""
    detector_url: str = INITIAL_DETECTOR_URL
    detector_service_id: str | None = None
    switch_reason: str | None = None
    failed: bool = False
    execution_time: float = SIMULATED_EXECUTION_TIME
    reliability: float = SIMULATED_RELIABILITY
    availability: float = SIMULATED_AVAILABILITY
    cost: float = SIMULATED_COST
    response_time_ms: float = SIMULATED_RESPONSE_TIME_MS
    failure_reason: str | None = None
    recover_at: float | None = None
    completed_batches: int = 0


state = AggregatorState()


DEFAULT_SIMULATION_VALUES = {
    "failed": False,
    "execution_time": SIMULATED_EXECUTION_TIME,
    "reliability": SIMULATED_RELIABILITY,
    "availability": SIMULATED_AVAILABILITY,
    "cost": SIMULATED_COST,
    "response_time_ms": SIMULATED_RESPONSE_TIME_MS,
}


def recover_if_needed() -> None:
    """Restaura automáticamente los fallos temporales después de 10-15 segundos.

    Esto permite simular degradaciones transitorias, como ocurriría en servicios externos reales.
    El reinicio/redespliegue real sigue existiendo, pero la avería inyectada no queda permanente.
    """
    recover_at = getattr(state, "recover_at", None)
    if recover_at is None or time.time() < float(recover_at):
        return
    state.failed = False
    state.execution_time = DEFAULT_SIMULATION_VALUES["execution_time"]
    state.reliability = DEFAULT_SIMULATION_VALUES["reliability"]
    state.availability = DEFAULT_SIMULATION_VALUES["availability"]
    state.cost = DEFAULT_SIMULATION_VALUES["cost"]
    state.response_time_ms = DEFAULT_SIMULATION_VALUES["response_time_ms"]
    if hasattr(state, "failure_reason"):
        state.failure_reason = None
    if hasattr(state, "reason"):
        state.reason = None
    state.recover_at = None
    append_event({"type": "temporary_failure_recovered", "message": "Agregador vuelve automáticamente a estado correcto tras fallo temporal."})


def now_iso() -> str:
    """Devuelve la fecha y hora actual en formato ISO 8601 y zona UTC."""
    return datetime.now(timezone.utc).isoformat()


def append_event(
    event: dict[str, Any],
    *,
    category: str = "operational",
    severity: str = "info",
) -> None:
    """Emite el evento por stdout y lo publica de forma no bloqueante."""
    payload = {"time": now_iso(), "service": "aggregator", **event}
    print(f"[AGGREGATOR] {json.dumps(payload, ensure_ascii=False)}", flush=True)
    observability.publish_later(
        category=category,
        event_type=str(event.get("type", "aggregator_event")),
        payload=payload,
        application_id=APPLICATION_ID,
        severity=severity,
        service_id=str(event.get("service_id") or "aggregator"),
        batch_id=event.get("batch_id"),
        timestamp=payload["time"],
    )


def normalize_temperature(value: float) -> float:
    """Normaliza una temperatura dentro del intervalo configurado."""
    return (value - SAFE_MIN_C) / (SAFE_MAX_C - SAFE_MIN_C)


def fail_if_simulated() -> None:
    """Aplica el retraso o error configurado por una simulación activa."""
    recover_if_needed()
    if state.failed:
        raise HTTPException(
            status_code=503,
            detail=state.failure_reason or "Fallo simulado del agregador.",
        )


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Gestiona el arranque y apagado mediante la API vigente de FastAPI."""
    append_event({
        "type": "service_started",
        "safe_interval_c": [SAFE_MIN_C, SAFE_MAX_C],
        "batch_size": BATCH_SIZE,
        "detector_url": state.detector_url,
    })
    observability.publish_later(
        category="aggregator",
        event_type="aggregator_status",
        payload={
            "buffered_readings": len(buffer),
            "batch_size": BATCH_SIZE,
            "completed_batches": state.completed_batches,
            "detector_url": state.detector_url,
            "detector_service_id": state.detector_service_id,
        },
        application_id=APPLICATION_ID,
        service_id="aggregator",
    )
    try:
        yield
    finally:
        append_event({"type": "service_stopped"})


app = FastAPI(title="Agregador de datos", version="1.2.0", lifespan=lifespan)


from aggregator_service.qos_signal import Signal
qos_signal = Signal()

@app.get("/health")
async def health() -> dict[str, object]:
    """Devuelve el estado de salud del componente para comprobaciones externas."""
    fail_if_simulated()
    return {
        "status": "ok",
        "safe_interval_c": [SAFE_MIN_C, SAFE_MAX_C],
        "batch_size": BATCH_SIZE,
        "detector_url": state.detector_url,
        "detector_service_id": state.detector_service_id,
    }


@app.get("/metrics")
async def metrics() -> dict[str, Any]:
    """Devuelve las métricas operativas expuestas por el componente."""
    recover_if_needed()
    return qos_signal.decorate({
        "status": "failed" if state.failed else "ok",
        "mode": "CPU",
        "execution_time": state.execution_time,
        "reliability": state.reliability,
        "availability": state.availability,
        "cost": state.cost,
        "response_time_ms": state.response_time_ms,
    })


@app.post("/admin/simulate")
async def simulate(request: SimulationRequest) -> dict[str, Any]:
    """Activa una simulación temporal con los parámetros recibidos."""
    if request.failed is not None:
        state.failed = request.failed
    if request.execution_time is not None:
        state.execution_time = request.execution_time
    if request.reliability is not None:
        state.reliability = request.reliability
    if request.availability is not None:
        state.availability = request.availability
    if request.cost is not None:
        state.cost = request.cost
    if request.response_time_ms is not None:
        state.response_time_ms = request.response_time_ms
    if request.reason is not None:
        state.failure_reason = request.reason
    if request.recover_after_seconds is not None:
        state.recover_at = time.time() + max(0.0, float(request.recover_after_seconds))
    append_event({"type": "simulation_updated", "simulation": state.model_dump()})
    return {"status": "updated", "simulation": state.model_dump()}


@app.post("/admin/relaunch")
async def relaunch() -> dict[str, Any]:
    """Simula relanzar el servicio: limpia únicamente el fallo de disponibilidad.

    Las métricas de coste y tiempo de respuesta no se resetean para que el
    monitor pueda distinguir entre una caída puntual y un servicio que sigue
    siendo ineficiente tras relanzarlo.
    """
    state.failed = False
    state.execution_time = DEFAULT_SIMULATION_VALUES["execution_time"]
    state.reliability = DEFAULT_SIMULATION_VALUES["reliability"]
    state.availability = DEFAULT_SIMULATION_VALUES["availability"]
    state.cost = DEFAULT_SIMULATION_VALUES["cost"]
    state.response_time_ms = DEFAULT_SIMULATION_VALUES["response_time_ms"]
    state.recover_at = None
    state.failure_reason = "Servicio relanzado por el monitor."
    append_event({"type": "service_relaunched", "state": state.model_dump()})
    return {"status": "relaunched", "state": state.model_dump()}


@app.post("/admin/detector")
async def switch_detector(request: DetectorSwitchRequest) -> dict[str, object]:
    """Actualiza el detector al que se enviarán los siguientes lotes."""
    previous_url = state.detector_url
    previous_service_id = state.detector_service_id
    state.detector_url = request.detector_url.rstrip("/")
    state.detector_service_id = request.service_id
    state.switch_reason = request.reason
    append_event({
        "type": "detector_switched",
        "previous_detector_url": previous_url,
        "previous_service_id": previous_service_id,
        "detector_url": state.detector_url,
        "service_id": request.service_id,
        "reason": request.reason,
    }, category="deployment")
    observability.publish_later(
        category="aggregator",
        event_type="detector_state",
        payload={
            "detector_url": state.detector_url,
            "service_id": state.detector_service_id,
            "reason": state.switch_reason,
        },
        application_id=APPLICATION_ID,
        service_id="aggregator",
    )
    return {
        "status": "detector_updated",
        "previous_detector_url": previous_url,
        "detector_url": state.detector_url,
        "service_id": state.detector_service_id,
    }


@app.get("/status")
async def status() -> dict[str, object]:
    """Devuelve el estado del búfer, los lotes y el detector activo."""
    async with lock:
        return {
            "buffered_readings": len(buffer),
            "batch_size": BATCH_SIZE,
            "completed_batches": state.completed_batches,
            "detector_url": state.detector_url,
            "detector_service_id": state.detector_service_id,
            "event_store": "observability-service",
        }


@app.post("/readings")
async def receive_reading(reading: SensorReading) -> dict[str, object]:
    """Recibe, normaliza y almacena una lectura enviada por un sensor."""
    fail_if_simulated()
    # La normalización usa el intervalo seguro configurado y puede producir
    # valores menores que 0 o mayores que 1; esos valores representan anomalías.
    normalized = normalize_temperature(reading.temperature_c)
    enriched = AggregatedReading(
        **reading.model_dump(),
        normalized_temperature=normalized,
    )

    batch: QuantumBatch | None = None
    # El bloqueo protege el búfer y evita que dos lecturas concurrentes formen
    # lotes con elementos duplicados o que se pierdan lecturas intermedias.
    async with lock:
        buffer.append(enriched)
        buffered_count = len(buffer)
        reading_record = {
            "time": now_iso(),
            "type": "reading_received",
            "sensor_id": reading.sensor_id,
            "zone": reading.zone,
            "sequence": reading.sequence,
            "timestamp": reading.timestamp.isoformat(),
            "temperature_c": reading.temperature_c,
            "normalized_temperature": normalized,
            "is_out_of_range": normalized < 0.0 or normalized > 1.0,
            "buffered_readings": buffered_count,
        }
        # El lote se extrae de forma atómica y se elimina del búfer antes de
        # abandonar la sección crítica. Las lecturas posteriores forman otro lote.
        if len(buffer) >= BATCH_SIZE:
            selected = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]
            batch = QuantumBatch(
                batch_id=f"batch-{uuid4().hex[:8]}",
                safe_min_c=SAFE_MIN_C,
                safe_max_c=SAFE_MAX_C,
                readings=selected,
                shots=SHOTS,
            )

    observability.publish_later(
        category="reading",
        event_type="reading_received",
        payload=reading_record,
        application_id=APPLICATION_ID,
        service_id=reading.sensor_id,
        timestamp=reading_record["time"],
    )

    if batch is None:
        return {
            "status": "buffering",
            "buffered_readings": buffered_count,
            "batch_size": BATCH_SIZE,
        }

    # La petición al detector se realiza fuera del bloqueo para no impedir la
    # recepción de nuevas lecturas durante un cálculo potencialmente costoso.
    detector_url = state.detector_url.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS, trust_env=False) as client:
            response = await client.post(
                f"{detector_url}/detect",
                json=batch.model_dump(mode="json"),
            )
            response.raise_for_status()
            detection = QuantumDetectionResult.model_validate(response.json())
    except (httpx.HTTPError, ValueError) as exc:
        # Si el detector falla, el lote se reintroduce al principio del búfer. Así
        # la recuperación o un cambio de alternativa no provoca pérdida de datos.
        async with lock:
            buffer[0:0] = batch.readings
        append_event({
            "type": "batch_detection_error",
            "batch_id": batch.batch_id,
            "detector_url": detector_url,
            "error": str(exc),
        }, category="error", severity="error")
        raise HTTPException(
            status_code=503,
            detail=f"No se pudo procesar el lote con el detector activo {detector_url}: {exc}",
        ) from exc

    record = {
        "time": now_iso(),
        "batch": batch.model_dump(mode="json"),
        "detection": detection.model_dump(mode="json"),
        "detector_url": detector_url,
        "detector_service_id": state.detector_service_id,
    }
    async with lock:
        state.completed_batches += 1

    observability.publish_later(
        category="batch",
        event_type="batch_completed",
        payload={"record": record},
        application_id=APPLICATION_ID,
        service_id="aggregator",
        batch_id=batch.batch_id,
        timestamp=record["time"],
    )
    observability.publish_later(
        category="aggregator",
        event_type="aggregator_status",
        payload={
            "buffered_readings": len(buffer),
            "batch_size": BATCH_SIZE,
            "completed_batches": state.completed_batches,
            "detector_url": state.detector_url,
            "detector_service_id": state.detector_service_id,
        },
        application_id=APPLICATION_ID,
        service_id="aggregator",
    )

    # Comunicación propia de la aplicación: la plataforma no interpreta ni
    # transporta estos resultados. La URL es una configuración de App A.
    result_url = os.getenv("RESULT_URL", "").rstrip("/")
    if result_url:
        try:
            async with httpx.AsyncClient(timeout=4, trust_env=False) as client:
                await client.post(result_url + "/results", json={"batch_id": batch.batch_id,
                    "result": detection.model_dump(mode="json")})
        except httpx.HTTPError:
            pass  # Un sumidero de demostración no bloquea el flujo de sensores.

    if detection.status == "anomaly_detected":
        anomaly_readings = [
            batch.readings[index].model_dump(mode="json")
            for index in detection.marked_indices
            if 0 <= index < len(batch.readings)
        ]
        anomaly_record = {
            "time": now_iso(),
            "type": "anomaly_detected",
            "batch_id": batch.batch_id,
            "detected_index": detection.detected_index,
            "marked_indices": detection.marked_indices,
            "marked_states": detection.marked_states,
            "measured_state": detection.measured_state,
            "detector_url": detector_url,
            "detector_service_id": state.detector_service_id,
            "anomalous_values": anomaly_readings,
            "message": detection.message,
        }
        append_event(anomaly_record, category="anomaly", severity="warning")
    return {
        "status": "batch_processed",
        "buffered_readings": len(buffer),
        "detection": detection.model_dump(mode="json"),
        "detector_url": detector_url,
        "detector_service_id": state.detector_service_id,
    }
