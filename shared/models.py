"""Modelos de datos compartidos entre los servicios del caso de estudio."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class SensorReading(BaseModel):
    """Representa una lectura individual producida por un sensor."""
    sensor_id: str
    zone: str
    sequence: int = Field(ge=1)
    timestamp: datetime
    temperature_c: float


class AggregatedReading(SensorReading):
    """Representa una lectura normalizada y enriquecida por el agregador."""
    normalized_temperature: float


class QuantumBatch(BaseModel):
    """Agrupa las lecturas enviadas a un detector de anomalías."""
    batch_id: str
    safe_min_c: float
    safe_max_c: float
    readings: list[AggregatedReading]
    shots: int = Field(default=2048, ge=128, le=100_000)


class QuantumDetectionResult(BaseModel):
    """Representa el resultado devuelto por un detector de anomalías."""
    batch_id: str
    status: Literal["normal", "anomaly_detected"]
    marked_indices: list[int]
    marked_states: list[str]
    measured_state: str | None
    detected_index: int | None
    grover_iterations: int
    counts: dict[str, int]
    num_qubits: int
    circuit_depth: int
    message: str
    circuit_text: str | None = None
