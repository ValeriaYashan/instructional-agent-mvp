"""
Tipos de dominio para el resultado de una solicitud de ensamblado de
Context Package (Iteracion 3).

Outcomes soportados:
- COMMITTED             : se ensamblo (o se hizo replay de) un
                          context_package.
- TASK_TYPE_UNRESOLVED  : task_type fuera del catalogo cerrado - Paso 1,
                          bloquea antes de cualquier retrieval.
- MAPPING_UNDEFINED     : task_type sin entrada en
                          TASK_TYPE_CONTEXT_MAPPING - Paso 2.
- NEWER_VERSION_AVAILABLE: existe una version aprobada mas nueva que la
                          version pineada explicitamente por la tarea y
                          no fue considerada - Paso 4, "bloquea".
                          Comparacion por version_number (semantica de
                          version), nunca por desigualdad de UUID
                          (CODE-CR-40).
- INVALID_OBJECT_VERSION: pinned_version_id no existe, o existe pero
                          pertenece a un object_id distinto de
                          target_object_id - vocabulario ya aprobado y
                          reutilizado de Iteracion 2 (CR-26/CR-28), NO
                          un outcome nuevo (CODE-CR-40).
- IDEMPOTENCY_CONFLICT  : misma idempotency_key, payload semantico de
                          solicitud distinto (CR-18, reutilizado).

Estos son los CUATRO outcomes de rechazo que impiden la creacion de un
context_package en esta iteracion (ver docstring de context_engine.py
para el analisis completo de por que los demas fallos del contrato de
10 pasos - RELATION_UNRESOLVED, NO_APPROVED_VERSION,
APPROVAL_INSUFFICIENT, evidencia MISSING/INSUFFICIENT - fluyen hacia
dentro del context_package como gaps[] / dependency_status[] /
evidence[] en vez de bloquear el ensamblado). TASK_TYPE_UNRESOLVED es
un caso especial: se rechaza en Task Intake ANTES de cualquier acceso a
base de datos, incluida idempotency_operation - nunca se persiste (a
diferencia de los otros tres, que si persisten via COMMIT explicito
para permitir replay).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID


class ContextAssemblyOutcome(str, Enum):
    COMMITTED = "COMMITTED"
    TASK_TYPE_UNRESOLVED = "TASK_TYPE_UNRESOLVED"
    MAPPING_UNDEFINED = "MAPPING_UNDEFINED"
    NEWER_VERSION_AVAILABLE = "NEWER_VERSION_AVAILABLE"
    INVALID_OBJECT_VERSION = "INVALID_OBJECT_VERSION"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"


@dataclass(frozen=True)
class ContextAssemblyResult:
    outcome: ContextAssemblyOutcome
    context_package_id: UUID | None = None
    context_package_hash: str | None = None
