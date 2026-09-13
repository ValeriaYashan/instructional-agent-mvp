"""
Tipos de dominio para Agent Run / Producer Execution (Iteracion 4,
Proposal v0.5).

AgentRunOutcome cubre los resultados de nivel de SOLICITUD (Fase A) y
de nivel de RUN LOGICO (estado terminal de agent_run). AttemptOutcome
cubre los resultados de nivel de INTENTO TECNICO (agent_run_attempt) -
ver Proposal v0.5 S13 para la taxonomia completa.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID


class AgentRunOutcome(str, Enum):
    # Resultados de Fase A / de solicitud.
    TASK_TYPE_UNRESOLVED = "TASK_TYPE_UNRESOLVED"
    INVALID_CONTEXT_PACKAGE = "INVALID_CONTEXT_PACKAGE"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    # Resultados terminales del logical run.
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    # Devuelto solo por polling (S10, paso 19) si el estado terminal no
    # se alcanza dentro del tiempo de espera acotado - no es un estado
    # de agent_run.status, es un resultado de la operacion de consulta.
    PENDING_POLL_TIMEOUT = "PENDING_POLL_TIMEOUT"


class AttemptOutcome(str, Enum):
    SUCCESS = "SUCCESS"
    TECHNICAL_FAILURE = "TECHNICAL_FAILURE"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    CLAIM_LOST = "CLAIM_LOST"


@dataclass(frozen=True)
class AgentRunResult:
    outcome: AgentRunOutcome
    logical_run_id: UUID | None = None
    object_version_id: UUID | None = None
    object_version_content_id: UUID | None = None  # = object_version_id (1:0..1)
