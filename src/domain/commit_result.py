"""
Tipos de dominio para el resultado de una operacion de Approved State Commit.

Alcance: Iteracion 1 del MVP (fundacion de persistencia y concurrencia).
No incluye Producer/QA, Routing Resolution ni Human Approval funcional.

Outcomes soportados en esta iteracion:
- COMMITTED            : el puntero se movio exitosamente.
- STATE_CONFLICT        : la version previa esperada no coincidia con la
                          actual (CR-13/CR-14, Fase 3). No se inserta
                          ApprovedStateCommit para este outcome; la
                          transaccion completa se revierte (ROLLBACK ALL).
- IDEMPOTENCY_CONFLICT  : la misma idempotency_key se reutilizo con un
                          payload semanticamente distinto (CR-18).
- INVALID_OBJECT_VERSION: el object_version_id referenciado no existe o
                          no pertenece al object_id declarado.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID


class CommitOutcome(str, Enum):
    COMMITTED = "COMMITTED"
    STATE_CONFLICT = "STATE_CONFLICT"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    INVALID_OBJECT_VERSION = "INVALID_OBJECT_VERSION"


@dataclass(frozen=True)
class CommitResult:
    outcome: CommitOutcome
    commit_id: UUID | None = None
    object_id: str | None = None
    object_version_id: UUID | None = None
