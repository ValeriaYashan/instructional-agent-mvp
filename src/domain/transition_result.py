"""
Tipos de dominio para el resultado de una solicitud de transicion del
Instructional Object Lifecycle (Iteracion 2).

Outcomes soportados:
- COMMITTED                          : la transicion se aplico.
- STATE_CONFLICT                     : from_state esperado no coincidia
                                        con el estado actual (CR-23).
- IDEMPOTENCY_CONFLICT                : misma idempotency_key, payload
                                        semanticamente distinto (CR-18,
                                        reutilizado).
- INVALID_OBJECT_VERSION             : target_object_version_id no
                                        pertenece a target_object_id
                                        (CR-26, CR-28).
- UNKNOWN_STATE                      : from_state o to_state no
                                        pertenecen a KNOWN_STATES.
- TRANSITION_NOT_ENABLED_THIS_ITERATION : la transicion es parte del
                                        vocabulario pero no esta
                                        habilitada en Iteracion 2
                                        (CR-22).
- TASK_TYPE_UNRESOLVED                : task_type fuera del catalogo
                                        cerrado de Task Intake - se
                                        rechaza antes de tocar cualquier
                                        tabla, incluida
                                        idempotency_operation.

Los cuatro outcomes de rechazo especificos de Workflow Transition
(STATE_CONFLICT, INVALID_OBJECT_VERSION, UNKNOWN_STATE,
TRANSITION_NOT_ENABLED_THIS_ITERATION) persisten su resultado en
idempotency_operation (status='REJECTED', rejected_reason=<outcome>)
para permitir replay identico ante un reintento (CR-27). IDEMPOTENCY_CONFLICT
y TASK_TYPE_UNRESOLVED nunca persisten nada nuevo.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID


class TransitionOutcome(str, Enum):
    COMMITTED = "COMMITTED"
    STATE_CONFLICT = "STATE_CONFLICT"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    INVALID_OBJECT_VERSION = "INVALID_OBJECT_VERSION"
    UNKNOWN_STATE = "UNKNOWN_STATE"
    TRANSITION_NOT_ENABLED_THIS_ITERATION = "TRANSITION_NOT_ENABLED_THIS_ITERATION"
    TASK_TYPE_UNRESOLVED = "TASK_TYPE_UNRESOLVED"


@dataclass(frozen=True)
class TransitionResult:
    outcome: TransitionOutcome
    transition_id: UUID | None = None
    object_version_id: UUID | None = None
    from_state: str | None = None
    to_state: str | None = None
