"""
Workflow/State Transition Manager - Iteracion 2.

Implementa exactamente el contrato aprobado en:
    - Component Design v0.2, CR-06/CR-07 (unico dueno de las
      transiciones del Instructional Object Lifecycle; Object Version /
      Workflow State Store logicamente separado del Approved State
      Pointer)
    - Iteration 2 Proposal v0.4, CR-21 (vocabulario completo del
      lifecycle, KNOWN_STATES)
    - Iteration 2 Proposal v0.4, CR-22 (KNOWN_STATES separado de
      ENABLED_TRANSITIONS_ITERATION_2; ninguna transicion fuera de
      ELIGIBLE_PENDING -> DRAFT se ejecuta, aunque este en el
      vocabulario)
    - Iteration 2 Proposal v0.4, CR-23 (concurrencia optimista
      explicita: UPDATE condicional sobre expected_from_state, atomico
      con la insercion de workflow_transition)
    - Iteration 2 Proposal v0.4, CR-24 (provenance: requested_by,
      correlation_id; causation_id diferido con razon explicita)
    - Iteration 2 Proposal v0.4, CR-25 (reserva de idempotencia
      reutilizando idempotency_operation, generalizada, discriminada
      por operation_type='WORKFLOW_TRANSITION')
    - Iteration 2 Proposal v0.4, CR-26 (target_object_version_id
      explicito, validado contra target_object_id antes de cualquier
      mutacion)
    - Iteration 2 Proposal v0.4, CR-27/CR-28 (semantica de replay para
      resultados rechazados: UNKNOWN_STATE,
      TRANSITION_NOT_ENABLED_THIS_ITERATION, STATE_CONFLICT,
      INVALID_OBJECT_VERSION persisten via COMMIT explicito, nunca
      ROLLBACK, para permitir replay identico)
    - IC-31 (dominio cerrado de operation_type, forzado por CHECK de
      base de datos - ver models.py)
    - CODE-CR-32 (Human Code Review de Iteracion 2): el hash de payload
      semantico ahora incluye requested_by, no solo
      target_object_id/target_object_version_id/from_state/to_state.
      Dos solicitantes distintos con la misma idempotency_key y los
      mismos campos de transicion ya NO se tratan como el mismo replay
      - producen IDEMPOTENCY_CONFLICT. correlation_id se excluye
      deliberadamente del hash (es generado automaticamente por
      llamada y un reintento legitimo puede recibir uno nuevo sin dejar
      de ser la misma solicitud). causation_id permanece diferido
      (CR-24).

Diferencia deliberada respecto al patron de Approved State Commit de
Iteracion 1: alli, STATE_CONFLICT hace ROLLBACK ALL y no persiste
ninguna fila. Aqui, los CUATRO outcomes de rechazo especificos de
Workflow Transition persisten su resultado (status='REJECTED',
rejected_reason=<outcome>) via COMMIT, precisamente porque CR-27 exige
que un reintento con la misma idempotency_key y el mismo payload
reciba el mismo resultado logico, incluido un resultado rechazado. El
contrato ya cerrado de Approved State Commit (Iteracion 1) no se toca.

Task Intake (validacion de task_type) ocurre ANTES de cualquier
interaccion con la base de datos - ver task_intake.py.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.domain.transition_result import TransitionOutcome, TransitionResult
from src.persistence.models import KNOWN_STATES, IdempotencyOperation, ObjectVersion, WorkflowTransition
from src.persistence.repositories.task_intake import is_known_task_type

_MAX_OWNERSHIP_RETRIES = 5
_OPERATION_TYPE = "WORKFLOW_TRANSITION"

# CR-22: separado explicitamente de KNOWN_STATES (vocabulario completo,
# importado de models.py). Unica transicion funcionalmente habilitada
# en Iteracion 2 - ninguna otra se ejecuta, aunque este/sean/parte del
# vocabulario del lifecycle aprobado.
ENABLED_TRANSITIONS_ITERATION_2: frozenset[tuple[str, str]] = frozenset(
    {
        ("ELIGIBLE_PENDING", "DRAFT"),
    }
)

_REJECTED_OUTCOMES = {
    TransitionOutcome.UNKNOWN_STATE,
    TransitionOutcome.TRANSITION_NOT_ENABLED_THIS_ITERATION,
    TransitionOutcome.STATE_CONFLICT,
    TransitionOutcome.INVALID_OBJECT_VERSION,
}


def compute_transition_payload_hash(
    target_object_id: str,
    target_object_version_id: uuid.UUID,
    from_state: str,
    to_state: str,
    requested_by: str,
) -> str:
    """
    Hash deterministico del payload semantico de una solicitud de
    transicion. Dos solicitudes con la misma idempotency_key y el mismo
    hash se tratan como la misma operacion logica (CR-18, reutilizado).

    CODE-CR-32: requested_by participa del hash porque es parte del
    payload semantico segun CR-24 - dos solicitantes distintos NO son
    la misma operacion logica, aunque el resto de los campos coincida.
    `correlation_id` se excluye deliberadamente: es generado
    automaticamente en cada llamada (ver request_workflow_transition) y
    un reintento legitimo puede recibir un correlation_id nuevo sin
    dejar de ser, semanticamente, la misma solicitud - incluirlo
    rompería el replay idempotente. `causation_id` permanece diferido
    (CR-24) y tampoco participa.
    """
    raw = "|".join(
        [
            target_object_id,
            str(target_object_version_id),
            from_state,
            to_state,
            requested_by,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def request_workflow_transition(
    session: Session,
    task_type: str,
    target_object_id: str,
    target_object_version_id: uuid.UUID,
    from_state: str,
    to_state: str,
    requested_by: str,
    idempotency_key: str,
    correlation_id: Optional[str] = None,
) -> TransitionResult:
    """
    Punto de entrada unico de Task Intake + Workflow/State Transition
    Manager para este slice.
    """
    # Task Intake: se rechaza ANTES de tocar cualquier tabla, incluida
    # idempotency_operation - un task_type invalido no es un intento de
    # operacion legitimo.
    if not is_known_task_type(task_type):
        return TransitionResult(outcome=TransitionOutcome.TASK_TYPE_UNRESOLVED)

    correlation_id = correlation_id or str(uuid.uuid4())
    payload_hash = compute_transition_payload_hash(
        target_object_id, target_object_version_id, from_state, to_state, requested_by
    )

    for _ in range(_MAX_OWNERSHIP_RETRIES):
        # Paso 0: intento de reserva - mismo mecanismo de bloqueo que
        # Iteracion 1 (ver approved_state.py), ahora bajo
        # operation_type='WORKFLOW_TRANSITION'.
        reservation_won = session.execute(
            pg_insert(IdempotencyOperation)
            .values(
                operation_type=_OPERATION_TYPE,
                idempotency_key=idempotency_key,
                idempotency_payload_hash=payload_hash,
                status="CLAIMED",
                commit_id=None,
                transition_id=None,
                rejected_reason=None,
            )
            .on_conflict_do_nothing(index_elements=["operation_type", "idempotency_key"])
            .returning(IdempotencyOperation.idempotency_key)
        ).scalar_one_or_none()

        if reservation_won is not None:
            return _proceed_as_owner(
                session=session,
                target_object_id=target_object_id,
                target_object_version_id=target_object_version_id,
                from_state=from_state,
                to_state=to_state,
                requested_by=requested_by,
                correlation_id=correlation_id,
                idempotency_key=idempotency_key,
                payload_hash=payload_hash,
            )

        # La key ya existe (confirmada - COMMITTED o REJECTED, nunca
        # CLAIMED de forma durable, ver docstring de IdempotencyOperation).
        session.rollback()
        existing = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == _OPERATION_TYPE,
                IdempotencyOperation.idempotency_key == idempotency_key,
            )
        ).scalar_one_or_none()

        if existing is None:
            # Solo ocurre ante un fallo tecnico no controlado que forzo
            # un rollback real (nunca por un REJECTED normal, que
            # siempre hace COMMIT - CR-27). Reintentar como propietaria.
            continue

        if existing.idempotency_payload_hash != payload_hash:
            session.rollback()
            return TransitionResult(outcome=TransitionOutcome.IDEMPOTENCY_CONFLICT)

        # Mismo payload: replay del resultado final ya persistido,
        # exitoso o rechazado (CR-27).
        result = _replay_result(session, existing)
        session.rollback()  # lectura, nada que confirmar
        return result

    raise RuntimeError(
        "No se pudo resolver la propiedad de la idempotency_key tras "
        f"{_MAX_OWNERSHIP_RETRIES} intentos; posible contencion sostenida."
    )


def _replay_result(session: Session, existing: IdempotencyOperation) -> TransitionResult:
    if existing.status == "COMMITTED":
        transition = session.execute(
            select(WorkflowTransition).where(
                WorkflowTransition.transition_id == existing.transition_id
            )
        ).scalar_one()
        return TransitionResult(
            outcome=TransitionOutcome.COMMITTED,
            transition_id=transition.transition_id,
            object_version_id=transition.object_version_id,
            from_state=transition.from_state,
            to_state=transition.to_state,
        )

    # status == 'REJECTED' (unico otro valor final posible - CR-30)
    return TransitionResult(outcome=TransitionOutcome(existing.rejected_reason))


def _reject(
    session: Session,
    idempotency_key: str,
    reason: TransitionOutcome,
) -> TransitionResult:
    """
    CR-27: un resultado rechazado de Workflow Transition PERSISTE via
    COMMIT explicito (nunca ROLLBACK), para permitir replay identico.
    Diferencia deliberada respecto a Approved State Commit (Iteracion 1),
    donde STATE_CONFLICT si hace ROLLBACK ALL - ese contrato no se toca.
    """
    session.execute(
        update(IdempotencyOperation)
        .where(
            IdempotencyOperation.operation_type == _OPERATION_TYPE,
            IdempotencyOperation.idempotency_key == idempotency_key,
        )
        .values(status="REJECTED", rejected_reason=reason.value)
    )
    session.commit()
    return TransitionResult(outcome=reason)


def _proceed_as_owner(
    session: Session,
    target_object_id: str,
    target_object_version_id: uuid.UUID,
    from_state: str,
    to_state: str,
    requested_by: str,
    correlation_id: str,
    idempotency_key: str,
    payload_hash: str,
) -> TransitionResult:
    # Paso 1 (CR-26, CR-28): target_object_version_id pertenece a
    # target_object_id.
    version_exists = session.execute(
        select(ObjectVersion.object_version_id).where(
            ObjectVersion.object_version_id == target_object_version_id,
            ObjectVersion.object_id == target_object_id,
        )
    ).scalar_one_or_none()

    if version_exists is None:
        return _reject(session, idempotency_key, TransitionOutcome.INVALID_OBJECT_VERSION)

    # Paso 2 (CR-21): from_state y to_state pertenecen al vocabulario
    # completo del lifecycle.
    if from_state not in KNOWN_STATES or to_state not in KNOWN_STATES:
        return _reject(session, idempotency_key, TransitionOutcome.UNKNOWN_STATE)

    # Paso 3 (CR-22): la transicion especifica esta habilitada en esta
    # iteracion - no se simula ejecucion de componentes fuera de alcance.
    if (from_state, to_state) not in ENABLED_TRANSITIONS_ITERATION_2:
        return _reject(
            session, idempotency_key, TransitionOutcome.TRANSITION_NOT_ENABLED_THIS_ITERATION
        )

    # Paso 4 (CR-23): concurrencia optimista explicita.
    update_result = session.execute(
        update(ObjectVersion)
        .where(ObjectVersion.object_version_id == target_object_version_id)
        .where(ObjectVersion.status == from_state)
        .values(status=to_state)
    )

    if update_result.rowcount == 0:
        return _reject(session, idempotency_key, TransitionOutcome.STATE_CONFLICT)

    # Paso 5: insertar workflow_transition (transicion EFECTIVAMENTE
    # aplicada) - la mutacion de estado y esta insercion son atomicas
    # (CR-23).
    transition_id = uuid.uuid4()
    session.add(
        WorkflowTransition(
            transition_id=transition_id,
            object_version_id=target_object_version_id,
            from_state=from_state,
            to_state=to_state,
            idempotency_key=idempotency_key,
            idempotency_payload_hash=payload_hash,
            requested_by=requested_by,
            correlation_id=correlation_id,
            causation_id=None,  # CR-24: diferido, ver docstring del modulo
        )
    )

    # Paso 6: finalizar la reserva de idempotencia en la MISMA transaccion.
    session.execute(
        update(IdempotencyOperation)
        .where(
            IdempotencyOperation.operation_type == _OPERATION_TYPE,
            IdempotencyOperation.idempotency_key == idempotency_key,
        )
        .values(status="COMMITTED", transition_id=transition_id)
    )

    session.commit()

    return TransitionResult(
        outcome=TransitionOutcome.COMMITTED,
        transition_id=transition_id,
        object_version_id=target_object_version_id,
        from_state=from_state,
        to_state=to_state,
    )
