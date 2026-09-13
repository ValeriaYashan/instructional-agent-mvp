"""
Approved State Commit - Iteracion 1 (corregido - CODE-CR-01).

Implementa exactamente el contrato transaccional aprobado en:
    - Fase 3 v0.2, CR-13 (atomicidad: validar -> mutar puntero ->
      persistir ApprovedStateCommit final -> persistir AuditEvent ->
      COMMIT; ROLLBACK ALL ante cualquier fallo)
    - Fase 3 v0.2, CR-14 (concurrencia optimista null-safe:
      IS NOT DISTINCT FROM, soporta primera aprobacion desde NULL)
    - Iteration 1 Proposal v0.2, CR-17 (audit_event minimo dentro de la
      misma transaccion atomica)
    - Iteration 1 Proposal v0.2, CR-18 (idempotencia con verificacion de
      payload: misma key + mismo payload -> resultado existente; misma
      key + payload distinto -> IDEMPOTENCY_CONFLICT)
    - Iteration 1 Proposal v0.2, CR-19/CR-20 (integridad relacional
      puntero/version y commit/version, garantizada por foreign keys
      compuestas en el modelo fisico - ver models.py)
    - CODE-CR-01 (Human Code Review de Iteracion 1): el defecto de
      atomicidad de la version anterior committeaba una fila 'PENDING'
      en approved_state_commit en una transaccion separada, antes de
      saber si la operacion completa tendria exito. Esta version lo
      corrige: approved_state_commit SOLO recibe una fila cuando la
      operacion completa ya tuvo exito, insertada en la MISMA
      transaccion atomica que muta el puntero y escribe el AuditEvent.
      approved_state_commit.outcome ahora tiene un CHECK que solo
      permite 'COMMITTED' - no existe ningun otro valor posible.

Diseno de la reserva de concurrencia (CODE-CR-01):

    El problema original: SELECT ... FOR UPDATE no puede bloquear una
    fila que todavia no existe, por lo que resolver la concurrencia
    sobre una idempotency_key nunca antes vista requiere ALGUN
    mecanismo de reserva anterior a conocer el resultado final. La
    version anterior resolvia esto insertando esa reserva directamente
    en approved_state_commit y haciendo COMMIT de inmediato - eso violaba
    CR-13 porque dejaba una fila 'PENDING' persistida por fuera de la
    transaccion atomica de la operacion real.

    La correccion introduce una tabla separada, `idempotency_operation`,
    exclusivamente para la reserva de concurrencia, y la inserta como el
    PRIMER statement de la MISMA transaccion atomica que hace todo lo
    demas (validar -> mutar puntero -> insertar ApprovedStateCommit ->
    insertar AuditEvent -> COMMIT). Nunca se hace COMMIT de esa reserva
    por separado:

        - Si la operacion completa tiene exito, la reserva se actualiza
          a status='COMMITTED' y se confirma junto con todo lo demas en
          el mismo COMMIT.
        - Si la operacion falla en cualquier punto posterior
          (STATE_CONFLICT, version invalida, error tecnico), se hace
          ROLLBACK ALL - deshaciendo tambien la reserva. Nunca queda
          una fila 'CLAIMED' persistida de forma aislada.

    La serializacion de dos solicitudes concurrentes con la misma
    idempotency_key nunca antes vista ocurre por el comportamiento
    estandar de PostgreSQL ante un INSERT que colisionaria con una fila
    insertada (pero aun no confirmada) por otra transaccion: el INSERT
    de la segunda transaccion BLOQUEA hasta que la primera termine.
        - Si la primera hizo COMMIT: la segunda ve un conflicto real,
          su INSERT ... ON CONFLICT DO NOTHING no inserta nada, y
          procede a leer la fila ya confirmada (ahora sí visible) para
          devolver el resultado existente o IDEMPOTENCY_CONFLICT segun
          corresponda.
        - Si la primera hizo ROLLBACK: la fila conflictiva nunca existio
          para efectos de otras transacciones, por lo que el INSERT de
          la segunda tiene exito sin conflicto y esta se convierte en la
          nueva propietaria, revalidando todo desde cero.

    Esto mantiene aislado de ApprovedStateCommit el problema de
    concurrencia de idempotencia, sin introducir ningun estado
    transitorio persistible en la tabla que CODE-CR-01 protege.

Nota Iteracion 2 (CR-25, IC-31): idempotency_operation se generalizo
para tambien servir a WorkflowTransition (ver
src/persistence/repositories/workflow_transition.py), discriminada por
`operation_type`. Este modulo se actualizo unicamente para declarar
`operation_type='APPROVED_STATE_COMMIT'` en cada INSERT/SELECT/UPDATE
sobre esa tabla - el comportamiento externamente observable de este
modulo (commit_approved_state, initialize_approved_state_pointer,
get_current_approved_version_id) NO CAMBIO: STATE_CONFLICT sigue sin
persistir ninguna fila (ROLLBACK ALL total), y los 18 tests de
Iteracion 1 corren sin modificacion contra el esquema post-migracion
0002 (ver tests/integration/test_approved_state_commit.py).
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Optional

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.domain.commit_result import CommitOutcome, CommitResult
from src.persistence.models import (
    ApprovedStateCommit,
    ApprovedStatePointer,
    AuditEvent,
    IdempotencyOperation,
    ObjectVersion,
)

_MAX_OWNERSHIP_RETRIES = 5

# Iteracion 2 (CR-25, IC-31): idempotency_operation ahora es compartida.
# Todo lo que este modulo escribe/lee en esa tabla se declara explicitamente
# bajo este operation_type, sin cambiar el comportamiento observable de
# Iteracion 1 (ver docstring del modulo y de IdempotencyOperation en models.py).
_OPERATION_TYPE = "APPROVED_STATE_COMMIT"


def compute_idempotency_payload_hash(
    object_id: str,
    object_version_id: uuid.UUID,
    expected_previous_approved_version_id: Optional[uuid.UUID],
) -> str:
    """
    Hash deterministico del payload semantico de un Approved State Commit.
    Dos solicitudes con la misma idempotency_key y el mismo hash se tratan
    como la misma operacion logica (CR-18).
    """
    raw = "|".join(
        [
            object_id,
            str(object_version_id),
            str(expected_previous_approved_version_id) if expected_previous_approved_version_id else "NULL",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def initialize_approved_state_pointer(session: Session, object_id: str) -> None:
    """
    Crea explicitamente el puntero de estado aprobado para un object_id
    nuevo, con current_approved_version_id = NULL. Debe llamarse antes
    de la primera aprobacion (ver Iteration 1 Proposal v0.1, seccion 12).
    """
    session.execute(
        pg_insert(ApprovedStatePointer)
        .values(object_id=object_id, current_approved_version_id=None)
        .on_conflict_do_nothing(index_elements=["object_id"])
    )
    session.commit()


def get_current_approved_version_id(session: Session, object_id: str) -> Optional[uuid.UUID]:
    row = session.execute(
        select(ApprovedStatePointer.current_approved_version_id).where(
            ApprovedStatePointer.object_id == object_id
        )
    ).scalar_one_or_none()
    return row


def commit_approved_state(
    session: Session,
    object_id: str,
    object_version_id: uuid.UUID,
    expected_previous_approved_version_id: Optional[uuid.UUID],
    idempotency_key: str,
) -> CommitResult:
    """
    Punto de entrada unico para mutar el Approved State Pointer.

    Cada intento de este bucle ocurre dentro de UNA transaccion; el
    primer statement de esa transaccion es siempre el intento de reserva
    sobre idempotency_operation (ver docstring del modulo).
    """
    payload_hash = compute_idempotency_payload_hash(
        object_id, object_version_id, expected_previous_approved_version_id
    )

    for _ in range(_MAX_OWNERSHIP_RETRIES):
        # Paso 0 (dentro de la transaccion atomica): intentar la reserva.
        # Este INSERT bloquea automaticamente si otra transaccion tiene
        # una reserva no confirmada con la misma key (comportamiento
        # estandar de PostgreSQL ante colision de UNIQUE con fila
        # concurrente no confirmada) - ver docstring del modulo.
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
                object_id=object_id,
                object_version_id=object_version_id,
                expected_previous_approved_version_id=expected_previous_approved_version_id,
                idempotency_key=idempotency_key,
                payload_hash=payload_hash,
            )

        # La key ya existe (o existio y ya fue confirmada). Al llegar aqui
        # el INSERT anterior ya espero a que cualquier transaccion en
        # conflicto terminara, asi que la fila -si existe- esta
        # confirmada y es segura de leer sin bloqueo adicional.
        session.rollback()  # descarta el intento de INSERT que no aplico
        existing = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == _OPERATION_TYPE,
                IdempotencyOperation.idempotency_key == idempotency_key,
            )
        ).scalar_one_or_none()

        if existing is None:
            # La propietaria anterior hizo ROLLBACK completo (fallo o
            # STATE_CONFLICT). La key quedo libre: reintentar como nueva
            # propietaria.
            continue

        if existing.idempotency_payload_hash != payload_hash:
            session.rollback()
            return CommitResult(outcome=CommitOutcome.IDEMPOTENCY_CONFLICT)

        # Mismo payload: devolver el resultado ya persistido. En esta
        # iteracion el unico status final observable es 'COMMITTED'
        # (ver docstring de IdempotencyOperation).
        commit_row = session.execute(
            select(ApprovedStateCommit).where(
                ApprovedStateCommit.commit_id == existing.commit_id
            )
        ).scalar_one()
        session.rollback()  # lectura, nada que confirmar
        return CommitResult(
            outcome=CommitOutcome.COMMITTED,
            commit_id=commit_row.commit_id,
            object_id=commit_row.object_id,
            object_version_id=commit_row.object_version_id,
        )

    raise RuntimeError(
        "No se pudo resolver la propiedad de la idempotency_key tras "
        f"{_MAX_OWNERSHIP_RETRIES} intentos; posible contencion sostenida."
    )


def _proceed_as_owner(
    session: Session,
    object_id: str,
    object_version_id: uuid.UUID,
    expected_previous_approved_version_id: Optional[uuid.UUID],
    idempotency_key: str,
    payload_hash: str,
) -> CommitResult:
    # Paso 1: validar que la version referenciada existe y pertenece al object_id.
    version_exists = session.execute(
        select(ObjectVersion.object_version_id).where(
            ObjectVersion.object_version_id == object_version_id,
            ObjectVersion.object_id == object_id,
        )
    ).scalar_one_or_none()

    if version_exists is None:
        session.rollback()  # revierte tambien la reserva CLAIMED
        return CommitResult(outcome=CommitOutcome.INVALID_OBJECT_VERSION)

    # Paso 2: concurrencia optimista null-safe (CR-14).
    update_result = session.execute(
        update(ApprovedStatePointer)
        .where(ApprovedStatePointer.object_id == object_id)
        .where(
            text(
                "current_approved_version_id IS NOT DISTINCT FROM :expected"
            ).bindparams(expected=expected_previous_approved_version_id)
        )
        .values(current_approved_version_id=object_version_id)
    )

    if update_result.rowcount == 0:
        # STATE_CONFLICT: se revierte TODA la transaccion, incluida la
        # reserva CLAIMED (Fase 3 CR-13: ningun ApprovedStateCommit ni
        # ninguna reserva de idempotencia persiste en este caso).
        session.rollback()
        return CommitResult(outcome=CommitOutcome.STATE_CONFLICT)

    # Paso 3: insertar el ApprovedStateCommit YA FINAL (nunca 'PENDING').
    commit_id = uuid.uuid4()
    session.add(
        ApprovedStateCommit(
            commit_id=commit_id,
            object_id=object_id,
            object_version_id=object_version_id,
            expected_previous_approved_version_id=expected_previous_approved_version_id,
            actual_previous_approved_version_id=expected_previous_approved_version_id,
            idempotency_key=idempotency_key,
            idempotency_payload_hash=payload_hash,
            outcome="COMMITTED",
        )
    )

    # Paso 4: audit event minimo, misma transaccion (CR-17).
    session.add(
        AuditEvent(
            event_type="APPROVED_STATE_COMMIT",
            object_id=object_id,
            object_version_id=object_version_id,
            commit_id=commit_id,
            payload={
                "idempotency_payload_hash": payload_hash,
                "expected_previous_approved_version_id": (
                    str(expected_previous_approved_version_id)
                    if expected_previous_approved_version_id
                    else None
                ),
            },
        )
    )

    # Paso 5: finalizar la reserva de idempotencia en la MISMA transaccion.
    session.execute(
        update(IdempotencyOperation)
        .where(
            IdempotencyOperation.operation_type == _OPERATION_TYPE,
            IdempotencyOperation.idempotency_key == idempotency_key,
        )
        .values(status="COMMITTED", commit_id=commit_id)
    )

    session.commit()

    return CommitResult(
        outcome=CommitOutcome.COMMITTED,
        commit_id=commit_id,
        object_id=object_id,
        object_version_id=object_version_id,
    )
