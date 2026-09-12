"""
Modelo fisico de Iteracion 1.

Tablas creadas en esta iteracion (Iteration 1 Implementation Proposal v0.2,
seccion 9, mas la correccion CODE-CR-01):
    - object_version
    - approved_state_pointer
    - approved_state_commit
    - idempotency_operation (nueva - aisla la reserva de concurrencia de
      idempotency_key del invariante de ApprovedStateCommit, ver
      CODE-CR-01 y el docstring de IdempotencyOperation abajo)
    - audit_event (alcance minimo: un evento por commit exitoso, CR-17)

No se crean en esta iteracion: routing_contract, agent_run,
agent_run_attempt, human_approval_wait_state. Llegan en iteraciones
posteriores sobre esta misma fundacion.

Invariantes de integridad relacional (CR-19, CR-20):
    - object_version tiene UNIQUE(object_id, object_version_id) para que
      pueda ser el destino de foreign keys compuestas.
    - approved_state_pointer.(object_id, current_approved_version_id)
      referencia object_version.(object_id, object_version_id) via FK
      compuesta -> la base de datos rechaza que el puntero de un
      object_id apunte a una version de otro object_id.
    - approved_state_commit.(object_id, object_version_id) referencia la
      misma UNIQUE compuesta -> la base de datos rechaza un commit cuyo
      object_version pertenezca a un object_id distinto del declarado.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class ObjectVersion(Base):
    __tablename__ = "object_version"

    object_version_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    object_id: Mapped[str] = mapped_column(String, nullable=False)
    version_number: Mapped[int] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    supersedes_version_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("object_version.object_version_id"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    __table_args__ = (
        # Requerido para que approved_state_pointer y approved_state_commit
        # puedan referenciar (object_id, object_version_id) como par y la
        # base de datos garantice CR-19/CR-20 a nivel de constraint.
        UniqueConstraint(
            "object_id", "object_version_id", name="uq_object_version_object_id_version_id"
        ),
    )


class ApprovedStatePointer(Base):
    __tablename__ = "approved_state_pointer"

    object_id: Mapped[str] = mapped_column(String, primary_key=True)
    current_approved_version_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        # CR-19: el puntero solo puede apuntar a una version que pertenezca
        # al mismo object_id. Nullable -> soporta la primera aprobacion
        # (CR-14): un FK compuesto con una columna NULL no se evalua.
        ForeignKeyConstraint(
            ["object_id", "current_approved_version_id"],
            ["object_version.object_id", "object_version.object_version_id"],
            name="fk_approved_state_pointer_object_version",
        ),
    )


class ApprovedStateCommit(Base):
    __tablename__ = "approved_state_commit"

    commit_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    object_id: Mapped[str] = mapped_column(String, nullable=False)
    object_version_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    expected_previous_approved_version_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )
    actual_previous_approved_version_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_payload_hash: Mapped[str] = mapped_column(String, nullable=False)
    # Correccion CODE-CR-01: esta fila SOLO se inserta una vez que la
    # mutacion del puntero ya tuvo exito, dentro de la misma transaccion
    # atomica que tambien inserta el AuditEvent. Nunca se inserta un
    # placeholder 'PENDING' aqui, y nunca se hace COMMIT de esta tabla
    # por separado del resto de la operacion. Por eso el unico valor
    # posible de 'outcome' es 'COMMITTED' - no existe ningun otro estado
    # transitorio que pueda sobrevivir a un fallo o a un STATE_CONFLICT.
    outcome: Mapped[str] = mapped_column(String, nullable=False, default="COMMITTED")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_approved_state_commit_idempotency_key"),
        ForeignKeyConstraint(
            ["object_id", "object_version_id"],
            ["object_version.object_id", "object_version.object_version_id"],
            name="fk_approved_state_commit_object_version",
        ),
        CheckConstraint(
            "outcome = 'COMMITTED'",
            name="ck_approved_state_commit_outcome_committed_only",
        ),
    )


class IdempotencyOperation(Base):
    """
    Mecanismo de reserva/concurrencia para idempotency_key, aislado de
    ApprovedStateCommit (correccion CODE-CR-01).

    Esta tabla resuelve la carrera de concurrencia sobre una
    idempotency_key nunca antes vista: el INSERT ... ON CONFLICT DO
    NOTHING sobre esta tabla es el UNICO barrero de concurrencia real
    (constraint UNIQUE de base de datos), y ocurre como el PRIMER
    statement de la misma transaccion atomica que despues valida, muta
    el puntero, inserta el ApprovedStateCommit final e inserta el
    AuditEvent. Si cualquier paso posterior falla (STATE_CONFLICT,
    version invalida, error tecnico), toda la transaccion se revierte
    (ROLLBACK ALL) - incluida esta fila de reserva, que jamas persiste
    aislada de una operacion exitosa.

    'status' solo puede tomar el valor transitorio 'CLAIMED' (mientras
    la transaccion sigue abierta, invisible para cualquier otra
    transaccion por aislamiento MVCC) o el valor final 'COMMITTED'
    (escrito antes del COMMIT). Ningun commit de base de datos deja
    jamas una fila en estado 'CLAIMED' - se prueba explicitamente en
    tests/integration/test_approved_state_commit.py.
    """

    __tablename__ = "idempotency_operation"

    idempotency_key: Mapped[str] = mapped_column(String, primary_key=True)
    idempotency_payload_hash: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="CLAIMED")
    commit_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("approved_state_commit.commit_id"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('CLAIMED', 'COMMITTED')",
            name="ck_idempotency_operation_status",
        ),
    )


class AuditEvent(Base):
    """
    Alcance minimo de auditoria para Iteracion 1 (CR-17).

    No es el modelo completo de Audit Trail de Fase 3 (correlation_id,
    causation_id, actor_type, run_id, etc.) - esos campos se agregan
    cuando exista un Agent Run real que los produzca. Esta tabla
    solo preserva el invariante ya aprobado: ningun commit exitoso
    queda sin su evento de auditoria correspondiente, en la misma
    transaccion atomica.
    """

    __tablename__ = "audit_event"

    event_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    object_id: Mapped[str] = mapped_column(String, nullable=False)
    object_version_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )
    commit_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("approved_state_commit.commit_id"),
        nullable=True,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
