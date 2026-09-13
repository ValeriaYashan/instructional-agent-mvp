"""
Modelo fisico. Iteracion 1 + Iteracion 2 + Iteracion 3.

Tablas de Iteracion 1 (sin cambios de proposito):
    - object_version
    - approved_state_pointer
    - approved_state_commit
    - audit_event

Tablas de Iteracion 2:
    - workflow_transition

Tablas nuevas de Iteracion 3 (Iteration 3 Proposal v0.3 FINAL BASELINE
CANDIDATE, CR-33 a CR-38):
    - instructional_object (identidad ESTABLE del objeto instruccional,
      separada de object_version - CR-33)
    - object_relation (relaciones requeridas entre objetos, Paso 3 del
      Context Engine)
    - evidence_source (metadatos/referencia de evidencia E0-E3, Paso 7,
      adaptador NO semantico - CR-34)
    - context_package (salida inmutable del Paso 9)

Generalizada en Iteracion 2 (CR-29) y ampliada en Iteracion 3 (CR-35,
mismo patron de migracion compatible hacia atras):
    - idempotency_operation: operation_type pasa de 2 a 3 valores
      ('APPROVED_STATE_COMMIT', 'WORKFLOW_TRANSITION',
      'CONTEXT_ASSEMBLY'). El comportamiento de los dos primeros no
      cambia.

No se crean en esta iteracion: routing_contract, agent_run,
agent_run_attempt, human_approval_wait_state. Llegan en iteraciones
posteriores.

Invariantes de integridad relacional heredadas de Iteracion 1 (CR-19,
CR-20) sin cambios: ver historial de este archivo en iteraciones
anteriores.

CR-33 (Iteracion 3): object_type es invariante de object_id, no de
object_version_id. Se introduce instructional_object como identidad
estable; object_version.object_id pasa a ser FK hacia
instructional_object.object_id, SIN modificar el significado ya
aprobado de object_version (object_version_id sigue siendo su PK,
version_number/status/supersedes_version_id sin cambios, y las FKs
compuestas de CR-19/CR-20 siguen funcionando identico).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# Vocabulario completo del Instructional Object Lifecycle (§6.10.3,
# Agent Specification v1.1). No se trunca ni se renombra (CR-21).
KNOWN_STATES: tuple[str, ...] = (
    "NOT_ELIGIBLE",
    "ELIGIBLE_PENDING",
    "DRAFT",
    "QA_IN_PROGRESS",
    "REVISION_REQUIRED",
    "QA_PASS_PROVISIONAL",
    "DEPENDENCY_RESOLUTION",
    "DEPENDENCY_BLOCKED",
    "ROUTING_RESOLUTION",
    "READY_FOR_HUMAN_APPROVAL",
    "APPROVED_CONTENT",
    "SUPERSEDED",
)

_known_states_sql_list = ", ".join(f"'{s}'" for s in KNOWN_STATES)

# Clases de evidencia del contrato de Gate 7 / §6.6 Paso 7.
EVIDENCE_CLASSES: tuple[str, ...] = ("E0", "E1", "E2", "E3")
_evidence_classes_sql_list = ", ".join(f"'{c}'" for c in EVIDENCE_CLASSES)


class InstructionalObject(Base):
    """
    Identidad ESTABLE del objeto instruccional (CR-33, Iteracion 3).

    object_type es invariante de esta identidad, no de una version
    particular - un objeto no cambia de tipo entre v1 y v2. Filas
    preexistentes de Iteracion 1/2 (creadas antes de que existiera esta
    tabla) se retropueblan con object_type = NULL (desconocido/legacy)
    UNICAMENTE durante la migracion 0003 - ese backfill es un evento de
    migracion unico, no un mecanismo permanente.

    CODE-CR-42 (Human Code Review): NO existe ningun trigger ni
    mecanismo de auto-provisionamiento permanente que cree filas de
    esta tabla implicitamente. Despues de la migracion 0003, todo
    object_id NUEVO debe crear su InstructionalObject explicitamente
    ANTES de que exista cualquier ObjectVersion que lo referencie - la
    FK de object_version.object_id lo exige a nivel de base de datos, y
    ningun trigger la elude. El orden correcto es siempre:
    InstructionalObject primero, ObjectVersion despues.
    """

    __tablename__ = "instructional_object"

    object_id: Mapped[str] = mapped_column(String, primary_key=True)
    object_type: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )


class ObjectVersion(Base):
    __tablename__ = "object_version"

    object_version_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    object_id: Mapped[str] = mapped_column(
        String, ForeignKey("instructional_object.object_id"), nullable=False
    )
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
        UniqueConstraint(
            "object_id", "object_version_id", name="uq_object_version_object_id_version_id"
        ),
        CheckConstraint(
            f"status IN ({_known_states_sql_list})",
            name="ck_object_version_status_known_states",
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


class WorkflowTransition(Base):
    __tablename__ = "workflow_transition"

    transition_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    object_version_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("object_version.object_version_id"),
        nullable=False,
    )
    from_state: Mapped[str] = mapped_column(String, nullable=False)
    to_state: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_payload_hash: Mapped[str] = mapped_column(String, nullable=False)
    requested_by: Mapped[str] = mapped_column(String, nullable=False)
    correlation_id: Mapped[str] = mapped_column(String, nullable=False)
    causation_id: Mapped[str | None] = mapped_column(String, nullable=True)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            f"from_state IN ({_known_states_sql_list})",
            name="ck_workflow_transition_from_state_known",
        ),
        CheckConstraint(
            f"to_state IN ({_known_states_sql_list})",
            name="ck_workflow_transition_to_state_known",
        ),
    )


class ObjectRelation(Base):
    """
    Relacion requerida entre dos Instructional Objects (Paso 3 del
    Context Engine, §6.6). Ej.: LearningObject -> PARENT_ACTIVITY ->
    Activity; Activity -> LEARNING_OUTCOME -> LearningOutcome.

    Cardinalidad (correccion secundaria del Human Code Review de
    Iteracion 3): cada relation_type es SINGULAR por from_object_id en
    el alcance de esta iteracion (un objeto tiene una unica
    PARENT_ACTIVITY, un unico LEARNING_OUTCOME) - forzado con
    UNIQUE(from_object_id, relation_type), mas estricto que la version
    anterior (que solo evitaba duplicados exactos de la tripleta). Esto
    es lo que hace seguro usar scalar_one_or_none() en
    PostgresDependencyResolver.
    """

    __tablename__ = "object_relation"

    relation_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    from_object_id: Mapped[str] = mapped_column(
        String, ForeignKey("instructional_object.object_id"), nullable=False
    )
    to_object_id: Mapped[str] = mapped_column(
        String, ForeignKey("instructional_object.object_id"), nullable=False
    )
    relation_type: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "from_object_id",
            "relation_type",
            name="uq_object_relation_from_object_relation_type_singular",
        ),
    )


class EvidenceSource(Base):
    """
    Metadatos/referencia de una fuente de evidencia (Paso 7 del Context
    Engine, §6.6). Adaptador de Iteracion 3: busqueda exacta por
    metadatos, NO retrieval semantico (CR-34) - pgvector queda diferido.
    """

    __tablename__ = "evidence_source"

    source_id: Mapped[str] = mapped_column(String, primary_key=True)
    object_id: Mapped[str] = mapped_column(
        String, ForeignKey("instructional_object.object_id"), nullable=False
    )
    evidence_class: Mapped[str] = mapped_column(String, nullable=False)
    content_reference: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            f"evidence_class IN ({_evidence_classes_sql_list})",
            name="ck_evidence_source_evidence_class_known",
        ),
    )


class ContextPackage(Base):
    """
    Salida inmutable del Paso 9 del Context Engine (§6.6). Nunca se
    actualiza tras su insercion - un nuevo ensamblado siempre produce
    una fila nueva con un context_package_id nuevo (CR-35: la
    idempotencia de la SOLICITUD puede hacer replay del mismo
    context_package_id ya existente, pero nunca se muta uno existente).

    context_contract_version (CR-38) es parte de la identidad
    SEMANTICA del contenido (participa del hash canonico);
    context_package_id y assembled_at son identidad/provenance de
    EJECUCION (nunca participan del hash canonico) - ver
    src/persistence/repositories/context_engine.py para el calculo
    exacto del hash.
    """

    __tablename__ = "context_package"

    context_package_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    context_package_hash: Mapped[str] = mapped_column(String, nullable=False)
    context_contract_version: Mapped[str] = mapped_column(String, nullable=False)
    task_type: Mapped[str] = mapped_column(String, nullable=False)
    target_object_id: Mapped[str] = mapped_column(
        String, ForeignKey("instructional_object.object_id"), nullable=False
    )
    pinned_version_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("object_version.object_version_id"),
        nullable=True,
    )
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    # Estructura fija del Paso 9 (§6.6) - JSONB, cada arreglo persistido
    # ya en el orden determinístico exigido por CR-36/CR-38 (por clave
    # natural), no en el orden de construccion del codigo.
    approved_objects: Mapped[list] = mapped_column(JSONB, nullable=False)
    relations: Mapped[list] = mapped_column(JSONB, nullable=False)
    evidence: Mapped[list] = mapped_column(JSONB, nullable=False)
    dependency_status: Mapped[list] = mapped_column(JSONB, nullable=False)
    gaps: Mapped[list] = mapped_column(JSONB, nullable=False)
    run_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False)
    assembled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )


class IdempotencyOperation(Base):
    """
    Mecanismo de reserva/concurrencia compartido (CODE-CR-01 ->
    generalizado en Iteracion 2, CR-25 -> ampliado en Iteracion 3,
    CR-35).

    operation_type ahora admite tres valores: 'APPROVED_STATE_COMMIT',
    'WORKFLOW_TRANSITION', 'CONTEXT_ASSEMBLY'. El comportamiento de los
    dos primeros NO cambia respecto a Iteracion 2.

    CONTEXT_ASSEMBLY (Iteracion 3, CR-35): request idempotency != context
    freshness. Un replay de la misma idempotency_key + mismo payload
    semantico de solicitud SIEMPRE devuelve el mismo context_package_id
    ya ensamblado, sin importar si el estado aprobado subyacente cambio
    despues - la idempotencia protege la solicitud, no garantiza
    "frescura". Un resultado rechazado (MAPPING_UNDEFINED,
    NEWER_VERSION_AVAILABLE, INVALID_OBJECT_VERSION - los fallos del
    contrato de 10 pasos que impiden la creacion de un context_package,
    ver context_engine.py) persiste via COMMIT explicito, igual que
    WORKFLOW_TRANSITION, para permitir replay identico. TASK_TYPE_UNRESOLVED
    es la excepcion: Task Intake lo rechaza ANTES de cualquier acceso a
    base de datos - nunca genera una fila en esta tabla, a diferencia de
    los tres anteriores (correccion de documentacion, Human Code
    Review de Iteracion 3; no cambia comportamiento en tiempo de
    ejecucion, que ya era este desde el build v1).
    """

    __tablename__ = "idempotency_operation"

    operation_type: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_payload_hash: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="CLAIMED")
    commit_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("approved_state_commit.commit_id"),
        nullable=True,
    )
    transition_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workflow_transition.transition_id"),
        nullable=True,
    )
    context_package_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("context_package.context_package_id"),
        nullable=True,
    )
    rejected_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    __table_args__ = (
        PrimaryKeyConstraint("operation_type", "idempotency_key", name="idempotency_operation_pkey"),
        CheckConstraint(
            "operation_type IN ('APPROVED_STATE_COMMIT', 'WORKFLOW_TRANSITION', "
            "'CONTEXT_ASSEMBLY')",
            name="ck_idempotency_operation_operation_type",
        ),
        CheckConstraint(
            "status IN ('CLAIMED', 'COMMITTED', 'REJECTED')",
            name="ck_idempotency_operation_status",
        ),
        CheckConstraint(
            "rejected_reason IS NULL OR rejected_reason IN "
            "('UNKNOWN_STATE', 'TRANSITION_NOT_ENABLED_THIS_ITERATION', "
            "'STATE_CONFLICT', 'INVALID_OBJECT_VERSION', "
            "'TASK_TYPE_UNRESOLVED', 'MAPPING_UNDEFINED', 'NEWER_VERSION_AVAILABLE')",
            name="ck_idempotency_operation_rejected_reason_canonical",
        ),
        # CR-30 (Iteracion 2) ampliado con la rama CONTEXT_ASSEMBLY
        # (Iteracion 3, CR-35): cada operation_type solo puede resolver
        # en su propia referencia de dominio (commit_id / transition_id /
        # context_package_id), nunca en mas de una a la vez, y
        # APPROVED_STATE_COMMIT+REJECTED sigue siendo estructuralmente
        # imposible.
        CheckConstraint(
            "(status = 'CLAIMED' AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND rejected_reason IS NULL) "
            "OR (operation_type = 'APPROVED_STATE_COMMIT' AND status = 'COMMITTED' "
            "AND commit_id IS NOT NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND rejected_reason IS NULL) "
            "OR (operation_type = 'WORKFLOW_TRANSITION' AND status = 'COMMITTED' "
            "AND commit_id IS NULL AND transition_id IS NOT NULL "
            "AND context_package_id IS NULL AND rejected_reason IS NULL) "
            "OR (operation_type = 'WORKFLOW_TRANSITION' AND status = 'REJECTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND rejected_reason IS NOT NULL) "
            "OR (operation_type = 'CONTEXT_ASSEMBLY' AND status = 'COMMITTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NOT NULL AND rejected_reason IS NULL) "
            "OR (operation_type = 'CONTEXT_ASSEMBLY' AND status = 'REJECTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND rejected_reason IS NOT NULL)",
            name="ck_idempotency_operation_row_invariants",
        ),
    )


class AuditEvent(Base):
    """
    Alcance minimo de auditoria para Iteracion 1 (CR-17), reutilizado en
    Iteracion 3 para el evento de ensamblado de Context Package
    (event_type='CONTEXT_PACKAGE_ASSEMBLED').
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
    context_package_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("context_package.context_package_id"),
        nullable=True,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
