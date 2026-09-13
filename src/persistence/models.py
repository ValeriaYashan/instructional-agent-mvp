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

Tablas nuevas de Iteracion 4 (Proposal v0.5 HUMAN_APPROVED, CR-47 a
CR-59):
    - agent_run (unidad logica de trabajo, con lease + fencing por
      generacion)
    - agent_run_attempt (intento tecnico, a lo sumo uno exitoso por
      logical_run_id, DB-enforced)
    - object_version_content (payload generado, 1:0..1 con
      object_version - CR-48, no ArtifactVersion separado)

Generalizada en Iteracion 2 (CR-29), ampliada en Iteracion 3 (CR-35) y
en Iteracion 4 (CR-51/CR-54, mismo patron de migracion compatible hacia
atras):
    - idempotency_operation: operation_type pasa de 3 a 4 valores,
      agrega 'PRODUCER_EXECUTION' (resuelve hacia agent_run_id, nunca
      hacia ObjectVersionContent directamente - CR-54). El
      comportamiento de los tres anteriores no cambia.

CR-57 (Iteracion 4): UNIQUE(object_id, version_number) agregada sobre
object_version como extension aditiva - OBJECT_VERSION_NUMBER_UNIQUE_PER_OBJECT.
No modifica el significado ya aprobado de object_version.

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
    Index,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    text,
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
        # CR-57 (Iteracion 4): extension aditiva - OBJECT_VERSION_NUMBER_UNIQUE_PER_OBJECT.
        # No modifica el significado ya aprobado de object_version (CR-19/CR-20/CR-21
        # intactos); refuerza una invariante natural que ya se cumplia de hecho.
        UniqueConstraint(
            "object_id", "version_number", name="uq_object_version_object_id_version_number"
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
    los tres anteriores.

    PRODUCER_EXECUTION (Iteracion 4, Proposal v0.5, CR-51/CR-54): resuelve
    rapido y de forma durable hacia agent_run_id (nunca hacia
    ObjectVersionContent directamente) - la reserva de idempotencia
    identifica QUE logical_run_id corresponde a una solicitud, no si esa
    ejecucion tuvo exito. Por eso CLAIMED nunca sobrevive fuera de la
    transaccion corta de Fase A (CR-51): a diferencia de
    WORKFLOW_TRANSITION/CONTEXT_ASSEMBLY, aqui NO existe una rama
    PRODUCER_EXECUTION+REJECTED para fallos de ejecucion del Producer
    (TECHNICAL_FAILURE/INVALID_OUTPUT/CLAIM_LOST/MAX_ATTEMPTS_EXCEEDED
    viven enteramente en agent_run/agent_run_attempt, nunca aqui) - la
    unica rama REJECTED de PRODUCER_EXECUTION es INVALID_CONTEXT_PACKAGE,
    detectado ANTES de crear el AgentRun (mismo patron que
    INVALID_OBJECT_VERSION/INVALID_CONTEXT_PACKAGE de las iteraciones
    anteriores).
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
    agent_run_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("agent_run.logical_run_id"),
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
            "'CONTEXT_ASSEMBLY', 'PRODUCER_EXECUTION')",
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
            "'TASK_TYPE_UNRESOLVED', 'MAPPING_UNDEFINED', 'NEWER_VERSION_AVAILABLE', "
            "'INVALID_CONTEXT_PACKAGE')",
            name="ck_idempotency_operation_rejected_reason_canonical",
        ),
        # CR-30 (I2) + CR-35 (I3) + Proposal v0.5 CR-54 (I4): cada
        # operation_type solo puede resolver en su propia referencia de
        # dominio, nunca en mas de una a la vez. Las seis ramas
        # preexistentes quedan TEXTUALMENTE SIN CAMBIO; se agregan dos
        # ramas nuevas para PRODUCER_EXECUTION.
        CheckConstraint(
            "(status = 'CLAIMED' AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND agent_run_id IS NULL "
            "AND rejected_reason IS NULL) "
            "OR (operation_type = 'APPROVED_STATE_COMMIT' AND status = 'COMMITTED' "
            "AND commit_id IS NOT NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND agent_run_id IS NULL "
            "AND rejected_reason IS NULL) "
            "OR (operation_type = 'WORKFLOW_TRANSITION' AND status = 'COMMITTED' "
            "AND commit_id IS NULL AND transition_id IS NOT NULL "
            "AND context_package_id IS NULL AND agent_run_id IS NULL "
            "AND rejected_reason IS NULL) "
            "OR (operation_type = 'WORKFLOW_TRANSITION' AND status = 'REJECTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND agent_run_id IS NULL "
            "AND rejected_reason IS NOT NULL) "
            "OR (operation_type = 'CONTEXT_ASSEMBLY' AND status = 'COMMITTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NOT NULL AND agent_run_id IS NULL "
            "AND rejected_reason IS NULL) "
            "OR (operation_type = 'CONTEXT_ASSEMBLY' AND status = 'REJECTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND agent_run_id IS NULL "
            "AND rejected_reason IS NOT NULL) "
            "OR (operation_type = 'PRODUCER_EXECUTION' AND status = 'COMMITTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND agent_run_id IS NOT NULL "
            "AND rejected_reason IS NULL) "
            "OR (operation_type = 'PRODUCER_EXECUTION' AND status = 'REJECTED' "
            "AND commit_id IS NULL AND transition_id IS NULL "
            "AND context_package_id IS NULL AND agent_run_id IS NULL "
            "AND rejected_reason IS NOT NULL)",
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
    agent_run_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("agent_run.logical_run_id"),
        nullable=True,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


# ---------------------------------------------------------------------------
# Iteracion 4 (Proposal v0.5, HUMAN_APPROVED) - Producer Execution & Agent Run
# Lifecycle. CR-47 a CR-59.
# ---------------------------------------------------------------------------


class AgentRun(Base):
    """
    Unidad logica de trabajo (Proposal v0.5 S8). role='PRODUCER'
    unicamente en esta iteracion - 'QA' queda reservado para Iteracion 5
    sin requerir cambio de schema futuro (producer_logical_run_id ya
    presente, NULL en I4).

    lease_generation es el TOKEN DE FENCING (CR-56/CR-59): cambia
    UNICAMENTE en adquisicion inicial o en reclamacion tras vencimiento
    del lease - NUNCA en un reintento tecnico bajo el mismo dueno
    (CR-58). La autorizacion de finalizacion (exitosa o por
    max_attempts) exige ATOMICAMENTE claimed_by + lease_generation +
    status='IN_PROGRESS' + lease_expires_at > clock_timestamp()
    (LEASE_IS_VALID, CR-59, corregido por CODE-CR-69 - clock_timestamp()
    de PostgreSQL, nunca now()/CURRENT_TIMESTAMP ni el reloj de la
    aplicacion; ver agent_run_manager.py para el fencing adicional
    contra espera de row lock) en el mismo UPDATE que otorga esa
    autoridad - nunca en una lectura previa desacoplada de la escritura.
    """

    __tablename__ = "agent_run"

    logical_run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    role: Mapped[str] = mapped_column(String, nullable=False)
    producer_logical_run_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("agent_run.logical_run_id"),
        nullable=True,
    )
    task_type: Mapped[str] = mapped_column(String, nullable=False)
    target_object_version_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("object_version.object_version_id"),
        nullable=False,
    )
    context_package_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("context_package.context_package_id"),
        nullable=False,
    )
    requested_by: Mapped[str] = mapped_column(String, nullable=False)
    correlation_id: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING")
    claimed_by: Mapped[str | None] = mapped_column(String, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    lease_generation: Mapped[int] = mapped_column(nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        CheckConstraint("role IN ('PRODUCER')", name="ck_agent_run_role_known"),
        CheckConstraint(
            "status IN ('PENDING', 'IN_PROGRESS', 'SUCCEEDED', 'FAILED')",
            name="ck_agent_run_status_known",
        ),
        CheckConstraint(
            "role != 'PRODUCER' OR producer_logical_run_id IS NULL",
            name="ck_agent_run_producer_has_no_producer_ref",
        ),
        # CR-56: a lo sumo un AgentRun ACTIVO por ObjectVersion - indice
        # unico PARCIAL (solo entre status PENDING/IN_PROGRESS), no una
        # UniqueConstraint plana (esa impediria para siempre un segundo
        # AgentRun sobre la misma version, incluso tras SUCCEEDED/FAILED).
        Index(
            "uq_agent_run_target_version_active",
            "target_object_version_id",
            unique=True,
            postgresql_where=text("status IN ('PENDING', 'IN_PROGRESS')"),
        ),
    )


class AgentRunAttempt(Base):
    """
    Intento tecnico dentro de un AgentRun (Proposal v0.5 S8). A lo sumo
    un attempt por logical_run_id puede tener outcome='SUCCESS'
    (CR-47, UNIQUE parcial). lease_generation_at_creation es el token de
    fencing capturado en el momento de creacion de este intento (CR-56) -
    se compara contra agent_run.lease_generation vigente en el momento
    de finalizacion (S8.7/S8.5), nunca inferido de otra forma.
    """

    __tablename__ = "agent_run_attempt"

    attempt_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    logical_run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("agent_run.logical_run_id"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(nullable=False)
    lease_generation_at_creation: Mapped[int] = mapped_column(nullable=False)
    outcome: Mapped[str | None] = mapped_column(String, nullable=True)
    failure_detail: Mapped[str | None] = mapped_column(String, nullable=True)
    provider: Mapped[str | None] = mapped_column(String, nullable=True)
    model_identifier: Mapped[str | None] = mapped_column(String, nullable=True)
    instructions_version: Mapped[str | None] = mapped_column(String, nullable=True)
    output_schema_version: Mapped[str | None] = mapped_column(String, nullable=True)
    output_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    token_usage: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    cost_estimate: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "outcome IS NULL OR outcome IN "
            "('SUCCESS', 'TECHNICAL_FAILURE', 'INVALID_OUTPUT', 'CLAIM_LOST')",
            name="ck_agent_run_attempt_outcome_known",
        ),
        UniqueConstraint(
            "logical_run_id", "attempt_number", name="uq_agent_run_attempt_logical_run_number"
        ),
        # CR-47: a lo sumo un intento exitoso por logical_run_id - indice
        # unico PARCIAL (WHERE outcome='SUCCESS'), forzado por PostgreSQL,
        # no solo por disciplina de aplicacion.
        Index(
            "uq_agent_run_attempt_logical_run_success",
            "logical_run_id",
            unique=True,
            postgresql_where=text("outcome = 'SUCCESS'"),
        ),
    )


class ObjectVersionContent(Base):
    """
    Payload de contenido generado, 1:0..1 con ObjectVersion (CR-48:
    ArtifactVersion separado RECHAZADO - el contenido es el payload de
    la version existente, no una segunda dimension de versionado). Solo
    existe una vez que Structural Validation (CR-53) tuvo exito - nunca
    para un intento INVALID_OUTPUT/TECHNICAL_FAILURE.
    """

    __tablename__ = "object_version_content"

    object_version_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("object_version.object_version_id"),
        primary_key=True,
    )
    schema_version: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    body: Mapped[str] = mapped_column(String, nullable=False)
    learning_outcome_refs: Mapped[list] = mapped_column(JSONB, nullable=False)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False)
    produced_by_attempt_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("agent_run_attempt.attempt_id"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
