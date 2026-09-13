"""Iteracion 4: agent_run, agent_run_attempt, object_version_content + generalizacion de idempotency_operation + UNIQUE(object_id, version_number)

Revision ID: 0004_producer_execution_agent_run
Revises: 0003_context_engine
Create Date: 2026-09-13

Proposal v0.5 HUMAN_APPROVED (CR-47 a CR-59). Compatible hacia atras:
opera sobre una base ya poblada por Iteraciones 1-3. 0001, 0002, 0003
permanecen inmutables.

Secuencia:
  1. CREATE TABLE agent_run (con indice unico parcial de version activa)
  2. CREATE TABLE agent_run_attempt (con indice unico parcial de exito unico)
  3. CREATE TABLE object_version_content
  4. object_version: ADD UNIQUE(object_id, version_number) - CR-57,
     extension aditiva, no reinterpreta el significado ya aprobado de
     object_version. Los datos existentes de I1-I3 ya la satisfacen
     (cada fixture de test asigna version_number unicos por object_id),
     verificado explicitamente antes de agregar la constraint.
  5. idempotency_operation:
     a. DROP/ADD CHECK operation_type (3 -> 4 valores, agrega
        'PRODUCER_EXECUTION')
     b. DROP/ADD CHECK rejected_reason canonico (agrega
        'INVALID_CONTEXT_PACKAGE')
     c. ADD COLUMN agent_run_id (nullable, FK -> agent_run)
     d. DROP/ADD CHECK de invariantes de fila (6 -> 8 ramas; las seis
        existentes quedan TEXTUALMENTE SIN CAMBIO)
  6. audit_event: ADD COLUMN agent_run_id (nullable, FK -> agent_run)
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_producer_agent_run"
down_revision: Union[str, None] = "0003_context_engine"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# CODE-CR-70 (Human Code Review de Build v5, resuelto y verificado
# contra PostgreSQL 16 real): alembic_version.version_num es
# VARCHAR(32). El identificador de revision original de este archivo,
# "0004_producer_execution_agent_run" (33 caracteres), excede ese
# limite y `alembic upgrade` fallaba al intentar escribirlo. Se acorto
# a "0004_producer_agent_run" (23 caracteres) - el NOMBRE DE ARCHIVO
# permanece sin cambios (0004_producer_execution_agent_run.py) para no
# alterar el orden lexicografico/historial de archivos de migracion;
# unicamente el string `revision` interno (lo que efectivamente se
# persiste en alembic_version) cambio. No hay ninguna migracion 0005
# todavia que dependa del valor viejo via down_revision.


def upgrade() -> None:
    # 1. agent_run.
    op.create_table(
        "agent_run",
        sa.Column("logical_run_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column(
            "producer_logical_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_run.logical_run_id"),
            nullable=True,
        ),
        sa.Column("task_type", sa.String(), nullable=False),
        sa.Column(
            "target_object_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("object_version.object_version_id"),
            nullable=False,
        ),
        sa.Column(
            "context_package_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("context_package.context_package_id"),
            nullable=False,
        ),
        sa.Column("requested_by", sa.String(), nullable=False),
        sa.Column("correlation_id", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="PENDING"),
        sa.Column("claimed_by", sa.String(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.CheckConstraint("role IN ('PRODUCER')", name="ck_agent_run_role_known"),
        sa.CheckConstraint(
            "status IN ('PENDING', 'IN_PROGRESS', 'SUCCEEDED', 'FAILED')",
            name="ck_agent_run_status_known",
        ),
        sa.CheckConstraint(
            "role != 'PRODUCER' OR producer_logical_run_id IS NULL",
            name="ck_agent_run_producer_has_no_producer_ref",
        ),
    )
    # CR-56: indice unico PARCIAL - a lo sumo un AgentRun activo por
    # ObjectVersion. No una UniqueConstraint plana (esa impediria para
    # siempre un segundo AgentRun sobre la misma version tras
    # SUCCEEDED/FAILED).
    op.execute(
        "CREATE UNIQUE INDEX uq_agent_run_target_version_active "
        "ON agent_run (target_object_version_id) "
        "WHERE status IN ('PENDING', 'IN_PROGRESS')"
    )

    # 2. agent_run_attempt.
    op.create_table(
        "agent_run_attempt",
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "logical_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_run.logical_run_id"),
            nullable=False,
        ),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("lease_generation_at_creation", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(), nullable=True),
        sa.Column("failure_detail", sa.String(), nullable=True),
        sa.Column("provider", sa.String(), nullable=True),
        sa.Column("model_identifier", sa.String(), nullable=True),
        sa.Column("instructions_version", sa.String(), nullable=True),
        sa.Column("output_schema_version", sa.String(), nullable=True),
        sa.Column("output_payload", postgresql.JSONB(), nullable=True),
        sa.Column("token_usage", postgresql.JSONB(), nullable=True),
        sa.Column("cost_estimate", postgresql.JSONB(), nullable=True),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN "
            "('SUCCESS', 'TECHNICAL_FAILURE', 'INVALID_OUTPUT', 'CLAIM_LOST')",
            name="ck_agent_run_attempt_outcome_known",
        ),
        sa.UniqueConstraint(
            "logical_run_id", "attempt_number", name="uq_agent_run_attempt_logical_run_number"
        ),
    )
    # CR-47: indice unico PARCIAL - a lo sumo un intento exitoso por
    # logical_run_id, forzado por PostgreSQL.
    op.execute(
        "CREATE UNIQUE INDEX uq_agent_run_attempt_logical_run_success "
        "ON agent_run_attempt (logical_run_id) WHERE outcome = 'SUCCESS'"
    )

    # 3. object_version_content.
    op.create_table(
        "object_version_content",
        sa.Column(
            "object_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("object_version.object_version_id"),
            primary_key=True,
        ),
        sa.Column("schema_version", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("body", sa.String(), nullable=False),
        sa.Column("learning_outcome_refs", postgresql.JSONB(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column(
            "produced_by_attempt_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_run_attempt.attempt_id"),
            nullable=False,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
    )

    # 4. CR-57: UNIQUE(object_id, version_number) - extension aditiva.
    connection = op.get_bind()
    duplicate_count = connection.execute(
        sa.text(
            "SELECT COUNT(*) FROM ("
            "  SELECT object_id, version_number FROM object_version "
            "  GROUP BY object_id, version_number HAVING COUNT(*) > 1"
            ") dupes"
        )
    ).scalar_one()
    if duplicate_count != 0:
        raise RuntimeError(
            "Migracion 0004 abortada: existen "
            f"{duplicate_count} pares (object_id, version_number) duplicados "
            "en object_version - no se puede agregar la UNIQUE constraint "
            "de CR-57 sin resolverlos manualmente primero."
        )
    op.create_unique_constraint(
        "uq_object_version_object_id_version_number",
        "object_version",
        ["object_id", "version_number"],
    )

    # 5. idempotency_operation.
    op.drop_constraint(
        "ck_idempotency_operation_operation_type", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_operation_type",
        "idempotency_operation",
        "operation_type IN ('APPROVED_STATE_COMMIT', 'WORKFLOW_TRANSITION', "
        "'CONTEXT_ASSEMBLY', 'PRODUCER_EXECUTION')",
    )

    op.drop_constraint(
        "ck_idempotency_operation_rejected_reason_canonical",
        "idempotency_operation",
        type_="check",
    )
    op.create_check_constraint(
        "ck_idempotency_operation_rejected_reason_canonical",
        "idempotency_operation",
        "rejected_reason IS NULL OR rejected_reason IN "
        "('UNKNOWN_STATE', 'TRANSITION_NOT_ENABLED_THIS_ITERATION', "
        "'STATE_CONFLICT', 'INVALID_OBJECT_VERSION', "
        "'TASK_TYPE_UNRESOLVED', 'MAPPING_UNDEFINED', 'NEWER_VERSION_AVAILABLE', "
        "'INVALID_CONTEXT_PACKAGE')",
    )

    op.add_column(
        "idempotency_operation",
        sa.Column(
            "agent_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_run.logical_run_id"),
            nullable=True,
        ),
    )

    op.drop_constraint(
        "ck_idempotency_operation_row_invariants", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_row_invariants",
        "idempotency_operation",
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
    )

    # 6. audit_event: referencia opcional a agent_run.
    op.add_column(
        "audit_event",
        sa.Column(
            "agent_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_run.logical_run_id"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    # Guarda explicita (mismo patron de 0002/0003): abortar si existen
    # datos de Iteracion 4 antes de restrechar cualquier CHECK/constraint.
    connection = op.get_bind()

    producer_execution_count = connection.execute(
        sa.text(
            "SELECT COUNT(*) FROM idempotency_operation WHERE operation_type = 'PRODUCER_EXECUTION'"
        )
    ).scalar_one()
    agent_run_count = connection.execute(sa.text("SELECT COUNT(*) FROM agent_run")).scalar_one()
    content_count = connection.execute(
        sa.text("SELECT COUNT(*) FROM object_version_content")
    ).scalar_one()

    if producer_execution_count != 0 or agent_run_count != 0 or content_count != 0:
        raise RuntimeError(
            "Downgrade de 0004 abortado: existen "
            f"{producer_execution_count} filas de idempotency_operation con "
            f"operation_type='PRODUCER_EXECUTION', {agent_run_count} filas de "
            f"agent_run y {content_count} filas de object_version_content. "
            "Ninguna puede representarse bajo el esquema de Iteracion 3. "
            "Eliminar o migrar manualmente esos datos antes de reintentar "
            "el downgrade."
        )

    op.drop_column("audit_event", "agent_run_id")

    op.drop_constraint(
        "ck_idempotency_operation_row_invariants", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_row_invariants",
        "idempotency_operation",
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
    )

    op.drop_column("idempotency_operation", "agent_run_id")

    op.drop_constraint(
        "ck_idempotency_operation_rejected_reason_canonical",
        "idempotency_operation",
        type_="check",
    )
    op.create_check_constraint(
        "ck_idempotency_operation_rejected_reason_canonical",
        "idempotency_operation",
        "rejected_reason IS NULL OR rejected_reason IN "
        "('UNKNOWN_STATE', 'TRANSITION_NOT_ENABLED_THIS_ITERATION', "
        "'STATE_CONFLICT', 'INVALID_OBJECT_VERSION', "
        "'TASK_TYPE_UNRESOLVED', 'MAPPING_UNDEFINED', 'NEWER_VERSION_AVAILABLE')",
    )

    op.drop_constraint(
        "ck_idempotency_operation_operation_type", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_operation_type",
        "idempotency_operation",
        "operation_type IN ('APPROVED_STATE_COMMIT', 'WORKFLOW_TRANSITION', 'CONTEXT_ASSEMBLY')",
    )

    op.drop_constraint("uq_object_version_object_id_version_number", "object_version", type_="unique")

    op.drop_table("object_version_content")
    op.execute("DROP INDEX IF EXISTS uq_agent_run_attempt_logical_run_success")
    op.drop_table("agent_run_attempt")
    op.execute("DROP INDEX IF EXISTS uq_agent_run_target_version_active")
    op.drop_table("agent_run")
