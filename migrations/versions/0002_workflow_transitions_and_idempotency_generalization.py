"""Iteracion 2: workflow_transition + generalizacion de idempotency_operation

Revision ID: 0002_workflow_transitions
Revises: 0001_initial_schema
Create Date: 2026-09-12

Compatible hacia atras (CR-29): opera sobre una base de datos que ya
tiene filas de idempotency_operation pobladas por Iteracion 1
(HUMAN_APPROVED_CLOSED). No asume base vacia. 0001_initial_schema
permanece inmutable; el comportamiento de Iteracion 1 no cambia.

Secuencia (orden real de ejecucion, distinto del orden expositivo de
Iteration 2 Proposal v0.4 solo por la dependencia tecnica de que
workflow_transition debe existir antes de que idempotency_operation
pueda referenciarla via FK):

  1. CREATE TABLE workflow_transition
  2. ALTER TABLE object_version: CHECK de KNOWN_STATES (CR-21)
  3. idempotency_operation:
     a. ADD COLUMN operation_type VARCHAR NULL
     b. UPDATE ... SET operation_type = 'APPROVED_STATE_COMMIT'
        (backfill: toda fila existente de Iteracion 1 es, por
        definicion, una operacion de Approved State Commit)
     c. verificar (assert de migracion) que no queda ningun
        operation_type NULL - falla la migracion si no se cumple
     d. ALTER COLUMN operation_type SET NOT NULL
     e. ADD COLUMN transition_id UUID NULL REFERENCES workflow_transition
     f. ADD COLUMN rejected_reason VARCHAR NULL
     g. reemplazar CHECK de status (agrega 'REJECTED')
     h. migrar PK de (idempotency_key) a (operation_type, idempotency_key)
     i. CHECK de dominio cerrado de operation_type (IC-31)
     j. CHECK de invariantes de fila (CR-30)
     k. CHECK de rejected_reason canonico

Downgrade (observacion no bloqueante del Human Code Review, opcion A
elegida): antes de recrear la PRIMARY KEY simple de Iteracion 1
(idempotency_key), se detecta explicitamente si existen claves
textualmente duplicadas entre operation_type distintos - la PK
compuesta de esta migracion lo permite, la PK simple de Iteracion 1
no. Si se detecta, el downgrade aborta con un diagnostico explicito en
vez de fallar con un error generico de constraint.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_workflow_transitions"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_KNOWN_STATES_SQL = (
    "'NOT_ELIGIBLE', 'ELIGIBLE_PENDING', 'DRAFT', 'QA_IN_PROGRESS', "
    "'REVISION_REQUIRED', 'QA_PASS_PROVISIONAL', 'DEPENDENCY_RESOLUTION', "
    "'DEPENDENCY_BLOCKED', 'ROUTING_RESOLUTION', 'READY_FOR_HUMAN_APPROVAL', "
    "'APPROVED_CONTENT', 'SUPERSEDED'"
)


def upgrade() -> None:
    # 1. workflow_transition primero - idempotency_operation la referencia.
    op.create_table(
        "workflow_transition",
        sa.Column("transition_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "object_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("object_version.object_version_id"),
            nullable=False,
        ),
        sa.Column("from_state", sa.String(), nullable=False),
        sa.Column("to_state", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("idempotency_payload_hash", sa.String(), nullable=False),
        sa.Column("requested_by", sa.String(), nullable=False),
        sa.Column("correlation_id", sa.String(), nullable=False),
        sa.Column("causation_id", sa.String(), nullable=True),
        sa.Column(
            "applied_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            f"from_state IN ({_KNOWN_STATES_SQL})",
            name="ck_workflow_transition_from_state_known",
        ),
        sa.CheckConstraint(
            f"to_state IN ({_KNOWN_STATES_SQL})",
            name="ck_workflow_transition_to_state_known",
        ),
    )

    # 2. CR-21: object_version.status restringido al vocabulario completo.
    # No se requiere backfill: toda fila de Iteracion 1 usa 'DRAFT', ya
    # incluido en el vocabulario.
    op.create_check_constraint(
        "ck_object_version_status_known_states",
        "object_version",
        f"status IN ({_KNOWN_STATES_SQL})",
    )

    # 3.a
    op.add_column(
        "idempotency_operation", sa.Column("operation_type", sa.String(), nullable=True)
    )

    # 3.b - backfill obligatorio (CR-29): toda fila preexistente de
    # Iteracion 1 es una operacion de Approved State Commit.
    op.execute(
        "UPDATE idempotency_operation SET operation_type = 'APPROVED_STATE_COMMIT' "
        "WHERE operation_type IS NULL"
    )

    # 3.c - verificacion explicita, falla la migracion si queda algun NULL.
    connection = op.get_bind()
    remaining_nulls = connection.execute(
        sa.text("SELECT COUNT(*) FROM idempotency_operation WHERE operation_type IS NULL")
    ).scalar_one()
    if remaining_nulls != 0:
        raise RuntimeError(
            "Migracion 0002 abortada: quedan "
            f"{remaining_nulls} filas de idempotency_operation con "
            "operation_type NULL tras el backfill (CR-29)."
        )

    # 3.d
    op.alter_column("idempotency_operation", "operation_type", nullable=False)

    # 3.e
    op.add_column(
        "idempotency_operation",
        sa.Column(
            "transition_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflow_transition.transition_id"),
            nullable=True,
        ),
    )

    # 3.f
    op.add_column(
        "idempotency_operation", sa.Column("rejected_reason", sa.String(), nullable=True)
    )

    # 3.g - reemplazar CHECK de status.
    op.drop_constraint(
        "ck_idempotency_operation_status", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_status",
        "idempotency_operation",
        "status IN ('CLAIMED', 'COMMITTED', 'REJECTED')",
    )

    # 3.h - migrar PK de (idempotency_key) a (operation_type, idempotency_key).
    op.drop_constraint(
        "idempotency_operation_pkey", "idempotency_operation", type_="primary"
    )
    op.create_primary_key(
        "idempotency_operation_pkey",
        "idempotency_operation",
        ["operation_type", "idempotency_key"],
    )

    # 3.i - IC-31: dominio cerrado de operation_type.
    op.create_check_constraint(
        "ck_idempotency_operation_operation_type",
        "idempotency_operation",
        "operation_type IN ('APPROVED_STATE_COMMIT', 'WORKFLOW_TRANSITION')",
    )

    # 3.j - CR-30: invariantes de fila.
    op.create_check_constraint(
        "ck_idempotency_operation_row_invariants",
        "idempotency_operation",
        "(status = 'CLAIMED' AND commit_id IS NULL AND transition_id IS NULL "
        "AND rejected_reason IS NULL) "
        "OR (operation_type = 'APPROVED_STATE_COMMIT' AND status = 'COMMITTED' "
        "AND commit_id IS NOT NULL AND transition_id IS NULL AND rejected_reason IS NULL) "
        "OR (operation_type = 'WORKFLOW_TRANSITION' AND status = 'COMMITTED' "
        "AND commit_id IS NULL AND transition_id IS NOT NULL AND rejected_reason IS NULL) "
        "OR (operation_type = 'WORKFLOW_TRANSITION' AND status = 'REJECTED' "
        "AND commit_id IS NULL AND transition_id IS NULL AND rejected_reason IS NOT NULL)",
    )

    # 3.k - rejected_reason canonico.
    op.create_check_constraint(
        "ck_idempotency_operation_rejected_reason_canonical",
        "idempotency_operation",
        "rejected_reason IS NULL OR rejected_reason IN "
        "('UNKNOWN_STATE', 'TRANSITION_NOT_ENABLED_THIS_ITERATION', "
        "'STATE_CONFLICT', 'INVALID_OBJECT_VERSION')",
    )


def downgrade() -> None:
    # Observacion no bloqueante del Human Code Review de Iteracion 2:
    # como esta version permite la misma idempotency_key textual bajo
    # operation_type distintos (APPROVED_STATE_COMMIT vs
    # WORKFLOW_TRANSITION), volver a la PK simple (idempotency_key) de
    # Iteracion 1 es imposible si existen claves textualmente
    # duplicadas entre operation_type. Se detecta explicitamente antes
    # de intentar la migracion de PK, en vez de dejar que el downgrade
    # falle con un error generico de constraint - opcion A elegida
    # sobre documentar unicamente como deuda tecnica (opcion B),
    # siguiendo la instruccion explicita de esta revision.
    connection = op.get_bind()
    colliding_keys = connection.execute(
        sa.text(
            "SELECT idempotency_key, COUNT(DISTINCT operation_type) AS distinct_types "
            "FROM idempotency_operation "
            "GROUP BY idempotency_key "
            "HAVING COUNT(DISTINCT operation_type) > 1"
        )
    ).fetchall()
    if colliding_keys:
        colliding_list = ", ".join(row[0] for row in colliding_keys)
        raise RuntimeError(
            "Downgrade de 0002 abortado: las siguientes idempotency_key "
            "existen bajo mas de un operation_type y no pueden "
            "representarse bajo la PRIMARY KEY simple (idempotency_key) "
            f"de Iteracion 1: {colliding_list}. Resolver manualmente "
            "(renombrar o eliminar las filas en conflicto) antes de "
            "reintentar el downgrade."
        )

    op.drop_constraint(
        "ck_idempotency_operation_rejected_reason_canonical",
        "idempotency_operation",
        type_="check",
    )
    op.drop_constraint(
        "ck_idempotency_operation_row_invariants", "idempotency_operation", type_="check"
    )
    op.drop_constraint(
        "ck_idempotency_operation_operation_type", "idempotency_operation", type_="check"
    )
    op.drop_constraint(
        "idempotency_operation_pkey", "idempotency_operation", type_="primary"
    )
    op.create_primary_key(
        "idempotency_operation_pkey", "idempotency_operation", ["idempotency_key"]
    )
    op.drop_constraint(
        "ck_idempotency_operation_status", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_status",
        "idempotency_operation",
        "status IN ('CLAIMED', 'COMMITTED')",
    )
    op.drop_column("idempotency_operation", "rejected_reason")
    op.drop_column("idempotency_operation", "transition_id")
    op.alter_column("idempotency_operation", "operation_type", nullable=True)
    op.drop_column("idempotency_operation", "operation_type")
    op.drop_constraint(
        "ck_object_version_status_known_states", "object_version", type_="check"
    )
    op.drop_table("workflow_transition")
