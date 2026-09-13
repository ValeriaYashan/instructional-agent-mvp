"""Iteracion 3: instructional_object, object_relation, evidence_source, context_package + generalizacion de idempotency_operation

Revision ID: 0003_context_engine
Revises: 0002_workflow_transitions
Create Date: 2026-09-13

Compatible hacia atras (CR-33, CR-35, mismo patron que CR-29 en 0002):
opera sobre una base de datos ya poblada por Iteraciones 1-2. No asume
base vacia. 0001 y 0002 permanecen inmutables.

Secuencia:
  1. CREATE TABLE instructional_object (object_type nullable)
  2. Backfill: un instructional_object por cada object_id DISTINCT ya
     presente en object_version, con object_type = NULL (desconocido/
     legacy - CR-33, no se inventa un valor)
  3. Verificar (assert de migracion) que no queda ningun object_id de
     object_version sin su instructional_object correspondiente
  4. ALTER TABLE object_version: ADD FOREIGN KEY (object_id) ->
     instructional_object.object_id. NO se instala ningun trigger de
     auto-provisionamiento permanente (CODE-CR-42, Human Code Review):
     el backfill del paso 2 es un evento de migracion unico; despues de
     esta migracion, todo object_id nuevo requiere su
     InstructionalObject creado explicitamente antes de cualquier
     ObjectVersion.
  5. CREATE TABLE object_relation
  6. CREATE TABLE evidence_source
  7. CREATE TABLE context_package
  8. idempotency_operation:
     a. DROP CHECK ck_idempotency_operation_operation_type (2 valores)
     b. ADD CHECK ck_idempotency_operation_operation_type (3 valores,
        agrega 'CONTEXT_ASSEMBLY')
     c. DROP CHECK ck_idempotency_operation_rejected_reason_canonical
        (4 valores)
     d. ADD CHECK ck_idempotency_operation_rejected_reason_canonical
        (7 valores, agrega los 3 rejected_reason de Context Assembly)
     e. ADD COLUMN context_package_id UUID NULL REFERENCES context_package
     f. DROP CHECK ck_idempotency_operation_row_invariants (4 ramas)
     g. ADD CHECK ck_idempotency_operation_row_invariants (6 ramas,
        agrega CONTEXT_ASSEMBLY+COMMITTED y CONTEXT_ASSEMBLY+REJECTED)
  9. audit_event: ADD COLUMN context_package_id UUID NULL REFERENCES
     context_package

Downgrade: guarda explicita de robustez (Human Code Review) - antes de
tocar nada, se verifica que no existan filas de idempotency_operation
con operation_type='CONTEXT_ASSEMBLY' ni filas en context_package. Si
existen, el downgrade aborta con un diagnostico claro en vez de fallar
mas adelante con un error opaco de CHECK constraint al intentar
restaurar el dominio de 2 valores de operation_type de Iteracion 2.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_context_engine"
down_revision: Union[str, None] = "0002_workflow_transitions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_EVIDENCE_CLASSES_SQL = "'E0', 'E1', 'E2', 'E3'"


def upgrade() -> None:
    # 1. instructional_object - identidad estable (CR-33).
    op.create_table(
        "instructional_object",
        sa.Column("object_id", sa.String(), primary_key=True),
        sa.Column("object_type", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # 2. Backfill obligatorio: un instructional_object por cada object_id
    # distinto ya presente en object_version, con object_type = NULL.
    op.execute(
        "INSERT INTO instructional_object (object_id, object_type, created_at) "
        "SELECT DISTINCT object_id, NULL, now() FROM object_version "
        "ON CONFLICT (object_id) DO NOTHING"
    )

    # 3. Verificacion explicita antes de agregar la FK.
    connection = op.get_bind()
    orphan_count = connection.execute(
        sa.text(
            "SELECT COUNT(*) FROM object_version ov "
            "LEFT JOIN instructional_object io ON io.object_id = ov.object_id "
            "WHERE io.object_id IS NULL"
        )
    ).scalar_one()
    if orphan_count != 0:
        raise RuntimeError(
            "Migracion 0003 abortada: quedan "
            f"{orphan_count} filas de object_version sin instructional_object "
            "correspondiente tras el backfill (CR-33)."
        )

    # 4. FK de object_version hacia la nueva identidad estable.
    op.create_foreign_key(
        "fk_object_version_instructional_object",
        "object_version",
        "instructional_object",
        ["object_id"],
        ["object_id"],
    )

    # CODE-CR-42 (Human Code Review): NO se instala ningun trigger de
    # auto-provisionamiento permanente. Despues de esta migracion, todo
    # object_id nuevo debe crear su InstructionalObject explicitamente
    # antes de cualquier ObjectVersion que lo referencie - la FK lo
    # exige sin excepcion. El backfill del paso 2 es un evento de
    # migracion unico para las filas legacy de Iteraciones 1-2, no un
    # mecanismo permanente.

    # 5. object_relation (Paso 3 del Context Engine).
    op.create_table(
        "object_relation",
        sa.Column("relation_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "from_object_id",
            sa.String(),
            sa.ForeignKey("instructional_object.object_id"),
            nullable=False,
        ),
        sa.Column(
            "to_object_id",
            sa.String(),
            sa.ForeignKey("instructional_object.object_id"),
            nullable=False,
        ),
        sa.Column("relation_type", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "from_object_id",
            "relation_type",
            name="uq_object_relation_from_object_relation_type_singular",
        ),
    )

    # 6. evidence_source (Paso 7 - adaptador NO semantico, CR-34).
    op.create_table(
        "evidence_source",
        sa.Column("source_id", sa.String(), primary_key=True),
        sa.Column(
            "object_id",
            sa.String(),
            sa.ForeignKey("instructional_object.object_id"),
            nullable=False,
        ),
        sa.Column("evidence_class", sa.String(), nullable=False),
        sa.Column("content_reference", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            f"evidence_class IN ({_EVIDENCE_CLASSES_SQL})",
            name="ck_evidence_source_evidence_class_known",
        ),
    )

    # 7. context_package (Paso 9 - salida inmutable).
    op.create_table(
        "context_package",
        sa.Column("context_package_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("context_package_hash", sa.String(), nullable=False),
        sa.Column("context_contract_version", sa.String(), nullable=False),
        sa.Column("task_type", sa.String(), nullable=False),
        sa.Column(
            "target_object_id",
            sa.String(),
            sa.ForeignKey("instructional_object.object_id"),
            nullable=False,
        ),
        sa.Column(
            "pinned_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("object_version.object_version_id"),
            nullable=True,
        ),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("approved_objects", postgresql.JSONB(), nullable=False),
        sa.Column("relations", postgresql.JSONB(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column("dependency_status", postgresql.JSONB(), nullable=False),
        sa.Column("gaps", postgresql.JSONB(), nullable=False),
        sa.Column("run_metadata", postgresql.JSONB(), nullable=False),
        sa.Column(
            "assembled_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # 8. idempotency_operation: ampliar operation_type, rejected_reason,
    # row invariants; agregar context_package_id.
    op.drop_constraint(
        "ck_idempotency_operation_operation_type", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_operation_type",
        "idempotency_operation",
        "operation_type IN ('APPROVED_STATE_COMMIT', 'WORKFLOW_TRANSITION', 'CONTEXT_ASSEMBLY')",
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
        "'TASK_TYPE_UNRESOLVED', 'MAPPING_UNDEFINED', 'NEWER_VERSION_AVAILABLE')",
    )

    op.add_column(
        "idempotency_operation",
        sa.Column(
            "context_package_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("context_package.context_package_id"),
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

    # 9. audit_event: referencia opcional a context_package.
    op.add_column(
        "audit_event",
        sa.Column(
            "context_package_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("context_package.context_package_id"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    # Chequeo de robustez explicito (Human Code Review): downgradear
    # esta migracion es destructivo para cualquier dato de Iteracion 3
    # (CONTEXT_ASSEMBLY es un operation_type que Iteracion 2 no conoce,
    # y context_package no existe alli). En vez de dejar que el
    # downgrade falle mas adelante con un error opaco de CHECK
    # constraint al reinsertar filas incompatibles, se detecta
    # explicitamente ANTES de tocar nada y se aborta con un
    # diagnostico claro - misma politica ya aplicada en el downgrade de
    # 0002 para colisiones de idempotency_key entre operation_type.
    connection = op.get_bind()

    context_assembly_count = connection.execute(
        sa.text(
            "SELECT COUNT(*) FROM idempotency_operation WHERE operation_type = 'CONTEXT_ASSEMBLY'"
        )
    ).scalar_one()
    context_package_count = connection.execute(
        sa.text("SELECT COUNT(*) FROM context_package")
    ).scalar_one()

    if context_assembly_count != 0 or context_package_count != 0:
        raise RuntimeError(
            "Downgrade de 0003 abortado: existen "
            f"{context_assembly_count} filas de idempotency_operation con "
            f"operation_type='CONTEXT_ASSEMBLY' y {context_package_count} filas "
            "de context_package. Ninguna de las dos puede representarse "
            "bajo el esquema de Iteracion 2 (que no conoce "
            "operation_type='CONTEXT_ASSEMBLY' ni la tabla context_package). "
            "Eliminar o migrar manualmente esos datos antes de reintentar "
            "el downgrade."
        )

    op.drop_column("audit_event", "context_package_id")

    op.drop_constraint(
        "ck_idempotency_operation_row_invariants", "idempotency_operation", type_="check"
    )
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

    op.drop_column("idempotency_operation", "context_package_id")

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
        "'STATE_CONFLICT', 'INVALID_OBJECT_VERSION')",
    )

    op.drop_constraint(
        "ck_idempotency_operation_operation_type", "idempotency_operation", type_="check"
    )
    op.create_check_constraint(
        "ck_idempotency_operation_operation_type",
        "idempotency_operation",
        "operation_type IN ('APPROVED_STATE_COMMIT', 'WORKFLOW_TRANSITION')",
    )

    op.drop_table("context_package")
    op.drop_table("evidence_source")
    op.drop_table("object_relation")
    op.drop_constraint(
        "fk_object_version_instructional_object", "object_version", type_="foreignkey"
    )
    op.drop_table("instructional_object")
