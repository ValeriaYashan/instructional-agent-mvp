"""Iteracion 1: object_version, approved_state_pointer, approved_state_commit, idempotency_operation, audit_event

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-09-11

Corregido (CODE-CR-01, Human Code Review): approved_state_commit.outcome
ahora solo permite 'COMMITTED'; se agrega idempotency_operation como
mecanismo de reserva de concurrencia aislado de approved_state_commit.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "object_version",
        sa.Column("object_version_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("object_id", sa.String(), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column(
            "supersedes_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("object_version.object_version_id"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "object_id", "object_version_id", name="uq_object_version_object_id_version_id"
        ),
    )

    op.create_table(
        "approved_state_pointer",
        sa.Column("object_id", sa.String(), primary_key=True),
        sa.Column("current_approved_version_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["object_id", "current_approved_version_id"],
            ["object_version.object_id", "object_version.object_version_id"],
            name="fk_approved_state_pointer_object_version",
        ),
    )

    op.create_table(
        "approved_state_commit",
        sa.Column("commit_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("object_id", sa.String(), nullable=False),
        sa.Column("object_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "expected_previous_approved_version_id", postgresql.UUID(as_uuid=True), nullable=True
        ),
        sa.Column(
            "actual_previous_approved_version_id", postgresql.UUID(as_uuid=True), nullable=True
        ),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("idempotency_payload_hash", sa.String(), nullable=False),
        # CODE-CR-01: unico valor permitido, nunca 'PENDING'. La fila
        # solo se inserta cuando la operacion completa ya tuvo exito,
        # dentro de la misma transaccion atomica que muta el puntero y
        # escribe el audit event.
        sa.Column("outcome", sa.String(), nullable=False, server_default="COMMITTED"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "idempotency_key", name="uq_approved_state_commit_idempotency_key"
        ),
        sa.ForeignKeyConstraint(
            ["object_id", "object_version_id"],
            ["object_version.object_id", "object_version.object_version_id"],
            name="fk_approved_state_commit_object_version",
        ),
        sa.CheckConstraint(
            "outcome = 'COMMITTED'",
            name="ck_approved_state_commit_outcome_committed_only",
        ),
    )

    # Nueva (CODE-CR-01): mecanismo de reserva de concurrencia para
    # idempotency_key, aislado del invariante de approved_state_commit.
    op.create_table(
        "idempotency_operation",
        sa.Column("idempotency_key", sa.String(), primary_key=True),
        sa.Column("idempotency_payload_hash", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="CLAIMED"),
        sa.Column(
            "commit_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("approved_state_commit.commit_id"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('CLAIMED', 'COMMITTED')",
            name="ck_idempotency_operation_status",
        ),
    )

    op.create_table(
        "audit_event",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("object_id", sa.String(), nullable=False),
        sa.Column("object_version_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "commit_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("approved_state_commit.commit_id"),
            nullable=True,
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default="{}"),
    )

    op.create_index(
        "ix_object_version_object_id", "object_version", ["object_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_object_version_object_id", table_name="object_version")
    op.drop_table("audit_event")
    op.drop_table("idempotency_operation")
    op.drop_table("approved_state_commit")
    op.drop_table("approved_state_pointer")
    op.drop_table("object_version")
