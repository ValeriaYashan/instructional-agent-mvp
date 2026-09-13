"""
Fixtures de integracion.

Requieren un daemon de Docker disponible (testcontainers levanta un
PostgreSQL 16 real). Estos tests validan comportamiento especifico de
PostgreSQL (IS NOT DISTINCT FROM, constraints UNIQUE/CHECK, foreign keys
compuestas) que no es fielmente reproducible contra un doble de prueba
o un motor distinto - por eso no se sustituyen por SQLite ni mocks
(decision ya tomada en Fase 3: pytest + testcontainers-python).
"""

from __future__ import annotations

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

try:
    # testcontainers >= ~4.9 movio este modulo a testcontainers.community.postgres
    from testcontainers.community.postgres import PostgresContainer
except ImportError:  # pragma: no cover - compatibilidad con testcontainers < 4.9
    from testcontainers.postgres import PostgresContainer

from src.persistence.db import make_engine


@pytest.fixture(scope="session")
def postgres_url() -> str:
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url(driver="psycopg")


@pytest.fixture(scope="session")
def _migrated_engine(postgres_url: str):
    engine = make_engine(postgres_url)
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(alembic_cfg, "head")
    yield engine
    engine.dispose()


@pytest.fixture()
def session_factory(_migrated_engine) -> sessionmaker[Session]:
    return sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _clean_tables(_migrated_engine):
    yield
    with _migrated_engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE TABLE audit_event, idempotency_operation, "
                "object_version_content, agent_run_attempt, agent_run, "
                "context_package, evidence_source, object_relation, "
                "workflow_transition, approved_state_commit, "
                "approved_state_pointer, object_version, "
                "instructional_object CASCADE"
            )
        )
