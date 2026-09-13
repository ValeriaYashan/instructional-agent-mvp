"""
CODE-CR-69 (Human Code Review de Build v4, BLOCKER) - la autoridad
temporal de leases (LEASE_IS_VALID/LEASE_IS_EXPIRED, y el calculo de
lease_expires_at en adquisicion/reclamacion/refresco) es EXCLUSIVAMENTE
clock_timestamp() de PostgreSQL - nunca now()/CURRENT_TIMESTAMP
(estable durante toda la transaccion, no avanza mientras un UPDATE
espera un row lock) ni datetime.now() del proceso Python (reloj de
aplicacion, potencialmente desincronizado del reloj de la base).

Requieren un daemon de Docker disponible (testcontainers levanta un
PostgreSQL 16 real) - en este entorno sin Docker se recolectan pero no
se ejecutan, igual que el resto de tests/integration/.
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text, update
from sqlalchemy.orm import sessionmaker

from src.persistence.models import AgentRun, ObjectVersion
from src.persistence.repositories import agent_run_manager as arm
from src.persistence.repositories.context_engine import assemble_context

from tests.integration.test_orchestrator import _prepare_target

_TASK_TYPE = "PRODUCE_LEARNING_OBJECT"


def _new_run(session, suffix: str, max_attempts: int = 3) -> uuid.UUID:
    target = _prepare_target(session, suffix)
    v1 = uuid.uuid4()
    session.add(ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING"))
    session.commit()

    ctx = assemble_context(session, _TASK_TYPE, target, f"ctx-{target}")
    logical_run_id = arm.create_agent_run(
        session, "PRODUCER", _TASK_TYPE, v1, ctx.context_package_id,
        "tester", str(uuid.uuid4()), f"pe-{target}-clock", max_attempts=max_attempts,
    )
    session.commit()
    return logical_run_id


# ---------------------------------------------------------------------------
# (1) lease valido -> terminal authority posible
# ---------------------------------------------------------------------------


def test_valid_lease_allows_terminal_authority(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run(session, "040")
        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        # Lease recien creado via _new_lease_expiry_expr() (clock_timestamp()
        # + LEASE_DURATION_SECONDS) - vigente por construccion.
        authorized = arm.authorize_terminal_success(session, logical_run_id, "worker-A", auth.generation)
        assert authorized is True
        session.commit()

        run = session.get(AgentRun, logical_run_id)
        assert run.status == "SUCCEEDED"


# ---------------------------------------------------------------------------
# (2) lease expirado -> terminal authority imposible
# ---------------------------------------------------------------------------


def test_expired_lease_blocks_terminal_authority(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run(session, "041")
        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        authorized = arm.authorize_terminal_success(session, logical_run_id, "worker-A", auth.generation)
        assert authorized is False
        session.rollback()

        run = session.get(AgentRun, logical_run_id)
        assert run.status == "IN_PROGRESS"  # nunca paso a SUCCEEDED


# ---------------------------------------------------------------------------
# (3) igualdad/boundary -> expired (LEASE_IS_EXPIRED incluye la igualdad)
# ---------------------------------------------------------------------------


def test_lease_expiry_boundary_equality_counts_as_expired(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run(session, "042")
        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        # Fijar lease_expires_at EXACTAMENTE al clock_timestamp() del
        # servidor evaluado ahora mismo - por el tiempo que toma
        # ejecutar el UPDATE siguiente, clock_timestamp() en ESE
        # instante ya sera estrictamente mayor, cayendo del lado
        # EXPIRADO de la frontera <=.
        current_db_time = session.execute(select(func.clock_timestamp())).scalar_one()
        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=current_db_time)
        )
        session.commit()

        authorized = arm.authorize_terminal_success(session, logical_run_id, "worker-A", auth.generation)
        assert authorized is False  # igualdad (o mas viejo) => EXPIRADO, nunca vigente
        session.rollback()


# ---------------------------------------------------------------------------
# (4) reclaim solo cuando expired, nunca cuando valid
# ---------------------------------------------------------------------------


def test_reclaim_only_succeeds_when_expired_not_when_valid(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run(session, "043")
        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        # Lease todavia vigente (recien creado) -> reclaim debe fallar.
        blocked = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
        session.rollback()
        assert blocked.generation is None
        assert blocked.attempt_id is None

        # Expirarlo -> ahora si debe tener exito.
        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        allowed = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
        session.commit()
        assert allowed.generation == auth.generation + 1
        assert allowed.attempt_id is not None


# ---------------------------------------------------------------------------
# (5) same-owner retry solo cuando valid, nunca cuando expired
# ---------------------------------------------------------------------------


def test_same_owner_retry_only_succeeds_when_valid_not_when_expired(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run(session, "044")
        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        arm.close_attempt(session, auth.attempt_id, outcome="TECHNICAL_FAILURE", failure_detail="simulado-1")
        session.commit()

        # Lease vigente -> refresh debe tener exito, bajo la MISMA generacion.
        ok = arm.refresh_and_create_retry_attempt(
            session, logical_run_id, "worker-A", auth.generation, max_attempts=3
        )
        session.commit()
        assert ok.generation == auth.generation
        assert ok.attempt_id is not None
        arm.close_attempt(session, ok.attempt_id, outcome="TECHNICAL_FAILURE", failure_detail="simulado-2")
        session.commit()

        # Expirar el lease -> refresh debe fallar (el llamador debe pasar a reclaim).
        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        blocked = arm.refresh_and_create_retry_attempt(
            session, logical_run_id, "worker-A", ok.generation, max_attempts=3
        )
        session.rollback()
        assert blocked.generation is None
        assert blocked.attempt_id is None


# ---------------------------------------------------------------------------
# (6) expiry/refresh se calcula desde el reloj de la base, no del proceso
# ---------------------------------------------------------------------------


def test_lease_expiry_is_computed_from_database_clock(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run(session, "045")

        db_time_before = session.execute(select(func.clock_timestamp())).scalar_one()
        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        run = session.get(AgentRun, logical_run_id)
        expected = db_time_before + timedelta(seconds=arm.LEASE_DURATION_SECONDS)
        # Tolerancia amplia (segundos) para el tiempo real de ejecucion
        # del test - lo verificado es que el ancla es el reloj de la
        # BASE (clock_timestamp()), no un valor arbitrario.
        assert abs((run.lease_expires_at - expected).total_seconds()) < 5


# ---------------------------------------------------------------------------
# (7) no dependencia del reloj Python/application
# ---------------------------------------------------------------------------


def test_lease_authority_has_no_python_clock_dependency(session_factory: sessionmaker, monkeypatch):
    """
    Guarda de regresion: agent_run_manager ya NO importa ni usa
    datetime.now() de Python en ningun calculo de vigencia/expiracion
    de lease (CODE-CR-69) - toda la autoridad temporal pasa por
    clock_timestamp() de PostgreSQL. Se envenena deliberadamente
    cualquier atributo `datetime` que pudiera existir en el modulo: si
    una regresion futura reintrodujera datetime.now() alli, esta
    llamada fallaria de forma ruidosa (AttributeError) en vez de
    pasar silenciosamente.
    """
    monkeypatch.setattr(arm, "datetime", None, raising=False)

    with session_factory() as session:
        logical_run_id = _new_run(session, "046")

        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        assert auth.generation == 1

        authorized = arm.authorize_terminal_success(session, logical_run_id, "worker-A", auth.generation)
        assert authorized is True
        session.commit()


# ---------------------------------------------------------------------------
# (8) lock-wait crossing test: la implementacion NO depende del
# timestamp de inicio de transaccion (now()/CURRENT_TIMESTAMP)
# ---------------------------------------------------------------------------


def test_lock_wait_crossing_expiry_uses_wall_clock_not_transaction_start(_migrated_engine):
    """
    Test critico de CODE-CR-69: demuestra que la decision de vigencia
    del lease usa el reloj REAL en el instante de evaluacion
    (clock_timestamp()), no el instante de INICIO de la transaccion
    (now()/CURRENT_TIMESTAMP/transaction_timestamp() - estables durante
    toda la transaccion, incluso mientras un statement espera un row
    lock).

    Escenario:
    1. Se fija manualmente un lease con vencimiento cercano (2s reales).
    2. Una transaccion A toma un row lock (SELECT ... FOR UPDATE) sobre
       la fila de AgentRun y lo retiene sin comitear.
    3. Una transaccion B INICIA su UPDATE de autorizacion terminal
       (authorize_terminal_success) ANTES de que el lease expire - pero
       queda BLOQUEADA esperando el row lock que retiene A.
    4. Se deja pasar tiempo real suficiente para que el lease expire
       MIENTRAS B sigue bloqueada esperando el lock.
    5. A libera el lock. Solo entonces el UPDATE de B puede ejecutarse
       - con clock_timestamp() ya avanzado mas alla del vencimiento.

    Si la implementacion usara now()/CURRENT_TIMESTAMP (fijo desde el
    INICIO de la transaccion de B, que fue ANTES del vencimiento), B
    veria el lease como todavia vigente y podria finalizar
    incorrectamente - violando NO_STALE_WORKER_CAN_COMMIT. Con
    clock_timestamp() (evaluado en el instante REAL de ejecucion del
    UPDATE, despues de liberado el lock), B debe ver el lease como
    EXPIRADO y authorize_terminal_success debe devolver False.
    """
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)
    lease_seconds = 2
    wait_past_expiry_seconds = lease_seconds + 2

    with session_factory() as setup:
        logical_run_id = _new_run(setup, "047")
        auth = arm.acquire_and_create_first_attempt(setup, logical_run_id, "worker-A")
        setup.commit()
        generation = auth.generation

        setup.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=lease_seconds))
        )
        setup.commit()

    results: dict = {}
    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_row_lock():
        with session_factory() as session:
            session.execute(
                text("SELECT * FROM agent_run WHERE logical_run_id = CAST(:id AS uuid) FOR UPDATE"),
                {"id": str(logical_run_id)},
            )
            lock_acquired.set()
            release_lock.wait(timeout=15)
            session.rollback()  # libera el row lock

    def try_authorize_after_lock_wait():
        lock_acquired.wait(timeout=15)
        # Esta transaccion inicia (autobegin) ANTES de que el lease
        # expire, pero el UPDATE de authorize_terminal_success queda
        # bloqueado esperando el row lock retenido por hold_row_lock -
        # el lease va a vencer EN TIEMPO REAL mientras espera.
        with session_factory() as session:
            authorized = arm.authorize_terminal_success(session, logical_run_id, "worker-A", generation)
            if authorized:
                session.commit()
            else:
                session.rollback()
            results["authorized"] = authorized

    t_lock = threading.Thread(target=hold_row_lock)
    t_auth = threading.Thread(target=try_authorize_after_lock_wait)

    t_lock.start()
    lock_acquired.wait(timeout=15)
    t_auth.start()

    # Dar tiempo a que t_auth quede efectivamente bloqueada esperando
    # el row lock (no solo esperando el Event de threading) antes de
    # dejar transcurrir el vencimiento real del lease.
    time.sleep(0.5)
    time.sleep(wait_past_expiry_seconds)
    release_lock.set()

    t_lock.join(timeout=15)
    t_auth.join(timeout=15)

    assert "authorized" in results
    assert results["authorized"] is False  # lease ya vencido en tiempo real al momento de evaluar

    with session_factory() as session:
        run = session.get(AgentRun, logical_run_id)
        assert run.status == "IN_PROGRESS"  # nunca paso a SUCCEEDED via un now() congelado


def test_lock_wait_crossing_expiry_blocks_mark_failed_if_owner(_migrated_engine):
    """
    CODE-CR-69 REABIERTO: mismo escenario de carrera que
    test_lock_wait_crossing_expiry_uses_wall_clock_not_transaction_start,
    aplicado a mark_failed_if_owner() - la otra funcion LEASE_IS_VALID
    que tambien recibio _lock_agent_run_row_for_update(). Un worker que
    perdio su lease MIENTRAS esperaba el row lock nunca debe poder
    marcar FAILED usando un clock_timestamp() calculado antes de esa
    espera.
    """
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)
    lease_seconds = 2
    wait_past_expiry_seconds = lease_seconds + 2

    with session_factory() as setup:
        logical_run_id = _new_run(setup, "048", max_attempts=1)
        auth = arm.acquire_and_create_first_attempt(setup, logical_run_id, "worker-A")
        setup.commit()
        generation = auth.generation

        setup.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=lease_seconds))
        )
        setup.commit()

    results: dict = {}
    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_row_lock():
        with session_factory() as session:
            session.execute(
                text("SELECT * FROM agent_run WHERE logical_run_id = CAST(:id AS uuid) FOR UPDATE"),
                {"id": str(logical_run_id)},
            )
            lock_acquired.set()
            release_lock.wait(timeout=15)
            session.rollback()

    def try_mark_failed_after_lock_wait():
        lock_acquired.wait(timeout=15)
        with session_factory() as session:
            marked = arm.mark_failed_if_owner(session, logical_run_id, "worker-A", generation)
            if marked:
                session.commit()
            else:
                session.rollback()
            results["marked"] = marked

    t_lock = threading.Thread(target=hold_row_lock)
    t_mark = threading.Thread(target=try_mark_failed_after_lock_wait)

    t_lock.start()
    lock_acquired.wait(timeout=15)
    t_mark.start()

    time.sleep(0.5)
    time.sleep(wait_past_expiry_seconds)
    release_lock.set()

    t_lock.join(timeout=15)
    t_mark.join(timeout=15)

    assert "marked" in results
    assert results["marked"] is False  # lease ya vencido en tiempo real al momento de evaluar

    with session_factory() as session:
        run = session.get(AgentRun, logical_run_id)
        assert run.status == "IN_PROGRESS"  # nunca paso a FAILED via un now() congelado


def test_lock_wait_crossing_expiry_blocks_same_owner_retry_refresh(_migrated_engine):
    """
    CODE-CR-69 REABIERTO: mismo escenario de carrera aplicado a
    refresh_lease_for_same_owner_retry() - un worker que perdio su
    lease MIENTRAS esperaba el row lock nunca debe poder refrescarlo
    usando un clock_timestamp() calculado antes de esa espera (eso
    permitiria a un worker obsoleto seguir compitiendo por Attempts
    bajo una generation que en realidad ya perdio).
    """
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)
    lease_seconds = 2
    wait_past_expiry_seconds = lease_seconds + 2

    with session_factory() as setup:
        logical_run_id = _new_run(setup, "049")
        auth = arm.acquire_and_create_first_attempt(setup, logical_run_id, "worker-A")
        setup.commit()
        generation = auth.generation

        original_expiry = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        setup.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=original_expiry)
        )
        setup.commit()

    results: dict = {}
    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_row_lock():
        with session_factory() as session:
            session.execute(
                text("SELECT * FROM agent_run WHERE logical_run_id = CAST(:id AS uuid) FOR UPDATE"),
                {"id": str(logical_run_id)},
            )
            lock_acquired.set()
            release_lock.wait(timeout=15)
            session.rollback()

    def try_refresh_after_lock_wait():
        lock_acquired.wait(timeout=15)
        with session_factory() as session:
            refreshed = arm.refresh_lease_for_same_owner_retry(session, logical_run_id, "worker-A", generation)
            if refreshed is not None:
                session.commit()
            else:
                session.rollback()
            results["refreshed"] = refreshed

    t_lock = threading.Thread(target=hold_row_lock)
    t_refresh = threading.Thread(target=try_refresh_after_lock_wait)

    t_lock.start()
    lock_acquired.wait(timeout=15)
    t_refresh.start()

    time.sleep(0.5)
    time.sleep(wait_past_expiry_seconds)
    release_lock.set()

    t_lock.join(timeout=15)
    t_refresh.join(timeout=15)

    assert "refreshed" in results
    assert results["refreshed"] is None  # lease ya vencido en tiempo real al momento de evaluar

    with session_factory() as session:
        run = session.get(AgentRun, logical_run_id)
        assert run.status == "IN_PROGRESS"
        assert run.claimed_by == "worker-A"  # nadie lo reclamo todavia
        # El lease NUNCA se extendio via un clock_timestamp() congelado
        # anterior al vencimiento real - sigue siendo el original (ya vencido).
        assert run.lease_expires_at == original_expiry
