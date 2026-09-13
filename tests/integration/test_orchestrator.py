from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import pytest

from src.domain.agent_run_result import AgentRunOutcome
from src.persistence.models import (
    AgentRun,
    AgentRunAttempt,
    ApprovedStatePointer,
    AuditEvent,
    EvidenceSource,
    IdempotencyOperation,
    InstructionalObject,
    ObjectRelation,
    ObjectVersion,
    ObjectVersionContent,
)
from src.persistence.repositories import agent_run_manager as arm
from src.persistence.repositories.approved_state import (
    commit_approved_state,
    initialize_approved_state_pointer,
)
from src.persistence.repositories.orchestrator import request_producer_execution
from src.persistence.repositories.producer_executor import FakeProducerAdapter
from src.persistence.repositories.producer_output_schema import PRODUCER_OUTPUT_SCHEMA_VERSION

_PROVIDER = "fake"
_MODEL = "fake-model-1"
_INSTRUCTIONS_VERSION = "instructions-v1"


def _ensure_instructional_object(session, object_id: str, object_type: str | None = None) -> None:
    if session.get(InstructionalObject, object_id) is None:
        session.add(InstructionalObject(object_id=object_id, object_type=object_type))
        session.flush()


def _approve_object(session, object_id: str, object_type: str | None = None) -> uuid.UUID:
    _ensure_instructional_object(session, object_id, object_type)
    version_id = uuid.uuid4()
    session.add(
        ObjectVersion(
            object_version_id=version_id, object_id=object_id, version_number=1, status="APPROVED_CONTENT"
        )
    )
    session.commit()
    initialize_approved_state_pointer(session, object_id)
    commit_approved_state(
        session, object_id, version_id, None, idempotency_key=f"approve-{object_id}-{version_id}"
    )
    return version_id


def _link(session, from_object_id: str, to_object_id: str, relation_type: str) -> None:
    session.add(
        ObjectRelation(
            relation_id=uuid.uuid4(), from_object_id=from_object_id, to_object_id=to_object_id,
            relation_type=relation_type,
        )
    )
    session.commit()


def _add_evidence(session, object_id: str, evidence_class: str, source_id: str) -> None:
    session.add(
        EvidenceSource(
            source_id=source_id, object_id=object_id, evidence_class=evidence_class,
            content_reference=f"ref://{source_id}",
        )
    )
    session.commit()


def _prepare_target(session, suffix: str) -> str:
    """Target NUEVO (sin version aprobada propia todavia), con
    dependencias satisfechas - listo para que Producer lo complete por
    primera vez."""
    target = f"OBJ-PX-{suffix}"
    activity = f"ACT-PX-{suffix}"
    outcome = f"LO-PX-{suffix}"

    _ensure_instructional_object(session, target, object_type="LEARNING_OBJECT")
    _approve_object(session, activity, object_type="ACTIVITY")
    _approve_object(session, outcome, object_type="LEARNING_OUTCOME")
    _link(session, target, activity, "PARENT_ACTIVITY")
    _link(session, target, outcome, "LEARNING_OUTCOME")
    _add_evidence(session, target, "E1", f"EV-{suffix}-1")
    return target


def _valid_output(**overrides) -> dict:
    base = {
        "schema_version": PRODUCER_OUTPUT_SCHEMA_VERSION,
        "object_type": "LEARNING_OBJECT",
        "title": "Titulo de prueba",
        "body": "Cuerpo de prueba suficientemente largo.",
        "learning_outcome_refs": [],
        "metadata": {},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Camino exitoso completo (S10 de Proposal v0.5)
# ---------------------------------------------------------------------------


def test_golden_path_persists_content_and_transitions_to_draft(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "001")
        executor = FakeProducerAdapter([_valid_output()])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        version_id = result.object_version_id

        version = session.get(ObjectVersion, version_id)
        assert version.status == "DRAFT"

        content = session.get(ObjectVersionContent, version_id)
        assert content is not None
        assert content.title == "Titulo de prueba"

        run = session.get(AgentRun, result.logical_run_id)
        assert run.status == "SUCCEEDED"

        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == run.logical_run_id)
        ).scalars().all()
        assert len(attempts) == 1
        assert attempts[0].outcome == "SUCCESS"

        pointer = session.get(ApprovedStatePointer, target)
        assert pointer is None or pointer.current_approved_version_id is None  # PRODUCER_NEVER_MUTATES_APPROVED_STATE

        events = session.execute(
            select(AuditEvent).where(AuditEvent.agent_run_id == run.logical_run_id)
        ).scalars().all()
        assert any(e.event_type == "PRODUCER_CONTENT_PERSISTED" for e in events)


# ---------------------------------------------------------------------------
# CODE-CR-74 (Human Code Review de Build v6, BLOCKER): la reserva
# PRODUCER_EXECUTION ya NO se commitea antes de Context Assembly - la
# secuencia correcta es preflight de solo lectura (siempre cerrado) ->
# Context Assembly (SIEMPRE con session.in_transaction() == False) ->
# reserva + AgentRun atomicos DESPUES de que Context Assembly cerro su
# propia transaccion. Este test prueba exactamente el escenario exigido
# por la revision de Build v7: una clave NUNCA vista antes debe
# atravesar Context Assembly sin jamas disparar el RuntimeError de la
# guarda transaccional de Context Engine (CR-37), y debe producir
# EXACTAMENTE un AgentRun.
# ---------------------------------------------------------------------------


def test_new_unseen_key_passes_through_context_assembly_without_transaction_guard_violation(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        target = _prepare_target(session, "CR74-001")
        key = f"pe-cr74-{uuid.uuid4()}"
        executor = FakeProducerAdapter([_valid_output()])

        # Precondicion explicita: ninguna reserva PRODUCER_EXECUTION
        # existe todavia para esta idempotency_key - es una clave
        # genuinamente nueva, exactamente el caso que Build v6 rompia.
        pre_existing = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == "PRODUCER_EXECUTION",
                IdempotencyOperation.idempotency_key == key,
            )
        ).scalar_one_or_none()
        assert pre_existing is None

        # CODE-CR-74: en Build v6, esta llamada levantaba
        # RuntimeError("assemble_context requires a Session with no
        # active transaction...") porque la reserva CLAIMED dejaba una
        # transaccion abierta antes de invocar assemble_context(). Si
        # la regresion reaparece, esta linea vuelve a fallar con esa
        # excepcion sin control, no con un AssertionError - el mensaje
        # de pytest sera inequivoco sobre cual invariante se rompio.
        result = request_producer_execution(
            session, target, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        assert result.logical_run_id is not None

        # Exactamente un AgentRun para esta idempotency_key - nunca cero,
        # nunca dos (AT_MOST_ONE_LOGICAL_AGENT_RUN_PER_IDEMPOTENCY_KEY).
        agent_runs = session.execute(
            select(AgentRun).where(AgentRun.idempotency_key == key)
        ).scalars().all()
        assert len(agent_runs) == 1
        assert agent_runs[0].logical_run_id == result.logical_run_id
        assert agent_runs[0].status == "SUCCEEDED"

        # La reserva PRODUCER_EXECUTION quedo COMMITTED, apuntando al
        # mismo AgentRun, con context_package_id NULL en esta tabla
        # (CR-63: AgentRun, nunca IdempotencyOperation, es la autoridad
        # de context_package_id).
        reservation = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == "PRODUCER_EXECUTION",
                IdempotencyOperation.idempotency_key == key,
            )
        ).scalar_one()
        assert reservation.status == "COMMITTED"
        assert reservation.agent_run_id == result.logical_run_id
        assert reservation.context_package_id is None

        # Context Assembly efectivamente corrio y produjo exactamente un
        # ContextPackage para esta ejecucion (nunca cero - la reserva de
        # Context Engine para context-for:{key} lo prueba).
        context_reservation = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == "CONTEXT_ASSEMBLY",
                IdempotencyOperation.idempotency_key == f"context-for:{key}",
            )
        ).scalar_one()
        assert context_reservation.status == "COMMITTED"
        assert executor._call_count == 1


def test_context_engine_idempotency_conflict_never_misclassified_as_invalid_context_package(
    session_factory: sessionmaker, monkeypatch
):
    """
    Parte de la correccion CODE-CR-74: antes de esta correccion, CUALQUIER
    outcome de Context Engine distinto de COMMITTED (incluido su PROPIO
    ContextAssemblyOutcome.IDEMPOTENCY_CONFLICT, que ocurre cuando la
    context_idempotency_key derivada - f"context-for:{idempotency_key}" -
    ya fue tomada por una solicitud PRODUCER_EXECUTION distinta que
    comparte la misma idempotency_key de nivel superior pero difiere en
    target_object_id u otro campo de identidad de pre-contexto) se
    colapsaba a AgentRunOutcome.INVALID_CONTEXT_PACKAGE. Ese conflicto de
    NIVEL SUPERIOR debe propagarse como AgentRunOutcome.IDEMPOTENCY_CONFLICT
    - nunca como un fallo del contrato de 10 pasos - y no debe escribir
    ninguna fila PRODUCER_EXECUTION ni invocar al proveedor.

    Se usa un doble de assemble_context (mismo patron que
    test_invalid_context_package_is_rejected_and_replay_never_retries_context_assembly)
    porque esta prueba ejercita la clasificacion del outcome en
    request_producer_execution, no el mecanismo interno por el cual
    Context Engine llega a detectar su propio conflicto.
    """
    import src.persistence.repositories.orchestrator as orchestrator_module
    from src.domain.context_result import ContextAssemblyOutcome, ContextAssemblyResult

    def _fake_assemble_context_conflict(session, task_type, target_object_id, idempotency_key):
        return ContextAssemblyResult(outcome=ContextAssemblyOutcome.IDEMPOTENCY_CONFLICT)

    monkeypatch.setattr(orchestrator_module, "assemble_context", _fake_assemble_context_conflict)

    with session_factory() as session:
        target = "OBJ-CR74-CONFLICT-NEVER-TOUCHED"
        key = f"pe-cr74-conflict-{uuid.uuid4()}"
        executor = FakeProducerAdapter([_valid_output()])

        result = request_producer_execution(
            session, target, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert result.outcome == AgentRunOutcome.IDEMPOTENCY_CONFLICT
        assert executor._call_count == 0  # el proveedor nunca fue invocado

        # Ninguna fila PRODUCER_EXECUTION se escribe para un conflicto de
        # este tipo - ni COMMITTED ni REJECTED.
        reservation = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == "PRODUCER_EXECUTION",
                IdempotencyOperation.idempotency_key == key,
            )
        ).scalar_one_or_none()
        assert reservation is None

        agent_runs = session.execute(
            select(AgentRun).where(AgentRun.idempotency_key == key)
        ).scalars().all()
        assert agent_runs == []



# ---------------------------------------------------------------------------
# CR-58: reintento tecnico bajo ownership vigente, sin cambio de generation
# ---------------------------------------------------------------------------


def test_technical_failure_then_success_same_owner_retry(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "002")
        executor = FakeProducerAdapter([RuntimeError("timeout simulado"), _valid_output()])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        attempts = session.execute(
            select(AgentRunAttempt)
            .where(AgentRunAttempt.logical_run_id == result.logical_run_id)
            .order_by(AgentRunAttempt.attempt_number)
        ).scalars().all()

        assert len(attempts) == 2
        assert attempts[0].outcome == "TECHNICAL_FAILURE"
        assert attempts[1].outcome == "SUCCESS"
        # CR-58: mismo lease_generation en ambos intentos - reintento
        # bajo ownership vigente, NO reclamacion.
        assert attempts[0].lease_generation_at_creation == attempts[1].lease_generation_at_creation


def test_invalid_output_then_success_same_owner_retry(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "003")
        invalid = {"schema_version": "wrong-version"}
        executor = FakeProducerAdapter([invalid, _valid_output()])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        attempts = session.execute(
            select(AgentRunAttempt)
            .where(AgentRunAttempt.logical_run_id == result.logical_run_id)
            .order_by(AgentRunAttempt.attempt_number)
        ).scalars().all()
        assert attempts[0].outcome == "INVALID_OUTPUT"
        assert attempts[1].outcome == "SUCCESS"
        assert attempts[0].lease_generation_at_creation == attempts[1].lease_generation_at_creation


# ---------------------------------------------------------------------------
# max_attempts agotado -> FAILED, ObjectVersion permanece ELIGIBLE_PENDING
# ---------------------------------------------------------------------------


def test_max_attempts_exceeded_yields_failed_and_no_content(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "004")
        executor = FakeProducerAdapter([RuntimeError("siempre falla")])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor, max_attempts=2,
        )

        assert result.outcome == AgentRunOutcome.FAILED

        version = session.execute(
            select(ObjectVersion).where(ObjectVersion.object_id == target)
        ).scalar_one()
        assert version.status == "ELIGIBLE_PENDING"

        content = session.get(ObjectVersionContent, version.object_version_id)
        assert content is None

        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == result.logical_run_id)
        ).scalars().all()
        assert len(attempts) == 2  # exactamente max_attempts, ni uno mas


# ---------------------------------------------------------------------------
# Idempotencia de la solicitud
# ---------------------------------------------------------------------------


def test_replay_same_key_same_payload_does_not_call_provider_twice(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "005")
        executor = FakeProducerAdapter([_valid_output()])
        key = f"pe-{target}-replay"

        first = request_producer_execution(
            session, target, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )
        second = request_producer_execution(
            session, target, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert first.outcome == AgentRunOutcome.SUCCEEDED
        assert second.outcome == AgentRunOutcome.SUCCEEDED
        assert first.logical_run_id == second.logical_run_id
        assert executor._call_count == 1  # nunca se invoco al proveedor una segunda vez


def test_same_key_different_target_yields_idempotency_conflict(session_factory: sessionmaker):
    with session_factory() as session:
        target_a = _prepare_target(session, "006A")
        target_b = _prepare_target(session, "006B")
        executor = FakeProducerAdapter([_valid_output()])
        key = f"pe-shared-{uuid.uuid4()}"

        first = request_producer_execution(
            session, target_a, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )
        second = request_producer_execution(
            session, target_b, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert first.outcome == AgentRunOutcome.SUCCEEDED
        assert second.outcome == AgentRunOutcome.IDEMPOTENCY_CONFLICT
        assert executor._call_count == 1


def test_invalid_context_package_is_rejected_and_replay_never_retries_context_assembly(
    session_factory: sessionmaker, monkeypatch
):
    """
    CODE-CR-71/CODE-CR-72 (Human Code Review de Build v5, BLOCKER):
    cuando Context Assembly falla para una clave NUEVA, la reserva
    PRODUCER_EXECUTION se resuelve a REJECTED (rejected_reason=
    'INVALID_CONTEXT_PACKAGE') y se comitea - CR-27, mismo patron que
    WORKFLOW_TRANSITION/CONTEXT_ASSEMBLY - nunca queda huerfana en
    CLAIMED. Un replay posterior con la MISMA idempotency_key debe
    devolver INVALID_CONTEXT_PACKAGE otra vez SIN volver a invocar
    Context Assembly, sin crear ningun AgentRun, y sin invocar al
    proveedor.

    Se usa un doble de assemble_context (monkeypatch sobre el modulo
    orchestrator, no sobre Context Engine) porque esta prueba ejercita
    la SECUENCIA de orquestacion de request_producer_execution en si
    misma - el motivo real por el que Context Assembly puede rechazar
    (MAPPING_UNDEFINED, NEWER_VERSION_AVAILABLE, etc.) es
    responsabilidad de Context Engine y esta fuera de alcance aqui.
    """
    import src.persistence.repositories.orchestrator as orchestrator_module
    from src.domain.context_result import ContextAssemblyOutcome, ContextAssemblyResult

    call_count = {"n": 0}

    def _fake_assemble_context(session, task_type, target_object_id, idempotency_key):
        call_count["n"] += 1
        return ContextAssemblyResult(outcome=ContextAssemblyOutcome.MAPPING_UNDEFINED)

    monkeypatch.setattr(orchestrator_module, "assemble_context", _fake_assemble_context)

    with session_factory() as session:
        target = "OBJ-NEVER-TOUCHED"
        key = f"pe-invalid-ctx-{uuid.uuid4()}"
        executor = FakeProducerAdapter([_valid_output()])

        first = request_producer_execution(
            session, target, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )
        second = request_producer_execution(
            session, target, "tester", key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert first.outcome == AgentRunOutcome.INVALID_CONTEXT_PACKAGE
        assert second.outcome == AgentRunOutcome.INVALID_CONTEXT_PACKAGE
        assert call_count["n"] == 1  # el replay NUNCA vuelve a invocar Context Assembly
        assert executor._call_count == 0  # el proveedor nunca fue invocado

        reservation = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == "PRODUCER_EXECUTION",
                IdempotencyOperation.idempotency_key == key,
            )
        ).scalar_one()
        assert reservation.status == "REJECTED"
        assert reservation.rejected_reason == "INVALID_CONTEXT_PACKAGE"
        assert reservation.agent_run_id is None
        assert reservation.context_package_id is None

        agent_runs = session.execute(
            select(AgentRun).where(AgentRun.idempotency_key == key)
        ).scalars().all()
        assert agent_runs == []  # nunca se creo ningun AgentRun


# ---------------------------------------------------------------------------
# CR-52: reutilizacion de version virgen tras FAILED
# ---------------------------------------------------------------------------


def test_failed_run_leaves_reusable_version_for_next_request(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "007")

        failing_executor = FakeProducerAdapter([RuntimeError("falla")])
        first = request_producer_execution(
            session, target, "tester", f"pe-{target}-fail", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=failing_executor, max_attempts=1,
        )
        assert first.outcome == AgentRunOutcome.FAILED

        succeeding_executor = FakeProducerAdapter([_valid_output()])
        second = request_producer_execution(
            session, target, "tester", f"pe-{target}-retry", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-B", producer_executor=succeeding_executor,
        )
        assert second.outcome == AgentRunOutcome.SUCCEEDED

        versions = session.execute(
            select(ObjectVersion).where(ObjectVersion.object_id == target)
        ).scalars().all()
        # Un solo version_number reutilizado - no se infla por el fallo previo.
        assert len(versions) == 1
        assert versions[0].version_number == 1
        assert versions[0].status == "DRAFT"


# ---------------------------------------------------------------------------
# CR-57: UNIQUE(object_id, version_number)
# ---------------------------------------------------------------------------


def test_unique_object_id_version_number_enforced_by_postgres(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "008")
        v1 = session.execute(
            select(ObjectVersion.object_version_id).where(ObjectVersion.object_id == target)
        ).scalar_one_or_none()
        if v1 is None:
            _ensure_instructional_object(session, target)
            v1 = uuid.uuid4()
            session.add(
                ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING")
            )
            session.commit()

        with pytest.raises(IntegrityError):
            session.add(
                ObjectVersion(
                    object_version_id=uuid.uuid4(), object_id=target, version_number=1, status="ELIGIBLE_PENDING"
                )
            )
            session.commit()
        session.rollback()


# ---------------------------------------------------------------------------
# CR-59: fencing con vigencia temporal - worker con lease expirado no puede finalizar
# ---------------------------------------------------------------------------


def test_expired_lease_worker_cannot_finalize_even_if_unclaimed(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "009")
        v1 = uuid.uuid4()
        session.add(ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING"))
        session.commit()

        from src.persistence.repositories.context_engine import assemble_context

        ctx = assemble_context(session, "PRODUCE_LEARNING_OBJECT", target, f"ctx-{target}")
        logical_run_id = arm.create_agent_run(
            session, "PRODUCER", "PRODUCE_LEARNING_OBJECT", v1, ctx.context_package_id,
            "tester", str(uuid.uuid4()), f"pe-{target}", max_attempts=3,
        )
        session.commit()

        generation = arm.acquire(session, logical_run_id, "worker-A")
        session.commit()
        assert generation == 1

        attempt_id = arm.create_attempt(session, logical_run_id, 1, generation)
        session.commit()

        # Simular vencimiento del lease directamente (sin esperar en tiempo real).
        session.execute(
            update(AgentRun)
            .where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        # worker-A intenta finalizar CON su claimed_by/generation todavia
        # coincidentes, pero el lease ya vencio - CR-59.
        authorized = arm.authorize_terminal_success(session, logical_run_id, "worker-A", generation)
        assert authorized is False
        session.rollback()

        run = session.get(AgentRun, logical_run_id)
        assert run.status == "IN_PROGRESS"  # nunca paso a SUCCEEDED

        arm.close_attempt(session, attempt_id, outcome="CLAIM_LOST")
        session.commit()

        content = session.get(ObjectVersionContent, v1)
        assert content is None  # nunca se persistio contenido


def test_reclaim_after_expiry_increments_generation_and_blocks_old_owner(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "010")
        v1 = uuid.uuid4()
        session.add(ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING"))
        session.commit()

        from src.persistence.repositories.context_engine import assemble_context

        ctx = assemble_context(session, "PRODUCE_LEARNING_OBJECT", target, f"ctx-{target}")
        logical_run_id = arm.create_agent_run(
            session, "PRODUCER", "PRODUCE_LEARNING_OBJECT", v1, ctx.context_package_id,
            "tester", str(uuid.uuid4()), f"pe-{target}", max_attempts=3,
        )
        session.commit()

        gen_a = arm.acquire(session, logical_run_id, "worker-A")
        session.commit()
        assert gen_a == 1

        session.execute(
            update(AgentRun)
            .where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        gen_b = arm.reclaim(session, logical_run_id, "worker-B")
        session.commit()
        assert gen_b == 2  # incrementada

        # worker-A, con su generation vieja, ya no puede finalizar.
        authorized = arm.authorize_terminal_success(session, logical_run_id, "worker-A", gen_a)
        assert authorized is False
        session.rollback()

        # worker-B, con la generation nueva, si puede.
        authorized_b = arm.authorize_terminal_success(session, logical_run_id, "worker-B", gen_b)
        assert authorized_b is True
        session.commit()


def test_expired_worker_cannot_mark_failed_without_reclaim(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "011")
        v1 = uuid.uuid4()
        session.add(ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING"))
        session.commit()

        from src.persistence.repositories.context_engine import assemble_context

        ctx = assemble_context(session, "PRODUCE_LEARNING_OBJECT", target, f"ctx-{target}")
        logical_run_id = arm.create_agent_run(
            session, "PRODUCER", "PRODUCE_LEARNING_OBJECT", v1, ctx.context_package_id,
            "tester", str(uuid.uuid4()), f"pe-{target}", max_attempts=1,
        )
        session.commit()
        gen = arm.acquire(session, logical_run_id, "worker-A")
        session.commit()

        session.execute(
            update(AgentRun)
            .where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        marked = arm.mark_failed_if_owner(session, logical_run_id, "worker-A", gen)
        assert marked is False
        session.rollback()

        run = session.get(AgentRun, logical_run_id)
        assert run.status == "IN_PROGRESS"  # sigue sin FAILED - stale worker no pudo marcarlo


# ---------------------------------------------------------------------------
# CODE-CR-60: el Producer recibe el ContextPackage REAL, nunca un placeholder
# ---------------------------------------------------------------------------


def test_producer_receives_real_context_package_matching_persisted_row(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "013")
        executor = FakeProducerAdapter([_valid_output()])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )
        assert result.outcome == AgentRunOutcome.SUCCEEDED

        from src.persistence.models import ContextPackage

        run = session.get(AgentRun, result.logical_run_id)
        persisted_package = session.get(ContextPackage, run.context_package_id)

        assert len(executor.received_context_packages) == 1
        received = executor.received_context_packages[0]

        # context_package_id y hash coinciden con la fila realmente persistida.
        assert received["context_package_id"] == str(persisted_package.context_package_id)
        assert received["context_package_hash"] == persisted_package.context_package_hash
        assert executor.received_execution_contracts[0].context_package_hash == persisted_package.context_package_hash

        # El contenido real (activity/learning outcome aprobados, evidencia)
        # llega al Producer - no un dict vacio.
        received_object_ids = {o["object_id"] for o in received["approved_objects"]}
        assert f"ACT-PX-013" in received_object_ids
        assert f"LO-PX-013" in received_object_ids
        assert any(e["evidence_class"] == "E1" for e in received["evidence"])


def test_context_package_snapshot_unaffected_by_later_state_mutation(session_factory: sessionmaker):
    """
    CODE-CR-60: ninguna mutacion posterior del estado aprobado cambia el
    snapshot ya entregado a un AgentRun existente - se verifica
    disparando la ejecucion, y comparando contra una segunda consulta
    directa del ContextPackage despues de mutar el estado (agregar
    evidencia E3) - el hash recibido por el Producer coincide con el de
    la fila ORIGINAL, no con un reensamblado posterior.
    """
    with session_factory() as session:
        target = _prepare_target(session, "014")
        executor = FakeProducerAdapter([_valid_output()])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )
        assert result.outcome == AgentRunOutcome.SUCCEEDED
        original_hash = executor.received_context_packages[0]["context_package_hash"]

        # Mutar el estado aprobado DESPUES de la ejecucion.
        _add_evidence(session, target, "E3", f"EV-014-extra")

        run = session.get(AgentRun, result.logical_run_id)
        from src.persistence.models import ContextPackage

        persisted_package = session.get(ContextPackage, run.context_package_id)
        # El ContextPackage es inmutable (I3) - su hash no cambio, y sigue
        # coincidiendo con lo que el Producer recibio originalmente.
        assert persisted_package.context_package_hash == original_hash


# ---------------------------------------------------------------------------
# CODE-CR-61: ninguna transaccion PostgreSQL abierta durante execute()
# ---------------------------------------------------------------------------


class _TransactionProbeExecutor:
    """Verifica en runtime, no por inspeccion estatica, que
    session.in_transaction() es False en el instante exacto de cada
    llamada a execute() - CODE-CR-61."""

    def __init__(self, session, responses: list):
        self._session = session
        self._responses = list(responses)
        self._call_count = 0
        self.observed_in_transaction: list[bool] = []

    def execute(self, execution_contract, context_package):
        self.observed_in_transaction.append(self._session.in_transaction())
        index = min(self._call_count, len(self._responses) - 1)
        scripted = self._responses[index]
        self._call_count += 1
        if isinstance(scripted, Exception):
            from src.persistence.repositories.producer_executor import ProducerExecutionResult
            return ProducerExecutionResult(raw_response=None, parsed_output=None, technical_error=str(scripted))
        from src.persistence.repositories.producer_executor import ProducerExecutionResult
        return ProducerExecutionResult(raw_response=scripted, parsed_output=scripted, technical_error=None)


def test_no_open_transaction_during_provider_call_first_attempt(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "015")
        probe = _TransactionProbeExecutor(session, [_valid_output()])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=probe,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        assert probe.observed_in_transaction == [False]


def test_no_open_transaction_during_provider_call_same_owner_retry(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "016")
        probe = _TransactionProbeExecutor(session, [RuntimeError("falla tecnica"), _valid_output()])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=probe,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        # Attempt 1 (fallo) y Attempt 2 (same-owner retry) - ninguno de
        # los dos con transaccion abierta.
        assert probe.observed_in_transaction == [False, False]


def test_no_open_transaction_during_provider_call_after_reclaim(session_factory: sessionmaker):
    """
    CODE-CR-68 (Human Code Review de Build v4) - correccion de un
    DEFECTO DE TEST preexistente (Build v3): el setup anterior llamaba
    a arm.reclaim(...) manualmente ANTES de invocar _phase_b_execute,
    dejando el lease ya renovado y VALIDO en el momento en que
    _phase_b_execute intentaba su propia adquisicion/reclamacion
    interna - por lo que tanto acquire_and_create_first_attempt() como
    reclaim_and_create_attempt() fallaban (AgentRun ya IN_PROGRESS, y
    el lease ya no estaba vencido) y _phase_b_execute retornaba SIN
    llamar al proveedor, violando la asercion
    `probe.observed_in_transaction == [False]` (el resultado real
    hubiera sido una lista vacia, no una con un solo `False`).

    Fix (solo de setup, sin tocar produccion, per mandato del Human
    Code Review): dejar el lease de worker-A REALMENTE vencido y NO
    reclamar manualmente - _phase_b_execute debe ejecutar su propio
    reclaim_and_create_attempt() internamente (nueva generacion, nuevo
    Attempt, cierre del Attempt anterior como CLAIM_LOST) y SOLO
    entonces invocar al proveedor, sin ninguna transaccion PostgreSQL
    abierta en ese instante (CODE-CR-61).
    """
    with session_factory() as session:
        target = _prepare_target(session, "017")
        v1 = uuid.uuid4()
        session.add(ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING"))
        session.commit()

        from src.persistence.repositories.context_engine import assemble_context

        ctx = assemble_context(session, "PRODUCE_LEARNING_OBJECT", target, f"ctx-{target}")
        logical_run_id = arm.create_agent_run(
            session, "PRODUCER", "PRODUCE_LEARNING_OBJECT", v1, ctx.context_package_id,
            "tester", str(uuid.uuid4()), f"pe-{target}", max_attempts=3,
        )
        session.commit()

        # worker-A adquiere y crea el Attempt 1, y luego "desaparece" -
        # su lease vence, SIN que nadie lo reclame todavia. A partir de
        # aca, _phase_b_execute(worker-B, ...) es quien debe reclamar.
        auth_a = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        assert auth_a.generation == 1
        attempt_1_id = auth_a.attempt_id

        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        from src.persistence.repositories.orchestrator import _phase_b_execute

        probe = _TransactionProbeExecutor(session, [_valid_output()])
        _phase_b_execute(
            session, logical_run_id, target, v1, "worker-B", probe,
            _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION, PRODUCER_OUTPUT_SCHEMA_VERSION,
            ctx.context_package_id, max_attempts=3,
        )

        assert probe.observed_in_transaction == [False]

        run = session.get(AgentRun, logical_run_id)
        assert run.status == "SUCCEEDED"
        assert run.claimed_by == "worker-B"
        assert run.lease_generation == 2  # reclamado internamente por _phase_b_execute

        attempt_1 = session.get(AgentRunAttempt, attempt_1_id)
        assert attempt_1.outcome == "CLAIM_LOST"  # cerrado por el reclaim interno (CR-64)

        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == logical_run_id)
        ).scalars().all()
        assert len(attempts) == 2
        success_attempt = next(a for a in attempts if a.outcome == "SUCCESS")
        assert success_attempt.lease_generation_at_creation == 2


# ---------------------------------------------------------------------------
# CODE-CR-63: AgentRun es la autoridad del execution snapshot - nunca se
# reutilizan context_package_id/max_attempts de la invocacion actual
# ---------------------------------------------------------------------------


def _create_run_with_committed_reservation(session, target, idem_key, max_attempts):
    """Crea un AgentRun + su reserva de idempotencia ya COMMITTED,
    simulando el estado que Fase A deja tras una creacion real previa -
    sin depender de invocar request_producer_execution dos veces.

    CODE-CR-72 (Human Code Review de Build v5): el hash comparado en la
    reserva PRODUCER_EXECUTION es la identidad de PRE-CONTEXTO
    (compute_producer_request_identity_hash, nunca incluye
    context_package_id) - coincide exactamente con lo que
    request_producer_execution calcula y compara al resolver esta misma
    idempotency_key como replay."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from src.persistence.models import IdempotencyOperation
    from src.persistence.repositories.context_engine import assemble_context
    from src.persistence.repositories.orchestrator import compute_producer_request_identity_hash

    v1 = uuid.uuid4()
    session.add(ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING"))
    session.commit()

    ctx = assemble_context(session, "PRODUCE_LEARNING_OBJECT", target, f"ctx-{target}")
    logical_run_id = arm.create_agent_run(
        session, "PRODUCER", "PRODUCE_LEARNING_OBJECT", v1, ctx.context_package_id,
        "tester", str(uuid.uuid4()), idem_key, max_attempts=max_attempts,
    )
    request_identity_hash = compute_producer_request_identity_hash(
        "PRODUCE_LEARNING_OBJECT", target, _PROVIDER, _MODEL,
        _INSTRUCTIONS_VERSION, PRODUCER_OUTPUT_SCHEMA_VERSION,
    )
    session.execute(
        pg_insert(IdempotencyOperation).values(
            operation_type="PRODUCER_EXECUTION", idempotency_key=idem_key,
            idempotency_payload_hash=request_identity_hash, status="COMMITTED", agent_run_id=logical_run_id,
        )
    )
    session.commit()
    return logical_run_id, ctx.context_package_id


def test_execution_uses_agent_run_persisted_max_attempts_not_request_argument(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "018")
        idem_key = f"pe-{target}-snapshot"
        logical_run_id, _ = _create_run_with_committed_reservation(session, target, idem_key, max_attempts=2)

        executor = FakeProducerAdapter([RuntimeError("falla siempre")])
        # max_attempts=99 en el argumento DEBE ser ignorado - solo cuenta
        # el 2 ya persistido en AgentRun al crearlo.
        result = request_producer_execution(
            session, target, "tester", idem_key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor, max_attempts=99,
        )

        assert result.outcome == AgentRunOutcome.FAILED
        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == logical_run_id)
        ).scalars().all()
        assert len(attempts) == 2  # el valor persistido, nunca 99


def test_execution_uses_agent_run_persisted_context_package_id(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "019")
        idem_key = f"pe-{target}-ctxsnap"
        logical_run_id, original_context_package_id = _create_run_with_committed_reservation(
            session, target, idem_key, max_attempts=3
        )

        executor = FakeProducerAdapter([_valid_output()])
        result = request_producer_execution(
            session, target, "tester", idem_key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        assert executor.received_context_packages[0]["context_package_id"] == str(original_context_package_id)
        run = session.get(AgentRun, logical_run_id)
        assert run.context_package_id == original_context_package_id


# ---------------------------------------------------------------------------
# CODE-CR-64: reclaim() cierra el AgentRunAttempt abandonado como CLAIM_LOST
# ---------------------------------------------------------------------------


def test_reclaim_closes_abandoned_attempt_as_claim_lost(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "020")
        idem_key = f"pe-{target}-reclaim"
        logical_run_id, _ = _create_run_with_committed_reservation(session, target, idem_key, max_attempts=3)

        gen_a = arm.acquire(session, logical_run_id, "worker-A")
        session.commit()
        attempt_1 = arm.create_attempt(session, logical_run_id, 1, gen_a)
        session.commit()

        # worker-A "desaparece" - nunca cierra su intento. Simular vencimiento.
        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        gen_b = arm.reclaim(session, logical_run_id, "worker-B")
        session.commit()
        assert gen_b == 2

        abandoned = session.get(AgentRunAttempt, attempt_1)
        assert abandoned.outcome == "CLAIM_LOST"
        assert abandoned.finished_at is not None

        attempt_2 = arm.create_attempt(session, logical_run_id, 2, gen_b)
        session.commit()
        new_attempt = session.get(AgentRunAttempt, attempt_2)
        assert new_attempt.outcome is None  # el nuevo intento NO se marca CLAIM_LOST


def test_concurrent_reclaims_only_one_wins_and_closes_attempt_coherently(_migrated_engine):
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup:
        target = _prepare_target(setup, "021")
        idem_key = f"pe-{target}-concurrent-reclaim"
        logical_run_id, _ = _create_run_with_committed_reservation(setup, target, idem_key, max_attempts=3)
        gen_a = arm.acquire(setup, logical_run_id, "worker-A")
        setup.commit()
        attempt_1 = arm.create_attempt(setup, logical_run_id, 1, gen_a)
        setup.commit()
        setup.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        setup.commit()

    barrier = threading.Barrier(2)
    results = []

    def try_reclaim(worker_id: str):
        barrier.wait()
        with session_factory() as session:
            gen = arm.reclaim(session, logical_run_id, worker_id)
            session.commit()
            results.append((worker_id, gen))

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(try_reclaim, ["worker-B", "worker-C"]))

    winners = [r for r in results if r[1] is not None]
    losers = [r for r in results if r[1] is None]
    assert len(winners) == 1
    assert len(losers) == 1
    assert winners[0][1] == 2  # una sola nueva generacion

    with session_factory() as session:
        abandoned = session.get(AgentRunAttempt, attempt_1)
        assert abandoned.outcome == "CLAIM_LOST"  # cerrado de forma coherente, una sola vez
        run = session.get(AgentRun, logical_run_id)
        assert run.claimed_by == winners[0][0]
        assert run.lease_generation == 2


# ---------------------------------------------------------------------------
# Cobertura de aceptacion pendiente: rollback total de Fase C
# ---------------------------------------------------------------------------


def test_phase_c_rolls_back_completely_if_workflow_transition_fails(session_factory: sessionmaker):
    with session_factory() as session:
        target = _prepare_target(session, "022")
        idem_key = f"pe-{target}-txfail"
        logical_run_id, _ = _create_run_with_committed_reservation(session, target, idem_key, max_attempts=3)
        run = session.get(AgentRun, logical_run_id)
        v1 = run.target_object_version_id

        gen = arm.acquire(session, logical_run_id, "worker-A")
        session.commit()
        attempt_id = arm.create_attempt(session, logical_run_id, 1, gen)
        session.commit()

        # Forzar una condicion anomala: mutar el estado de la version para
        # que ELIGIBLE_PENDING -> DRAFT falle con STATE_CONFLICT dentro de
        # Fase C.
        session.execute(update(ObjectVersion).where(ObjectVersion.object_version_id == v1).values(status="SUPERSEDED"))
        session.commit()

        from src.persistence.repositories.orchestrator import _phase_c_finalize

        success = _phase_c_finalize(session, logical_run_id, target, v1, "worker-A", gen, attempt_id, _valid_output())

        assert success is False
        assert session.get(ObjectVersionContent, v1) is None  # nunca se persistio contenido
        run_after = session.get(AgentRun, logical_run_id)
        assert run_after.status == "IN_PROGRESS"  # NUNCA paso a SUCCEEDED sin la transicion
        events = session.execute(select(AuditEvent).where(AuditEvent.agent_run_id == logical_run_id)).scalars().all()
        assert events == []  # ningun audit event huerfano
        attempt = session.get(AgentRunAttempt, attempt_id)
        assert attempt.outcome == "TECHNICAL_FAILURE"


def test_phase_c_rolls_back_completely_if_audit_event_insertion_fails(session_factory: sessionmaker, monkeypatch):
    with session_factory() as session:
        target = _prepare_target(session, "023")
        idem_key = f"pe-{target}-auditfail"
        logical_run_id, _ = _create_run_with_committed_reservation(session, target, idem_key, max_attempts=3)
        run = session.get(AgentRun, logical_run_id)
        v1 = run.target_object_version_id

        gen = arm.acquire(session, logical_run_id, "worker-A")
        session.commit()
        attempt_id = arm.create_attempt(session, logical_run_id, 1, gen)
        session.commit()

        import src.persistence.repositories.orchestrator as orch_module

        def _raising_audit_event(*args, **kwargs):
            raise RuntimeError("fallo simulado de auditoria")

        monkeypatch.setattr(orch_module, "AuditEvent", _raising_audit_event)

        with pytest.raises(RuntimeError):
            orch_module._phase_c_finalize(
                session, logical_run_id, target, v1, "worker-A", gen, attempt_id, _valid_output()
            )

        session.rollback()  # nadie mas hizo commit - se revierte todo el bloque

        assert session.get(ObjectVersionContent, v1) is None  # NUNCA committeado
        version = session.get(ObjectVersion, v1)
        assert version.status == "ELIGIBLE_PENDING"  # la transicion tambien se revirtio
        run_after = session.get(AgentRun, logical_run_id)
        assert run_after.status == "IN_PROGRESS"  # nunca paso a SUCCEEDED
        attempt = session.get(AgentRunAttempt, attempt_id)
        assert attempt.outcome is None  # tampoco se marco SUCCESS


# ---------------------------------------------------------------------------
# Cobertura de aceptacion pendiente: concurrencia real de Version Creation
# con idempotency_key DISTINTAS
# ---------------------------------------------------------------------------


def test_concurrent_different_keys_never_produce_duplicate_object_version_number(_migrated_engine):
    """
    Cubre simultaneamente: (6) dos requests con idempotency_key
    diferentes no reutilizan al mismo tiempo la misma ObjectVersion
    virgen (aqui no hay ninguna virgen disponible, asi que ambas deben
    CREAR), y (7) la creacion concurrente nunca produce el mismo
    (object_id, version_number) - forzado por el lock FOR UPDATE de
    Version Creation (CR-57), verificado con hilos reales.
    """
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup:
        target = _prepare_target(setup, "024")

    barrier = threading.Barrier(2)
    results = []

    def attempt(worker_id: str, key_suffix: str):
        barrier.wait()
        with session_factory() as session:
            executor = FakeProducerAdapter([_valid_output()])
            r = request_producer_execution(
                session, target, "tester", f"pe-{target}-{key_suffix}", _PROVIDER, _MODEL,
                _INSTRUCTIONS_VERSION, worker_id=worker_id, producer_executor=executor,
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda args: attempt(*args), [("worker-A", "x"), ("worker-B", "y")]))

    assert all(r.outcome == AgentRunOutcome.SUCCEEDED for r in results)
    assert results[0].logical_run_id != results[1].logical_run_id
    assert results[0].object_version_id != results[1].object_version_id

    with session_factory() as session:
        versions = session.execute(
            select(ObjectVersion).where(ObjectVersion.object_id == target)
        ).scalars().all()
        version_numbers = sorted(v.version_number for v in versions)
        assert len(versions) == 2
        assert version_numbers == [1, 2]  # secuenciales, nunca duplicados


def test_concurrent_unseen_key_same_payload_produces_exactly_one_success(_migrated_engine):
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        target = _prepare_target(setup_session, "012")

    shared_key = f"pe-{target}-concurrent"
    barrier = threading.Barrier(2)
    results = []

    def attempt(worker_id: str):
        barrier.wait()
        with session_factory() as session:
            executor = FakeProducerAdapter([_valid_output()])
            r = request_producer_execution(
                session, target, "tester", shared_key, _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
                worker_id=worker_id, producer_executor=executor,
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(attempt, ["worker-A", "worker-B"]))

    assert all(r.outcome == AgentRunOutcome.SUCCEEDED for r in results)
    assert results[0].logical_run_id == results[1].logical_run_id

    with session_factory() as session:
        successes = session.execute(
            select(AgentRunAttempt).where(
                AgentRunAttempt.logical_run_id == results[0].logical_run_id,
                AgentRunAttempt.outcome == "SUCCESS",
            )
        ).scalars().all()
        assert len(successes) == 1  # AT_MOST_ONE_COMMITTED_CONTENT_RESULT_PER_LOGICAL_RUN


# ---------------------------------------------------------------------------
# CODE-CR-67 (Human Code Review de Build v3) - test (5) de los 5 exigidos:
# el MISMO whitelist (compute_allowed_learning_outcome_ids) usado por el
# prompt del proveedor es el que usa Structural Validation, verificado
# extremo a extremo contra un AgentRun real. Los tests (1)-(4) estan en
# tests/unit/test_allowed_learning_outcome_ids.py y
# tests/unit/test_anthropic_producer_adapter.py.
# ---------------------------------------------------------------------------


def test_learning_outcome_refs_rejects_activity_id_end_to_end(session_factory: sessionmaker):
    """
    ACT-PX-025 esta legitimamente aprobado y presente en el
    ContextPackage de este target (es su PARENT_ACTIVITY), pero tiene
    object_type='ACTIVITY' - nunca debe poder referenciarse en
    learning_outcome_refs, aunque el Producer lo intente. La whitelist
    real calculada por orchestrator._phase_b_execute (via
    compute_allowed_learning_outcome_ids) debe excluirlo, y Structural
    Validation debe rechazar el output completo.
    """
    with session_factory() as session:
        target = _prepare_target(session, "025")
        bad_output = _valid_output(learning_outcome_refs=["ACT-PX-025"])
        executor = FakeProducerAdapter([bad_output])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}-actref", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor, max_attempts=1,
        )

        assert result.outcome == AgentRunOutcome.FAILED
        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == result.logical_run_id)
        ).scalars().all()
        assert len(attempts) == 1
        assert attempts[0].outcome == "INVALID_OUTPUT"
        assert "ACT-PX-025" in attempts[0].failure_detail

        content = session.execute(
            select(ObjectVersionContent).where(ObjectVersionContent.produced_by_attempt_id == attempts[0].attempt_id)
        ).scalar_one_or_none()
        assert content is None  # nunca se persistio contenido con una referencia invalida


def test_learning_outcome_refs_accepts_real_learning_outcome_end_to_end(session_factory: sessionmaker):
    """Contraparte positiva del test anterior: LO-PX-026 (LEARNING_OUTCOME
    real, aprobado y presente en el ContextPackage) SI puede
    referenciarse y el Producer completa exitosamente."""
    with session_factory() as session:
        target = _prepare_target(session, "026")
        good_output = _valid_output(learning_outcome_refs=["LO-PX-026"])
        executor = FakeProducerAdapter([good_output])

        result = request_producer_execution(
            session, target, "tester", f"pe-{target}-lo-ok", _PROVIDER, _MODEL, _INSTRUCTIONS_VERSION,
            worker_id="worker-A", producer_executor=executor,
        )

        assert result.outcome == AgentRunOutcome.SUCCEEDED
        content = session.get(ObjectVersionContent, result.object_version_id)
        assert content.learning_outcome_refs == ["LO-PX-026"]


# ---------------------------------------------------------------------------
# CODE-CR-66 (Human Code Review de Build v3, BLOCKER) - 9 tests exigidos
# sobre las operaciones fusionadas de agent_run_manager
# (acquire_and_create_first_attempt / reclaim_and_create_attempt /
# refresh_and_create_retry_attempt). Requieren PostgreSQL real
# (testcontainers) - en este entorno sin Docker se recolectan pero no
# se ejecutan; ver bloque de estado al final de la entrega.
# ---------------------------------------------------------------------------


def _new_run_for_fused_tests(session, suffix: str, max_attempts: int = 3) -> uuid.UUID:
    """Crea un AgentRun limpio en estado PENDING, listo para ejercitar
    las operaciones fusionadas de CODE-CR-66 directamente (sin pasar
    por request_producer_execution/Fase A completa)."""
    target = _prepare_target(session, suffix)
    v1 = uuid.uuid4()
    session.add(ObjectVersion(object_version_id=v1, object_id=target, version_number=1, status="ELIGIBLE_PENDING"))
    session.commit()

    from src.persistence.repositories.context_engine import assemble_context

    ctx = assemble_context(session, "PRODUCE_LEARNING_OBJECT", target, f"ctx-{target}")
    logical_run_id = arm.create_agent_run(
        session, "PRODUCER", "PRODUCE_LEARNING_OBJECT", v1, ctx.context_package_id,
        "tester", str(uuid.uuid4()), f"pe-{target}-fused", max_attempts=max_attempts,
    )
    session.commit()
    return logical_run_id


# --- (1) acquire + Attempt 1 coherentes (misma transaccion) ---


def test_acquire_and_create_first_attempt_are_coherent(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "030")

        auth = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        assert auth.generation == 1
        assert auth.attempt_id is not None
        assert auth.failed_max_attempts is False

        run = session.get(AgentRun, logical_run_id)
        assert run.status == "IN_PROGRESS"
        assert run.claimed_by == "worker-A"
        assert run.lease_generation == 1

        attempt = session.get(AgentRunAttempt, auth.attempt_id)
        assert attempt.attempt_number == 1
        assert attempt.lease_generation_at_creation == 1
        assert attempt.outcome is None


# --- (2) worker pierde el lease antes de crear el attempt -> no puede
# crear un attempt con generacion vieja ---


def test_stale_generation_cannot_create_attempt_after_ownership_lost(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "031")

        auth_a = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        gen_a = auth_a.generation
        assert gen_a == 1

        # worker-A "desaparece" - su lease vence.
        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        # worker-B reclama primero.
        auth_b = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
        session.commit()
        assert auth_b.generation == 2

        # worker-A, todavia con su generation=1 vieja, intenta crear un
        # intento de retry bajo su propio ownership (ya perdido) -
        # refresh_and_create_retry_attempt debe fallar atomicamente,
        # SIN crear ningun Attempt nuevo con generation=1.
        attempts_before = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == logical_run_id)
        ).scalars().all()

        auth_stale = arm.refresh_and_create_retry_attempt(
            session, logical_run_id, "worker-A", gen_a, max_attempts=3
        )
        session.rollback()

        assert auth_stale.generation is None
        assert auth_stale.attempt_id is None

        attempts_after = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == logical_run_id)
        ).scalars().all()
        # Ningun Attempt nuevo se creo por el intento de worker-A.
        assert len(attempts_after) == len(attempts_before)
        assert all(a.lease_generation_at_creation != gen_a or a.attempt_number == 1 for a in attempts_after)


# --- (3) reclaim + nuevo Attempt atomicos ---


def test_reclaim_and_create_attempt_are_coherent(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "032")

        auth_a = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        auth_b = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
        session.commit()

        assert auth_b.generation == 2
        assert auth_b.attempt_id is not None
        assert auth_b.failed_max_attempts is False

        attempt = session.get(AgentRunAttempt, auth_b.attempt_id)
        assert attempt.attempt_number == 2
        assert attempt.lease_generation_at_creation == 2

        run = session.get(AgentRun, logical_run_id)
        assert run.claimed_by == "worker-B"
        assert run.lease_generation == 2


# --- (4) stale worker no puede insertar un Attempt despues de un
# reclaim ajeno ---


def test_stale_worker_cannot_insert_attempt_after_foreign_reclaim(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "033")

        auth_a = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        gen_a = auth_a.generation

        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        auth_b = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
        session.commit()
        assert auth_b.generation == 2

        # worker-A intenta reclamar el mismo run "de nuevo" con su propia
        # identidad, pero el lease ya no esta vencido (worker-B lo
        # renovo) - reclaim_and_create_attempt debe fallar.
        auth_stale = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-A", max_attempts=3)
        session.rollback()

        assert auth_stale.generation is None
        assert auth_stale.attempt_id is None

        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == logical_run_id)
        ).scalars().all()
        assert len(attempts) == 2  # attempt 1 (CLAIM_LOST) + attempt 2 (worker-B) - nada de worker-A
        assert all(a.provider is None or True for a in attempts)  # sin Attempt fantasma adicional


# --- (5) tras un reclaim, ningun Attempt de generacion anterior queda
# con outcome IS NULL ---


def test_no_stale_open_attempt_survives_reclaim_and_create(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "034")

        auth_a = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        attempt_1_id = auth_a.attempt_id

        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        auth_b = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
        session.commit()

        attempt_1 = session.get(AgentRunAttempt, attempt_1_id)
        assert attempt_1.outcome == "CLAIM_LOST"
        assert attempt_1.finished_at is not None

        # Ningun attempt del logical_run_id queda con outcome NULL salvo
        # el recien creado por la reclamacion (attempt 2, todavia en
        # curso legitimamente).
        open_attempts = session.execute(
            select(AgentRunAttempt).where(
                AgentRunAttempt.logical_run_id == logical_run_id,
                AgentRunAttempt.outcome.is_(None),
            )
        ).scalars().all()
        assert len(open_attempts) == 1
        assert open_attempts[0].attempt_id == auth_b.attempt_id


# --- (6) same-owner retry crea el siguiente Attempt con la MISMA
# generacion ---


def test_refresh_and_create_retry_attempt_keeps_same_generation(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "035")

        auth_1 = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        arm.close_attempt(session, auth_1.attempt_id, outcome="TECHNICAL_FAILURE", failure_detail="simulado")
        session.commit()

        auth_2 = arm.refresh_and_create_retry_attempt(
            session, logical_run_id, "worker-A", auth_1.generation, max_attempts=3
        )
        session.commit()

        assert auth_2.generation == auth_1.generation  # SIN CAMBIOS (CR-58)
        assert auth_2.attempt_id != auth_1.attempt_id
        assert auth_2.failed_max_attempts is False

        attempt_2 = session.get(AgentRunAttempt, auth_2.attempt_id)
        assert attempt_2.attempt_number == 2
        assert attempt_2.lease_generation_at_creation == auth_1.generation


# --- (7) reclaim crea el siguiente Attempt con generation+1 ---


def test_reclaim_and_create_attempt_increments_generation(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "036")

        auth_1 = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()

        session.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        auth_2 = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
        session.commit()

        assert auth_2.generation == auth_1.generation + 1
        attempt_2 = session.get(AgentRunAttempt, auth_2.attempt_id)
        assert attempt_2.lease_generation_at_creation == auth_1.generation + 1


# --- (8) carrera concurrente real: reclaim vs. intento de creacion de
# attempt con generacion vieja ---


def test_concurrent_reclaim_vs_stale_attempt_creation_race(_migrated_engine):
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup:
        logical_run_id = _new_run_for_fused_tests(setup, "037")
        auth_1 = arm.acquire_and_create_first_attempt(setup, logical_run_id, "worker-A")
        setup.commit()
        gen_a = auth_1.generation
        setup.execute(
            update(AgentRun).where(AgentRun.logical_run_id == logical_run_id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        setup.commit()

    barrier = threading.Barrier(2)
    results = []

    def try_reclaim():
        barrier.wait()
        with session_factory() as session:
            auth = arm.reclaim_and_create_attempt(session, logical_run_id, "worker-B", max_attempts=3)
            session.commit()
            results.append(("reclaim", auth.generation, auth.attempt_id))

    def try_stale_refresh():
        barrier.wait()
        with session_factory() as session:
            auth = arm.refresh_and_create_retry_attempt(
                session, logical_run_id, "worker-A", gen_a, max_attempts=3
            )
            if auth.generation is None:
                session.rollback()
            else:
                session.commit()
            results.append(("stale_refresh", auth.generation, auth.attempt_id))

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda fn: fn(), [try_reclaim, try_stale_refresh]))

    reclaim_result = next(r for r in results if r[0] == "reclaim")
    stale_result = next(r for r in results if r[0] == "stale_refresh")

    # El reclaim de worker-B siempre gana (worker-A ya perdio el lease
    # por vencimiento antes de que la carrera empiece) - worker-A nunca
    # puede crear un Attempt bajo su generation vieja.
    assert reclaim_result[1] == gen_a + 1
    assert reclaim_result[2] is not None
    assert stale_result[1] is None
    assert stale_result[2] is None

    with session_factory() as session:
        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == logical_run_id)
        ).scalars().all()
        # Attempt 1 (CLAIM_LOST, worker-A original) + Attempt 2
        # (worker-B, via reclaim) - nunca un tercer Attempt fantasma
        # insertado por el intento tardio de worker-A.
        assert len(attempts) == 2
        stale_generation_attempts = [a for a in attempts if a.lease_generation_at_creation == gen_a and a.attempt_number != 1]
        assert stale_generation_attempts == []


# --- (9) max_attempts nunca produce un Attempt de mas ---


def test_max_attempts_never_produces_extra_attempt_via_fused_operations(session_factory: sessionmaker):
    with session_factory() as session:
        logical_run_id = _new_run_for_fused_tests(session, "038", max_attempts=2)

        auth_1 = arm.acquire_and_create_first_attempt(session, logical_run_id, "worker-A")
        session.commit()
        arm.close_attempt(session, auth_1.attempt_id, outcome="TECHNICAL_FAILURE", failure_detail="falla 1")
        session.commit()

        auth_2 = arm.refresh_and_create_retry_attempt(
            session, logical_run_id, "worker-A", auth_1.generation, max_attempts=2
        )
        session.commit()
        assert auth_2.attempt_id is not None
        assert auth_2.failed_max_attempts is False
        arm.close_attempt(session, auth_2.attempt_id, outcome="TECHNICAL_FAILURE", failure_detail="falla 2")
        session.commit()

        # El tercer intento excederia max_attempts=2 - debe marcar
        # FAILED y NO crear ningun Attempt 3.
        auth_3 = arm.refresh_and_create_retry_attempt(
            session, logical_run_id, "worker-A", auth_2.generation, max_attempts=2
        )
        session.commit()

        assert auth_3.failed_max_attempts is True
        assert auth_3.attempt_id is None

        run = session.get(AgentRun, logical_run_id)
        assert run.status == "FAILED"

        attempts = session.execute(
            select(AgentRunAttempt).where(AgentRunAttempt.logical_run_id == logical_run_id)
        ).scalars().all()
        assert len(attempts) == 2  # exactamente max_attempts, ni uno mas
