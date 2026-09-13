from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from src.domain.transition_result import TransitionOutcome
from src.persistence.models import IdempotencyOperation, InstructionalObject, ObjectVersion, WorkflowTransition
from src.persistence.repositories.workflow_transition import request_workflow_transition

_TASK_TYPE = "PRODUCE_LEARNING_OBJECT"


def _ensure_instructional_object(session, object_id: str) -> None:
    """CODE-CR-42: sin trigger; InstructionalObject primero, ObjectVersion despues."""
    if session.get(InstructionalObject, object_id) is None:
        session.add(InstructionalObject(object_id=object_id, object_type=None))
        session.flush()


def _create_object_version(session, object_id: str, status: str) -> uuid.UUID:
    _ensure_instructional_object(session, object_id)
    version_id = uuid.uuid4()
    session.add(
        ObjectVersion(
            object_version_id=version_id,
            object_id=object_id,
            version_number=1,
            status=status,
        )
    )
    session.commit()
    return version_id


def _get_status(session, object_version_id: uuid.UUID) -> str:
    return session.execute(
        select(ObjectVersion.status).where(ObjectVersion.object_version_id == object_version_id)
    ).scalar_one()


# ---------------------------------------------------------------------------
# 1-2: transicion valida e invalida
# ---------------------------------------------------------------------------


def test_valid_eligible_pending_to_draft(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-WF-001"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")

        result = request_workflow_transition(
            session,
            task_type=_TASK_TYPE,
            target_object_id=object_id,
            target_object_version_id=v1,
            from_state="ELIGIBLE_PENDING",
            to_state="DRAFT",
            requested_by="tester",
            idempotency_key=f"wf-{object_id}-1",
        )

        assert result.outcome == TransitionOutcome.COMMITTED
        assert _get_status(session, v1) == "DRAFT"

        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.object_version_id == v1)
        ).scalars().all()
        assert len(transitions) == 1
        assert transitions[0].requested_by == "tester"


def test_transition_not_enabled_this_iteration_is_rejected_without_mutation(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_id = "OBJ-WF-002"
        # DRAFT -> QA_IN_PROGRESS es parte del vocabulario (KNOWN_STATES)
        # pero NO esta en ENABLED_TRANSITIONS_ITERATION_2.
        v1 = _create_object_version(session, object_id, "DRAFT")

        result = request_workflow_transition(
            session,
            task_type=_TASK_TYPE,
            target_object_id=object_id,
            target_object_version_id=v1,
            from_state="DRAFT",
            to_state="QA_IN_PROGRESS",
            requested_by="tester",
            idempotency_key=f"wf-{object_id}-2",
        )

        assert result.outcome == TransitionOutcome.TRANSITION_NOT_ENABLED_THIS_ITERATION
        assert _get_status(session, v1) == "DRAFT"  # sin mutacion

        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.object_version_id == v1)
        ).scalars().all()
        assert transitions == []  # sin fila en workflow_transition


def test_unknown_state_is_rejected(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-WF-003"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")

        result = request_workflow_transition(
            session,
            task_type=_TASK_TYPE,
            target_object_id=object_id,
            target_object_version_id=v1,
            from_state="ELIGIBLE_PENDING",
            to_state="TOTALLY_MADE_UP_STATE",
            requested_by="tester",
            idempotency_key=f"wf-{object_id}-3",
        )

        assert result.outcome == TransitionOutcome.UNKNOWN_STATE
        assert _get_status(session, v1) == "ELIGIBLE_PENDING"


# ---------------------------------------------------------------------------
# 3: version previa obsoleta -> STATE_CONFLICT
# ---------------------------------------------------------------------------


def test_stale_from_state_yields_state_conflict(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-WF-004"
        v1 = _create_object_version(session, object_id, "DRAFT")  # ya no esta en ELIGIBLE_PENDING

        result = request_workflow_transition(
            session,
            task_type=_TASK_TYPE,
            target_object_id=object_id,
            target_object_version_id=v1,
            from_state="ELIGIBLE_PENDING",
            to_state="DRAFT",
            requested_by="tester",
            idempotency_key=f"wf-{object_id}-conflict",
        )

        assert result.outcome == TransitionOutcome.STATE_CONFLICT
        assert _get_status(session, v1) == "DRAFT"

        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.object_version_id == v1)
        ).scalars().all()
        assert transitions == []


# ---------------------------------------------------------------------------
# 4-5: idempotencia - mismo payload / payload distinto
# ---------------------------------------------------------------------------


def test_duplicate_same_key_same_payload_replays_existing_result(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-WF-005"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")
        key = f"wf-{object_id}-replay"

        first = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )
        second = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )

        assert first.outcome == TransitionOutcome.COMMITTED
        assert second.outcome == TransitionOutcome.COMMITTED
        assert first.transition_id == second.transition_id

        count = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.idempotency_key == key)
        ).scalars().all()
        assert len(count) == 1


def test_duplicate_same_key_different_payload_yields_idempotency_conflict(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_a = "OBJ-WF-006-A"
        object_b = "OBJ-WF-006-B"
        v1_a = _create_object_version(session, object_a, "ELIGIBLE_PENDING")
        v1_b = _create_object_version(session, object_b, "ELIGIBLE_PENDING")
        key = f"wf-shared-key-{uuid.uuid4()}"

        first = request_workflow_transition(
            session, _TASK_TYPE, object_a, v1_a, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )
        second = request_workflow_transition(
            session, _TASK_TYPE, object_b, v1_b, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )

        assert first.outcome == TransitionOutcome.COMMITTED
        assert second.outcome == TransitionOutcome.IDEMPOTENCY_CONFLICT
        assert _get_status(session, v1_b) == "ELIGIBLE_PENDING"  # sin mutacion


def test_same_key_identical_transition_different_requested_by_yields_idempotency_conflict(
    session_factory: sessionmaker,
):
    """
    CODE-CR-32: requested_by participa del payload semantico. Misma
    idempotency_key, mismo target_object_id/target_object_version_id/
    from_state/to_state, pero requested_by distinto -> NO es un replay,
    es IDEMPOTENCY_CONFLICT. Exactamente un workflow_transition persiste,
    y su requested_by pertenece al solicitante que gano la reserva.
    """
    with session_factory() as session:
        object_id = "OBJ-WF-006-C"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")
        key = f"wf-{object_id}-requested-by-conflict"

        first = request_workflow_transition(
            session,
            _TASK_TYPE,
            object_id,
            v1,
            "ELIGIBLE_PENDING",
            "DRAFT",
            "alice",
            key,
        )
        second = request_workflow_transition(
            session,
            _TASK_TYPE,
            object_id,
            v1,
            "ELIGIBLE_PENDING",
            "DRAFT",
            "bob",
            key,
        )

        assert first.outcome == TransitionOutcome.COMMITTED
        assert second.outcome == TransitionOutcome.IDEMPOTENCY_CONFLICT

        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.idempotency_key == key)
        ).scalars().all()
        assert len(transitions) == 1
        assert transitions[0].requested_by == "alice"  # pertenece al propietario exitoso
        assert _get_status(session, v1) == "DRAFT"  # solo la mutacion de alice se aplico


# ---------------------------------------------------------------------------
# 6: dos solicitudes validas concurrentes desde el mismo estado original
# ---------------------------------------------------------------------------


def test_two_concurrent_valid_requests_from_same_state_only_one_mutates(_migrated_engine):
    object_id = "OBJ-WF-007"
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        v1 = _create_object_version(setup_session, object_id, "ELIGIBLE_PENDING")

    barrier = threading.Barrier(2)
    results = []

    def attempt(suffix: str):
        barrier.wait()
        with session_factory() as session:
            r = request_workflow_transition(
                session,
                _TASK_TYPE,
                object_id,
                v1,
                "ELIGIBLE_PENDING",
                "DRAFT",
                "tester",
                f"wf-{object_id}-concurrent-{suffix}",
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(attempt, ["A", "B"]))

    committed = [r for r in results if r.outcome == TransitionOutcome.COMMITTED]
    conflicted = [r for r in results if r.outcome == TransitionOutcome.STATE_CONFLICT]
    assert len(committed) == 1
    assert len(conflicted) == 1

    with session_factory() as session:
        assert _get_status(session, v1) == "DRAFT"
        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.object_version_id == v1)
        ).scalars().all()
        assert len(transitions) == 1


# ---------------------------------------------------------------------------
# 7-8: idempotency_key nunca antes vista, concurrente, mismo/distinto payload
# ---------------------------------------------------------------------------


def test_concurrent_unseen_key_same_payload_produces_exactly_one_transition(_migrated_engine):
    object_id = "OBJ-WF-008"
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        v1 = _create_object_version(setup_session, object_id, "ELIGIBLE_PENDING")

    shared_key = f"wf-{object_id}-shared-unseen"
    barrier = threading.Barrier(2)
    results = []

    def attempt():
        barrier.wait()
        with session_factory() as session:
            r = request_workflow_transition(
                session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "tester", shared_key
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: attempt(), range(2)))

    assert all(r.outcome == TransitionOutcome.COMMITTED for r in results)
    assert results[0].transition_id == results[1].transition_id

    with session_factory() as session:
        assert _get_status(session, v1) == "DRAFT"
        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.idempotency_key == shared_key)
        ).scalars().all()
        assert len(transitions) == 1


def test_concurrent_unseen_key_different_payload_one_commits_one_conflicts(_migrated_engine):
    object_a = "OBJ-WF-009-A"
    object_b = "OBJ-WF-009-B"
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        v1_a = _create_object_version(setup_session, object_a, "ELIGIBLE_PENDING")
        v1_b = _create_object_version(setup_session, object_b, "ELIGIBLE_PENDING")

    shared_key = f"wf-shared-unseen-conflicting-{uuid.uuid4()}"
    barrier = threading.Barrier(2)
    results = []

    def attempt(object_id: str, version_id: uuid.UUID):
        barrier.wait()
        with session_factory() as session:
            r = request_workflow_transition(
                session,
                _TASK_TYPE,
                object_id,
                version_id,
                "ELIGIBLE_PENDING",
                "DRAFT",
                "tester",
                shared_key,
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(
            pool.map(
                lambda args: attempt(*args), [(object_a, v1_a), (object_b, v1_b)]
            )
        )

    committed = [r for r in results if r.outcome == TransitionOutcome.COMMITTED]
    conflicted = [r for r in results if r.outcome == TransitionOutcome.IDEMPOTENCY_CONFLICT]
    assert len(committed) == 1
    assert len(conflicted) == 1

    with session_factory() as session:
        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.idempotency_key == shared_key)
        ).scalars().all()
        assert len(transitions) == 1  # nunca duplicado


# ---------------------------------------------------------------------------
# 9-11: replay de resultados rechazados
# ---------------------------------------------------------------------------


def test_retry_same_key_same_payload_after_unknown_state_replays_identically(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_id = "OBJ-WF-010"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")
        key = f"wf-{object_id}-unknown-replay"

        first = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "NOT_A_REAL_STATE", "tester", key
        )
        second = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "NOT_A_REAL_STATE", "tester", key
        )

        assert first.outcome == TransitionOutcome.UNKNOWN_STATE
        assert second.outcome == TransitionOutcome.UNKNOWN_STATE


def test_retry_same_key_same_payload_after_not_enabled_replays_identically(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_id = "OBJ-WF-011"
        v1 = _create_object_version(session, object_id, "DRAFT")
        key = f"wf-{object_id}-not-enabled-replay"

        first = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "DRAFT", "QA_IN_PROGRESS", "tester", key
        )
        second = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "DRAFT", "QA_IN_PROGRESS", "tester", key
        )

        assert first.outcome == TransitionOutcome.TRANSITION_NOT_ENABLED_THIS_ITERATION
        assert second.outcome == TransitionOutcome.TRANSITION_NOT_ENABLED_THIS_ITERATION


def test_retry_same_key_same_payload_after_state_conflict_replays_identically(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_id = "OBJ-WF-012"
        v1 = _create_object_version(session, object_id, "DRAFT")  # ya no ELIGIBLE_PENDING
        key = f"wf-{object_id}-conflict-replay"

        first = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )
        second = request_workflow_transition(
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )

        assert first.outcome == TransitionOutcome.STATE_CONFLICT
        assert second.outcome == TransitionOutcome.STATE_CONFLICT
        # no se reintento la mutacion contra el estado actual en el replay
        assert _get_status(session, v1) == "DRAFT"


# ---------------------------------------------------------------------------
# 12: ninguna fila CLAIMED sobrevive, en ningun camino
# ---------------------------------------------------------------------------


def test_no_claimed_row_survives_any_workflow_transition_path(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-WF-013"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")
        v2 = _create_object_version(session, object_id, "DRAFT")

        request_workflow_transition(  # COMMITTED
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "t", f"wf-{object_id}-ok"
        )
        request_workflow_transition(  # UNKNOWN_STATE
            session, _TASK_TYPE, object_id, v1, "ELIGIBLE_PENDING", "NOPE", "t", f"wf-{object_id}-unk"
        )
        request_workflow_transition(  # TRANSITION_NOT_ENABLED_THIS_ITERATION
            session, _TASK_TYPE, object_id, v2, "DRAFT", "QA_IN_PROGRESS", "t", f"wf-{object_id}-ne"
        )
        request_workflow_transition(  # STATE_CONFLICT
            session, _TASK_TYPE, object_id, v2, "ELIGIBLE_PENDING", "DRAFT", "t", f"wf-{object_id}-sc"
        )
        request_workflow_transition(  # INVALID_OBJECT_VERSION
            session,
            _TASK_TYPE,
            "OBJ-WF-013-OTHER",
            v1,
            "ELIGIBLE_PENDING",
            "DRAFT",
            "t",
            f"wf-{object_id}-inv",
        )

        claimed = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == "WORKFLOW_TRANSITION",
                IdempotencyOperation.status == "CLAIMED",
            )
        ).scalars().all()
        assert claimed == []


# ---------------------------------------------------------------------------
# 13 y CR-28 (14-15): target_object_version_id no pertenece a target_object_id
# ---------------------------------------------------------------------------


def test_version_not_belonging_to_target_object_is_rejected_before_mutation(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_a = "OBJ-WF-014-A"
        object_b = "OBJ-WF-014-B"
        v1_a = _create_object_version(session, object_a, "ELIGIBLE_PENDING")

        result = request_workflow_transition(
            session,
            _TASK_TYPE,
            object_b,  # target_object_id distinto del dueno real de v1_a
            v1_a,
            "ELIGIBLE_PENDING",
            "DRAFT",
            "tester",
            f"wf-{object_b}-invalid-version",
        )

        assert result.outcome == TransitionOutcome.INVALID_OBJECT_VERSION
        assert _get_status(session, v1_a) == "ELIGIBLE_PENDING"


def test_retry_same_key_same_payload_after_invalid_object_version_replays_identically(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_a = "OBJ-WF-015-A"
        object_b = "OBJ-WF-015-B"
        v1_a = _create_object_version(session, object_a, "ELIGIBLE_PENDING")
        key = f"wf-{object_b}-invalid-replay"

        first = request_workflow_transition(
            session, _TASK_TYPE, object_b, v1_a, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )
        second = request_workflow_transition(
            session, _TASK_TYPE, object_b, v1_a, "ELIGIBLE_PENDING", "DRAFT", "tester", key
        )

        assert first.outcome == TransitionOutcome.INVALID_OBJECT_VERSION
        assert second.outcome == TransitionOutcome.INVALID_OBJECT_VERSION


# ---------------------------------------------------------------------------
# Task Intake: task_type fuera de catalogo, rechazado antes de tocar tablas
# ---------------------------------------------------------------------------


def test_unknown_task_type_is_rejected_without_touching_any_table(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-WF-016"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")

        result = request_workflow_transition(
            session,
            task_type="SOME_OTHER_TASK_TYPE",
            target_object_id=object_id,
            target_object_version_id=v1,
            from_state="ELIGIBLE_PENDING",
            to_state="DRAFT",
            requested_by="tester",
            idempotency_key=f"wf-{object_id}-unresolved",
        )

        assert result.outcome == TransitionOutcome.TASK_TYPE_UNRESOLVED
        assert _get_status(session, v1) == "ELIGIBLE_PENDING"

        reservations = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.idempotency_key == f"wf-{object_id}-unresolved"
            )
        ).scalars().all()
        assert reservations == []  # nunca toco idempotency_operation
