from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.domain.commit_result import CommitOutcome
from src.persistence.models import (
    ApprovedStateCommit,
    ApprovedStatePointer,
    AuditEvent,
    IdempotencyOperation,
    ObjectVersion,
)
from src.persistence.repositories.approved_state import (
    commit_approved_state,
    get_current_approved_version_id,
    initialize_approved_state_pointer,
)


def _create_object_version(session, object_id: str, version_number: int) -> uuid.UUID:
    version_id = uuid.uuid4()
    session.add(
        ObjectVersion(
            object_version_id=version_id,
            object_id=object_id,
            version_number=version_number,
            status="DRAFT",
        )
    )
    session.commit()
    return version_id


# ---------------------------------------------------------------------------
# Casos heredados de Fase 1 / Iteration 1 Proposal v0.1
# ---------------------------------------------------------------------------


def test_pointer_starts_as_null_before_first_approval(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-REQ-001"
        _create_object_version(session, object_id, version_number=1)
        initialize_approved_state_pointer(session, object_id)

        assert get_current_approved_version_id(session, object_id) is None


def test_first_approval_from_null_succeeds(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-REQ-002"
        v1 = _create_object_version(session, object_id, version_number=1)
        initialize_approved_state_pointer(session, object_id)

        result = commit_approved_state(
            session,
            object_id=object_id,
            object_version_id=v1,
            expected_previous_approved_version_id=None,
            idempotency_key=f"commit-{object_id}-v1",
        )

        assert result.outcome == CommitOutcome.COMMITTED
        assert get_current_approved_version_id(session, object_id) == v1


def test_second_valid_approval_moves_pointer_forward(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-REQ-003"
        v1 = _create_object_version(session, object_id, version_number=1)
        v2 = _create_object_version(session, object_id, version_number=2)
        initialize_approved_state_pointer(session, object_id)

        commit_approved_state(
            session, object_id, v1, None, idempotency_key=f"commit-{object_id}-v1"
        )
        result = commit_approved_state(
            session, object_id, v2, v1, idempotency_key=f"commit-{object_id}-v2"
        )

        assert result.outcome == CommitOutcome.COMMITTED
        assert get_current_approved_version_id(session, object_id) == v2


# ---------------------------------------------------------------------------
# CODE-CR-03: demostrar que el fallo previamente oculto ya no ocurre
# ---------------------------------------------------------------------------


def test_stale_expected_previous_version_yields_state_conflict_with_zero_persisted_rows(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_id = "OBJ-REQ-004"
        v1 = _create_object_version(session, object_id, version_number=1)
        v2 = _create_object_version(session, object_id, version_number=2)
        initialize_approved_state_pointer(session, object_id)

        commit_approved_state(
            session, object_id, v1, None, idempotency_key=f"commit-{object_id}-v1"
        )

        conflict_key = f"commit-{object_id}-stale"
        # Alguien mas ya movio el puntero a v1; este intento asume que seguia en NULL.
        result = commit_approved_state(
            session, object_id, v2, None, idempotency_key=conflict_key
        )

        assert result.outcome == CommitOutcome.STATE_CONFLICT

        # El puntero no cambio.
        assert get_current_approved_version_id(session, object_id) == v1

        # Cero filas de ApprovedStateCommit para este intento especifico.
        commit_rows = session.execute(
            select(ApprovedStateCommit).where(
                ApprovedStateCommit.idempotency_key == conflict_key
            )
        ).scalars().all()
        assert commit_rows == []

        # Cero AuditEvent asociados a este intento (ningun commit_id que
        # referenciar, y ningun evento huerfano con ese object_version_id
        # objetivo por fuera del commit exitoso de v1).
        conflict_events = session.execute(
            select(AuditEvent).where(AuditEvent.object_version_id == v2)
        ).scalars().all()
        assert conflict_events == []

        # Tampoco queda una reserva de idempotencia huerfana para esta key.
        reservation = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.idempotency_key == conflict_key
            )
        ).scalar_one_or_none()
        assert reservation is None


def test_idempotent_retry_same_payload_returns_existing_result(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-REQ-005"
        v1 = _create_object_version(session, object_id, version_number=1)
        initialize_approved_state_pointer(session, object_id)

        key = f"commit-{object_id}-v1"
        first = commit_approved_state(session, object_id, v1, None, idempotency_key=key)
        second = commit_approved_state(session, object_id, v1, None, idempotency_key=key)

        assert first.outcome == CommitOutcome.COMMITTED
        assert second.outcome == CommitOutcome.COMMITTED
        assert first.commit_id == second.commit_id

        count = session.execute(
            select(ApprovedStateCommit).where(ApprovedStateCommit.idempotency_key == key)
        ).scalars().all()
        assert len(count) == 1


# ---------------------------------------------------------------------------
# CR-18: conflicto de idempotencia por payload distinto (secuencial)
# ---------------------------------------------------------------------------


def test_same_key_different_payload_yields_idempotency_conflict(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-REQ-006"
        v1 = _create_object_version(session, object_id, version_number=1)
        v2 = _create_object_version(session, object_id, version_number=2)
        initialize_approved_state_pointer(session, object_id)

        key = f"commit-{object_id}-reused-key"
        first = commit_approved_state(session, object_id, v1, None, idempotency_key=key)
        # Misma key, payload semanticamente distinto (otra version objetivo).
        second = commit_approved_state(session, object_id, v2, v1, idempotency_key=key)

        assert first.outcome == CommitOutcome.COMMITTED
        assert second.outcome == CommitOutcome.IDEMPOTENCY_CONFLICT
        # El puntero no avanzo a v2 por la solicitud en conflicto.
        assert get_current_approved_version_id(session, object_id) == v1


# ---------------------------------------------------------------------------
# CR-19 / CR-20: integridad relacional cruzada, forzada a nivel de base de datos
# ---------------------------------------------------------------------------


def test_pointer_cannot_reference_version_of_another_object(session_factory: sessionmaker):
    with session_factory() as session:
        object_a = "OBJ-REQ-007-A"
        object_b = "OBJ-REQ-007-B"
        v1_a = _create_object_version(session, object_a, version_number=1)
        _create_object_version(session, object_b, version_number=1)
        initialize_approved_state_pointer(session, object_a)
        initialize_approved_state_pointer(session, object_b)

        with pytest.raises(IntegrityError):
            session.execute(
                ApprovedStatePointer.__table__.update()
                .where(ApprovedStatePointer.object_id == object_b)
                .values(current_approved_version_id=v1_a)
            )
            session.commit()
        session.rollback()


def test_commit_cannot_reference_cross_object_version_pair(session_factory: sessionmaker):
    with session_factory() as session:
        object_a = "OBJ-REQ-008-A"
        object_b = "OBJ-REQ-008-B"
        v1_a = _create_object_version(session, object_a, version_number=1)
        _create_object_version(session, object_b, version_number=1)
        initialize_approved_state_pointer(session, object_a)
        initialize_approved_state_pointer(session, object_b)

        # object_id=B pero object_version_id perteneciente a A -> viola la
        # FK compuesta fk_approved_state_commit_object_version.
        with pytest.raises(IntegrityError):
            session.add(
                ApprovedStateCommit(
                    commit_id=uuid.uuid4(),
                    object_id=object_b,
                    object_version_id=v1_a,
                    expected_previous_approved_version_id=None,
                    idempotency_key=f"invalid-cross-object-{uuid.uuid4()}",
                    idempotency_payload_hash="irrelevant",
                    outcome="COMMITTED",
                )
            )
            session.commit()
        session.rollback()


def test_commit_rejects_object_version_belonging_to_different_object_id(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        object_a = "OBJ-REQ-009-A"
        object_b = "OBJ-REQ-009-B"
        v1_a = _create_object_version(session, object_a, version_number=1)
        initialize_approved_state_pointer(session, object_b)

        result = commit_approved_state(
            session,
            object_id=object_b,
            object_version_id=v1_a,
            expected_previous_approved_version_id=None,
            idempotency_key=f"commit-{object_b}-invalid",
        )
        assert result.outcome == CommitOutcome.INVALID_OBJECT_VERSION
        assert get_current_approved_version_id(session, object_b) is None


# ---------------------------------------------------------------------------
# Atomicidad (CR-13/CR-17, corregida por CODE-CR-01)
# ---------------------------------------------------------------------------


def test_successful_commit_has_matching_audit_event(session_factory: sessionmaker):
    with session_factory() as session:
        object_id = "OBJ-REQ-010"
        v1 = _create_object_version(session, object_id, version_number=1)
        initialize_approved_state_pointer(session, object_id)

        result = commit_approved_state(
            session, object_id, v1, None, idempotency_key=f"commit-{object_id}-v1"
        )

        events = session.execute(
            select(AuditEvent).where(AuditEvent.commit_id == result.commit_id)
        ).scalars().all()
        assert len(events) == 1
        assert events[0].object_id == object_id
        assert events[0].object_version_id == v1


def test_no_idempotency_operation_ever_persists_in_claimed_state(session_factory: sessionmaker):
    """
    CODE-CR-01: prueba directa de que el defecto de atomicidad no puede
    volver a ocurrir. Se ejecutan varios escenarios (exito, conflicto,
    version invalida) y se verifica que, en NINGUN caso, queda una fila
    de idempotency_operation en estado 'CLAIMED' una vez terminada la
    transaccion - solo puede quedar ausente (rollback completo) o en
    'COMMITTED' (exito).
    """
    with session_factory() as session:
        object_id = "OBJ-REQ-014"
        v1 = _create_object_version(session, object_id, version_number=1)
        v2 = _create_object_version(session, object_id, version_number=2)
        initialize_approved_state_pointer(session, object_id)

        # Exito.
        commit_approved_state(
            session, object_id, v1, None, idempotency_key=f"commit-{object_id}-ok"
        )
        # STATE_CONFLICT.
        commit_approved_state(
            session, object_id, v2, None, idempotency_key=f"commit-{object_id}-conflict"
        )
        # INVALID_OBJECT_VERSION.
        other_object = "OBJ-REQ-014-OTHER"
        initialize_approved_state_pointer(session, other_object)
        commit_approved_state(
            session,
            other_object,
            v1,  # v1 pertenece a object_id, no a other_object
            None,
            idempotency_key=f"commit-{other_object}-invalid",
        )

        claimed_rows = session.execute(
            select(IdempotencyOperation).where(IdempotencyOperation.status == "CLAIMED")
        ).scalars().all()
        assert claimed_rows == []


# ---------------------------------------------------------------------------
# Concurrencia (CR-14 + CODE-CR-02): primera aprobacion concurrente,
# idempotency_key nunca antes vista con mismo payload, e idempotency_key
# nunca antes vista con payload DISTINTO
# ---------------------------------------------------------------------------


def test_concurrent_first_approval_only_one_succeeds(_migrated_engine):
    object_id = "OBJ-REQ-012"
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        v1 = _create_object_version(setup_session, object_id, version_number=1)
        initialize_approved_state_pointer(setup_session, object_id)

    barrier = threading.Barrier(2)
    results = []

    def attempt(key_suffix: str):
        barrier.wait()
        with session_factory() as session:
            r = commit_approved_state(
                session,
                object_id,
                v1,
                None,
                idempotency_key=f"commit-{object_id}-concurrent-{key_suffix}",
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(attempt, ["A", "B"]))

    committed = [r for r in results if r.outcome == CommitOutcome.COMMITTED]
    conflicted = [r for r in results if r.outcome == CommitOutcome.STATE_CONFLICT]
    assert len(committed) == 1
    assert len(conflicted) == 1

    with session_factory() as session:
        assert get_current_approved_version_id(session, object_id) == v1
        all_commits = session.execute(
            select(ApprovedStateCommit).where(ApprovedStateCommit.object_id == object_id)
        ).scalars().all()
        assert len(all_commits) == 1  # el STATE_CONFLICT no dejo fila persistida


def test_concurrent_unseen_idempotency_key_same_payload_produces_exactly_one_commit(
    _migrated_engine,
):
    """
    Dos solicitudes concurrentes usan la MISMA idempotency_key nunca
    antes vista, con el MISMO payload semantico. Se espera exactamente
    un ApprovedStateCommit, una mutacion de puntero y un audit_event.
    """
    object_id = "OBJ-REQ-013"
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        v1 = _create_object_version(setup_session, object_id, version_number=1)
        initialize_approved_state_pointer(setup_session, object_id)

    shared_key = f"commit-{object_id}-shared-unseen-key"
    barrier = threading.Barrier(2)
    results = []

    def attempt():
        barrier.wait()
        with session_factory() as session:
            r = commit_approved_state(
                session, object_id, v1, None, idempotency_key=shared_key
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: attempt(), range(2)))

    assert all(r.outcome == CommitOutcome.COMMITTED for r in results)
    assert results[0].commit_id == results[1].commit_id

    with session_factory() as session:
        assert get_current_approved_version_id(session, object_id) == v1

        commits = session.execute(
            select(ApprovedStateCommit).where(ApprovedStateCommit.idempotency_key == shared_key)
        ).scalars().all()
        assert len(commits) == 1

        events = session.execute(
            select(AuditEvent).where(AuditEvent.commit_id == commits[0].commit_id)
        ).scalars().all()
        assert len(events) == 1


def test_concurrent_unseen_idempotency_key_different_payload_one_commits_one_conflicts(
    _migrated_engine,
):
    """
    CODE-CR-02: dos solicitudes concurrentes usan la MISMA
    idempotency_key nunca antes vista, pero con payload semantico
    DISTINTO (dos object_version_id objetivo diferentes). Se espera que
    exactamente una tenga exito (COMMITTED) y la otra reciba
    IDEMPOTENCY_CONFLICT - nunca dos commits, nunca dos mutaciones de
    puntero, nunca un audit_event huerfano.
    """
    object_id = "OBJ-REQ-015"
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        v1 = _create_object_version(setup_session, object_id, version_number=1)
        v2 = _create_object_version(setup_session, object_id, version_number=2)
        initialize_approved_state_pointer(setup_session, object_id)

    shared_key = f"commit-{object_id}-shared-unseen-key-conflicting"
    barrier = threading.Barrier(2)
    results = []

    def attempt(target_version: uuid.UUID):
        barrier.wait()
        with session_factory() as session:
            r = commit_approved_state(
                session, object_id, target_version, None, idempotency_key=shared_key
            )
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(attempt, [v1, v2]))

    committed = [r for r in results if r.outcome == CommitOutcome.COMMITTED]
    idempotency_conflicted = [
        r for r in results if r.outcome == CommitOutcome.IDEMPOTENCY_CONFLICT
    ]

    # Exactamente un commit logico tuvo exito; el otro fue rechazado por
    # reutilizar la key con un payload distinto - nunca dos exitos ni dos
    # rechazos.
    assert len(committed) == 1
    assert len(idempotency_conflicted) == 1

    with session_factory() as session:
        # El puntero se movio exactamente una vez, hacia la version del
        # que efectivamente gano la reserva de la key.
        current = get_current_approved_version_id(session, object_id)
        assert current == committed[0].object_version_id

        all_commits_for_key = session.execute(
            select(ApprovedStateCommit).where(ApprovedStateCommit.idempotency_key == shared_key)
        ).scalars().all()
        assert len(all_commits_for_key) == 1  # nunca un ApprovedStateCommit duplicado

        events = session.execute(
            select(AuditEvent).where(AuditEvent.commit_id == all_commits_for_key[0].commit_id)
        ).scalars().all()
        assert len(events) == 1  # nunca un audit event huerfano o duplicado
