from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import sessionmaker

from src.domain.context_result import ContextAssemblyOutcome
from src.persistence.models import (
    ContextPackage,
    EvidenceSource,
    IdempotencyOperation,
    InstructionalObject,
    ObjectRelation,
    ObjectVersion,
)
from src.persistence.repositories import context_engine as context_engine_module
from src.persistence.repositories.approved_state import (
    commit_approved_state,
    initialize_approved_state_pointer,
)
from src.persistence.repositories.context_engine import assemble_context

_TASK_TYPE = "PRODUCE_LEARNING_OBJECT"


def _ensure_instructional_object(session, object_id: str, object_type: str | None = None) -> None:
    """CODE-CR-42: sin trigger; InstructionalObject primero, ObjectVersion despues."""
    existing = session.get(InstructionalObject, object_id)
    if existing is None:
        session.add(InstructionalObject(object_id=object_id, object_type=object_type))
        session.flush()


def _create_draft_version(session, object_id: str, object_type: str | None, status: str) -> uuid.UUID:
    _ensure_instructional_object(session, object_id, object_type)
    version_id = uuid.uuid4()
    session.add(
        ObjectVersion(object_version_id=version_id, object_id=object_id, version_number=1, status=status)
    )
    session.commit()
    return version_id


def _approve_object(session, object_id: str, object_type: str | None = None) -> uuid.UUID:
    version_id = _create_draft_version(session, object_id, object_type, "APPROVED_CONTENT")
    initialize_approved_state_pointer(session, object_id)
    commit_approved_state(
        session, object_id, version_id, None, idempotency_key=f"approve-{object_id}-{version_id}"
    )
    return version_id


def _link(session, from_object_id: str, to_object_id: str, relation_type: str) -> None:
    session.add(
        ObjectRelation(
            relation_id=uuid.uuid4(),
            from_object_id=from_object_id,
            to_object_id=to_object_id,
            relation_type=relation_type,
        )
    )
    session.commit()


def _add_evidence(session, object_id: str, evidence_class: str, source_id: str) -> None:
    session.add(
        EvidenceSource(
            source_id=source_id,
            object_id=object_id,
            evidence_class=evidence_class,
            content_reference=f"ref://{source_id}",
        )
    )
    session.commit()


def _fully_satisfied_target(session, suffix: str) -> str:
    """Target con parent activity + learning outcome del TIPO CORRECTO,
    relaciones y evidencia E1 - satisface el mapeo completo."""
    target = f"OBJ-CE-{suffix}"
    activity = f"ACT-CE-{suffix}"
    outcome = f"LO-CE-{suffix}"

    _approve_object(session, target, object_type="LEARNING_OBJECT")
    _approve_object(session, activity, object_type="ACTIVITY")
    _approve_object(session, outcome, object_type="LEARNING_OUTCOME")
    _link(session, target, activity, "PARENT_ACTIVITY")
    _link(session, target, outcome, "LEARNING_OUTCOME")
    _add_evidence(session, target, "E1", f"EV-{suffix}-1")
    return target


def _evidence_entry(package: ContextPackage, evidence_class: str) -> dict:
    return next(e for e in package.evidence if e["evidence_class"] == evidence_class)


# ---------------------------------------------------------------------------
# Paso 1: Task Intake - rechazo sin tocar ninguna tabla
# ---------------------------------------------------------------------------


def test_unknown_task_type_rejected_before_touching_any_table(session_factory: sessionmaker):
    with session_factory() as session:
        result = assemble_context(
            session,
            task_type="SOME_OTHER_TASK_TYPE",
            target_object_id="OBJ-DOESNT-MATTER",
            idempotency_key="ce-unresolved-1",
        )
        assert result.outcome == ContextAssemblyOutcome.TASK_TYPE_UNRESOLVED

        touched = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.idempotency_key == "ce-unresolved-1"
            )
        ).scalars().all()
        assert touched == []  # nunca persistido - no es un replay, es un rechazo sincrono


# ---------------------------------------------------------------------------
# Paso 2: MAPPING_UNDEFINED (rama simulada via monkeypatch - inalcanzable
# por la via publica normal porque el catalogo de Task Intake y el mapeo
# coinciden 1:1 en esta iteracion)
# ---------------------------------------------------------------------------


def test_mapping_undefined_rejected_and_replayable(session_factory: sessionmaker, monkeypatch):
    monkeypatch.setattr(context_engine_module, "is_known_task_type", lambda task_type: True)
    with session_factory() as session:
        key = "ce-mapping-undefined"
        first = assemble_context(
            session, task_type="UNMAPPED_TASK_TYPE", target_object_id="OBJ-X", idempotency_key=key
        )
        second = assemble_context(
            session, task_type="UNMAPPED_TASK_TYPE", target_object_id="OBJ-X", idempotency_key=key
        )
        assert first.outcome == ContextAssemblyOutcome.MAPPING_UNDEFINED
        assert second.outcome == ContextAssemblyOutcome.MAPPING_UNDEFINED


# ---------------------------------------------------------------------------
# CODE-CR-39: validacion de object_type en relaciones requeridas
# ---------------------------------------------------------------------------


def test_relation_to_wrong_typed_object_does_not_satisfy_dependency(session_factory: sessionmaker):
    with session_factory() as session:
        target = "OBJ-CE-WRONGTYPE"
        wrong_typed = "OBJ-CE-WRONGTYPE-ACT"

        _approve_object(session, target, object_type="LEARNING_OBJECT")
        # Esta relacion declara PARENT_ACTIVITY (requiere ACTIVITY) pero
        # el objeto destino es, en realidad, de tipo LEARNING_OUTCOME.
        _approve_object(session, wrong_typed, object_type="LEARNING_OUTCOME")
        _link(session, target, wrong_typed, "PARENT_ACTIVITY")

        result = assemble_context(
            session, task_type=_TASK_TYPE, target_object_id=target, idempotency_key=f"ce-{target}"
        )

        assert result.outcome == ContextAssemblyOutcome.COMMITTED  # no rechaza, es un gap
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()

        gap_ids = {g["gap_id"] for g in package.gaps}
        assert "BLOCKED_BY_DEPENDENCY:PARENT_ACTIVITY" in gap_ids
        # El objeto de tipo incorrecto NO entra a approved_objects.
        assert wrong_typed not in {o["object_id"] for o in package.approved_objects}


def test_relation_to_untyped_legacy_object_does_not_satisfy_dependency(session_factory: sessionmaker):
    """Un objeto legacy con object_type=NULL (retropoblado por la
    migracion 0003) tampoco satisface un requisito de tipo explicito."""
    with session_factory() as session:
        target = "OBJ-CE-UNTYPED"
        untyped_activity = "OBJ-CE-UNTYPED-ACT"

        _approve_object(session, target, object_type="LEARNING_OBJECT")
        _approve_object(session, untyped_activity, object_type=None)  # legacy, sin tipo
        _link(session, target, untyped_activity, "PARENT_ACTIVITY")

        result = assemble_context(
            session, task_type=_TASK_TYPE, target_object_id=target, idempotency_key=f"ce-{target}"
        )

        assert result.outcome == ContextAssemblyOutcome.COMMITTED
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()
        assert "BLOCKED_BY_DEPENDENCY:PARENT_ACTIVITY" in {g["gap_id"] for g in package.gaps}


# ---------------------------------------------------------------------------
# CODE-CR-40: validacion completa de pinned_version_id
# ---------------------------------------------------------------------------


def test_pinned_version_nonexistent_is_invalid_object_version(session_factory: sessionmaker):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "PIN-NONE")
        result = assemble_context(
            session,
            task_type=_TASK_TYPE,
            target_object_id=target,
            idempotency_key=f"ce-{target}-pin-none",
            pinned_version_id=uuid.uuid4(),  # no existe
        )
        assert result.outcome == ContextAssemblyOutcome.INVALID_OBJECT_VERSION


def test_pinned_version_belonging_to_different_object_is_invalid(session_factory: sessionmaker):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "PIN-XOBJ-A")
        other = _approve_object(session, "OBJ-CE-PIN-XOBJ-B", object_type="LEARNING_OBJECT")

        result = assemble_context(
            session,
            task_type=_TASK_TYPE,
            target_object_id=target,
            idempotency_key=f"ce-{target}-pin-xobj",
            pinned_version_id=other,  # version real, pero de OTRO object_id
        )
        assert result.outcome == ContextAssemblyOutcome.INVALID_OBJECT_VERSION


def test_pinned_version_with_insufficient_status_produces_gap_not_rejection(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        target = "OBJ-CE-PIN-INSUFFICIENT"
        draft_version = _create_draft_version(session, target, "LEARNING_OBJECT", "DRAFT")
        initialize_approved_state_pointer(session, target)  # nunca se aprueba

        result = assemble_context(
            session,
            task_type=_TASK_TYPE,
            target_object_id=target,
            idempotency_key=f"ce-{target}-pin-insufficient",
            pinned_version_id=draft_version,
        )

        assert result.outcome == ContextAssemblyOutcome.COMMITTED  # gap, no rechazo
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()
        assert any(g["gap_id"] == f"APPROVAL_INSUFFICIENT:{draft_version}" for g in package.gaps)
        assert target not in {o["object_id"] for o in package.approved_objects}


def test_pinned_version_matching_current_approved_pointer_succeeds(session_factory: sessionmaker):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "PIN-MATCH")
        current = session.execute(
            select(ObjectVersion.object_version_id).where(ObjectVersion.object_id == target)
        ).scalar_one()

        # CR-37 / CODE-CR-46: fixture/preparation reads must finish before
        # Context Engine takes ownership of its REPEATABLE READ transaction.
        session.rollback()

        result = assemble_context(
            session,
            task_type=_TASK_TYPE,
            target_object_id=target,
            idempotency_key=f"ce-{target}-pin-match",
            pinned_version_id=current,
        )
        assert result.outcome == ContextAssemblyOutcome.COMMITTED


def test_pinned_version_older_than_approved_pointer_is_rejected_by_version_number(
    session_factory: sessionmaker,
):
    """CODE-CR-40: la comparacion es por version_number, no por
    desigualdad de UUID."""
    with session_factory() as session:
        object_id = "OBJ-CE-NEWER"
        v1 = _approve_object(session, object_id, object_type="LEARNING_OBJECT")
        v2 = uuid.uuid4()
        session.add(
            ObjectVersion(
                object_version_id=v2, object_id=object_id, version_number=2, status="APPROVED_CONTENT"
            )
        )
        session.commit()
        commit_approved_state(session, object_id, v2, v1, idempotency_key=f"approve-{object_id}-v2")

        result = assemble_context(
            session,
            task_type=_TASK_TYPE,
            target_object_id=object_id,
            idempotency_key="ce-newer-version",
            pinned_version_id=v1,  # version_number=1, el puntero ya esta en version_number=2
        )
        assert result.outcome == ContextAssemblyOutcome.NEWER_VERSION_AVAILABLE


# ---------------------------------------------------------------------------
# Ensamblado exitoso, todo satisfecho
# ---------------------------------------------------------------------------


def test_fully_satisfied_assembly_commits_with_no_blocking_gaps(session_factory: sessionmaker):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "001")

        result = assemble_context(
            session, task_type=_TASK_TYPE, target_object_id=target, idempotency_key=f"ce-{target}"
        )

        assert result.outcome == ContextAssemblyOutcome.COMMITTED
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()

        assert len(package.approved_objects) == 3
        blocking_gaps = [g for g in package.gaps if g["blocking"]]
        assert blocking_gaps == []
        non_blocking_gaps = [g for g in package.gaps if not g["blocking"]]
        assert any(g["gap_id"] == "EVIDENCE_MISSING:E3" for g in non_blocking_gaps)


# ---------------------------------------------------------------------------
# CODE-CR-41: tupla completa de Evidence Sufficiency preservada
# ---------------------------------------------------------------------------


def test_evidence_tuple_required_blocking_found(session_factory: sessionmaker):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "EV-FOUND")
        result = assemble_context(session, _TASK_TYPE, target, f"ce-{target}")
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()

        e1 = _evidence_entry(package, "E1")
        assert e1["requirement_level"] == "REQUIRED"
        assert e1["blocking_behavior"] == "BLOCKING"
        assert e1["resolution_status"] == "FOUND"
        assert e1["source_id"] is not None


def test_evidence_tuple_required_blocking_missing(session_factory: sessionmaker):
    with session_factory() as session:
        target = "OBJ-CE-EV-MISSING"
        _approve_object(session, target, object_type="LEARNING_OBJECT")  # sin evidencia E1

        result = assemble_context(session, _TASK_TYPE, target, f"ce-{target}")
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()

        e1 = _evidence_entry(package, "E1")
        assert e1["requirement_level"] == "REQUIRED"
        assert e1["blocking_behavior"] == "BLOCKING"
        assert e1["resolution_status"] == "MISSING"
        assert e1["source_id"] is None
        e1_gap = next(g for g in package.gaps if g["gap_id"] == "EVIDENCE_MISSING:E1")
        assert e1_gap["blocking"] is True


def test_evidence_tuple_optional_non_blocking_missing(session_factory: sessionmaker):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "EV-OPTIONAL")  # tiene E1, no tiene E3
        result = assemble_context(session, _TASK_TYPE, target, f"ce-{target}")
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()

        e3 = _evidence_entry(package, "E3")
        assert e3["requirement_level"] == "OPTIONAL"
        assert e3["blocking_behavior"] == "NON_BLOCKING"
        assert e3["resolution_status"] == "MISSING"
        e3_gap = next(g for g in package.gaps if g["gap_id"] == "EVIDENCE_MISSING:E3")
        assert e3_gap["blocking"] is False


# ---------------------------------------------------------------------------
# Gaps no bloqueantes del ensamblado: NO_APPROVED_VERSION, BLOCKED_BY_DEPENDENCY
# ---------------------------------------------------------------------------


def test_missing_dependencies_and_evidence_become_gaps_not_rejections(session_factory: sessionmaker):
    with session_factory() as session:
        target = "OBJ-CE-002"
        _approve_object(session, target, object_type="LEARNING_OBJECT")

        result = assemble_context(
            session, task_type=_TASK_TYPE, target_object_id=target, idempotency_key=f"ce-{target}"
        )

        assert result.outcome == ContextAssemblyOutcome.COMMITTED
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()

        gap_ids = {g["gap_id"] for g in package.gaps}
        assert "BLOCKED_BY_DEPENDENCY:PARENT_ACTIVITY" in gap_ids
        assert "BLOCKED_BY_DEPENDENCY:LEARNING_OUTCOME" in gap_ids
        assert "EVIDENCE_MISSING:E1" in gap_ids


def test_target_without_approved_version_produces_no_approved_version_gap(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        target = "OBJ-CE-003"
        _create_draft_version(session, target, "LEARNING_OBJECT", "DRAFT")
        initialize_approved_state_pointer(session, target)

        result = assemble_context(
            session, task_type=_TASK_TYPE, target_object_id=target, idempotency_key=f"ce-{target}"
        )

        assert result.outcome == ContextAssemblyOutcome.COMMITTED
        package = session.execute(
            select(ContextPackage).where(ContextPackage.context_package_id == result.context_package_id)
        ).scalar_one()
        gap_ids = {g["gap_id"] for g in package.gaps}
        assert f"NO_APPROVED_VERSION:{target}" in gap_ids
        assert package.approved_objects == []


# ---------------------------------------------------------------------------
# CR-35: request idempotency != context freshness
# ---------------------------------------------------------------------------


def test_replay_returns_same_package_even_after_approved_state_changed(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "004")
        key = f"ce-{target}-replay"

        first = assemble_context(session, _TASK_TYPE, target, key)
        assert first.outcome == ContextAssemblyOutcome.COMMITTED

        _add_evidence(session, target, "E3", f"EV-{target}-optional-now-present")

        second = assemble_context(session, _TASK_TYPE, target, key)

        assert second.outcome == ContextAssemblyOutcome.COMMITTED
        assert second.context_package_id == first.context_package_id
        assert second.context_package_hash == first.context_package_hash


def test_same_key_different_target_yields_idempotency_conflict(session_factory: sessionmaker):
    with session_factory() as session:
        target_a = _fully_satisfied_target(session, "005A")
        target_b = _fully_satisfied_target(session, "005B")
        key = f"ce-shared-key-{uuid.uuid4()}"

        first = assemble_context(session, _TASK_TYPE, target_a, key)
        second = assemble_context(session, _TASK_TYPE, target_b, key)

        assert first.outcome == ContextAssemblyOutcome.COMMITTED
        assert second.outcome == ContextAssemblyOutcome.IDEMPOTENCY_CONFLICT


# ---------------------------------------------------------------------------
# CR-36/CR-38: el hash de contenido es estable entre ensamblados
# independientes del mismo estado
# ---------------------------------------------------------------------------


def test_content_hash_identical_for_independent_assemblies_of_same_state(
    session_factory: sessionmaker,
):
    with session_factory() as session:
        target = _fully_satisfied_target(session, "006")

        first = assemble_context(session, _TASK_TYPE, target, f"ce-{target}-key-a")
        second = assemble_context(session, _TASK_TYPE, target, f"ce-{target}-key-b")

        assert first.context_package_id != second.context_package_id
        assert first.context_package_hash == second.context_package_hash


# ---------------------------------------------------------------------------
# Ninguna fila CLAIMED sobrevive
# ---------------------------------------------------------------------------


def test_no_claimed_row_survives_any_context_assembly_path(session_factory: sessionmaker):
    with session_factory() as session:
        target_ok = _fully_satisfied_target(session, "007")
        assemble_context(session, _TASK_TYPE, target_ok, f"ce-{target_ok}-ok")

        assemble_context(session, "NOPE", "OBJ-IRRELEVANT", "ce-007-unresolved")

        object_id = "OBJ-CE-007-NEWER"
        v1 = _approve_object(session, object_id, object_type="LEARNING_OBJECT")
        v2 = uuid.uuid4()
        session.add(
            ObjectVersion(object_version_id=v2, object_id=object_id, version_number=2, status="APPROVED_CONTENT")
        )
        session.commit()
        commit_approved_state(session, object_id, v2, v1, idempotency_key=f"approve-{object_id}-v2")
        assemble_context(session, _TASK_TYPE, object_id, "ce-007-newer", pinned_version_id=v1)

        assemble_context(
            session, _TASK_TYPE, object_id, "ce-007-invalid-pin", pinned_version_id=uuid.uuid4()
        )

        claimed = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == "CONTEXT_ASSEMBLY",
                IdempotencyOperation.status == "CLAIMED",
            )
        ).scalars().all()
        assert claimed == []


# ---------------------------------------------------------------------------
# Concurrencia: misma idempotency_key nunca antes vista, concurrente
# ---------------------------------------------------------------------------


def test_concurrent_unseen_key_same_request_produces_exactly_one_package(_migrated_engine):
    session_factory = sessionmaker(bind=_migrated_engine, future=True, expire_on_commit=False)

    with session_factory() as setup_session:
        target = _fully_satisfied_target(setup_session, "008")

    shared_key = f"ce-{target}-concurrent"
    barrier = threading.Barrier(2)
    results = []

    def attempt():
        barrier.wait()
        with session_factory() as session:
            r = assemble_context(session, _TASK_TYPE, target, shared_key)
            results.append(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: attempt(), range(2)))

    assert all(r.outcome == ContextAssemblyOutcome.COMMITTED for r in results)
    assert results[0].context_package_id == results[1].context_package_id

    with session_factory() as session:
        packages = session.execute(
            select(ContextPackage).where(ContextPackage.idempotency_key == shared_key)
        ).scalars().all()
        assert len(packages) == 1

def test_context_engine_runs_steps_under_repeatable_read(session_factory: sessionmaker):
    """CODE-CR-46 / CR-37: Steps 1-9 execute in one PostgreSQL
    REPEATABLE READ transaction."""
    with session_factory() as session:
        target = _fully_satisfied_target(session, "ISOLATION-RR")

        observed_isolation_levels: list[str] = []
        delegate = context_engine_module.PostgresDependencyResolver()

        class IsolationProbeDependencyResolver:
            def resolve(self, session, object_id, required_relations):
                observed_isolation_levels.append(
                    session.execute(text("SHOW transaction_isolation")).scalar_one()
                )
                return delegate.resolve(session, object_id, required_relations)

        result = assemble_context(
            session,
            task_type=_TASK_TYPE,
            target_object_id=target,
            idempotency_key=f"ce-{target}-isolation",
            dependency_resolver=IsolationProbeDependencyResolver(),
        )

        assert result.outcome == ContextAssemblyOutcome.COMMITTED
        assert observed_isolation_levels
        assert all(
            level == "repeatable read"
            for level in observed_isolation_levels
        )

