from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from src.domain.transition_result import TransitionOutcome
from src.persistence.models import InstructionalObject, ObjectVersion, WorkflowTransition
from src.persistence.repositories.workflow_transition import (
    apply_transition_in_transaction,
    compute_transition_payload_hash,
)

_TASK_TYPE = "PRODUCE_LEARNING_OBJECT"


def _create_object_version(session, object_id: str, status: str) -> uuid.UUID:
    if session.get(InstructionalObject, object_id) is None:
        session.add(InstructionalObject(object_id=object_id, object_type=None))
        session.flush()
    version_id = uuid.uuid4()
    session.add(
        ObjectVersion(object_version_id=version_id, object_id=object_id, version_number=1, status=status)
    )
    session.commit()
    return version_id


def test_apply_transition_in_transaction_does_not_commit_on_its_own(session_factory: sessionmaker):
    """
    CR-55: apply_transition_in_transaction nunca hace session.commit()
    ni session.rollback() - el llamador decide el limite transaccional.
    Se verifica ejecutando la funcion y luego haciendo ROLLBACK
    explicito del lado del test: si la funcion hubiera hecho su propio
    commit, este rollback no revertiria nada y el estado persistiria
    de todas formas.
    """
    with session_factory() as session:
        object_id = "OBJ-WFTS-001"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")

        payload_hash = compute_transition_payload_hash(object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "tester")
        result = apply_transition_in_transaction(
            session=session,
            target_object_id=object_id,
            target_object_version_id=v1,
            from_state="ELIGIBLE_PENDING",
            to_state="DRAFT",
            requested_by="tester",
            correlation_id=str(uuid.uuid4()),
            idempotency_key=f"wfts-{object_id}",
            payload_hash=payload_hash,
        )
        assert result.outcome == TransitionOutcome.COMMITTED

        # Deliberadamente hacemos ROLLBACK nosotros mismos, del lado del
        # test - si apply_transition_in_transaction hubiera hecho su
        # propio commit, esto no tendria efecto y la version seguiria
        # en DRAFT.
        session.rollback()

        version = session.get(ObjectVersion, v1)
        assert version.status == "ELIGIBLE_PENDING"  # el rollback SI revirtio el cambio

        transitions = session.execute(
            select(WorkflowTransition).where(WorkflowTransition.object_version_id == v1)
        ).scalars().all()
        assert transitions == []  # tambien revertida


def test_apply_transition_in_transaction_composes_with_external_commit(session_factory: sessionmaker):
    """El caso de uso real de Fase C: el llamador SI decide comitear,
    junto con otras escrituras en la misma transaccion."""
    with session_factory() as session:
        object_id = "OBJ-WFTS-002"
        v1 = _create_object_version(session, object_id, "ELIGIBLE_PENDING")

        payload_hash = compute_transition_payload_hash(object_id, v1, "ELIGIBLE_PENDING", "DRAFT", "tester")
        result = apply_transition_in_transaction(
            session=session,
            target_object_id=object_id,
            target_object_version_id=v1,
            from_state="ELIGIBLE_PENDING",
            to_state="DRAFT",
            requested_by="tester",
            correlation_id=str(uuid.uuid4()),
            idempotency_key=f"wfts-{object_id}",
            payload_hash=payload_hash,
        )
        assert result.outcome == TransitionOutcome.COMMITTED

        session.commit()  # el llamador decide el limite transaccional

        version = session.get(ObjectVersion, v1)
        assert version.status == "DRAFT"
