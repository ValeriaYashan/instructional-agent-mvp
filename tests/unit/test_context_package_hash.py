import uuid

from src.persistence.repositories.context_engine import (
    compute_context_package_hash,
    compute_request_idempotency_hash,
)


def _sample_kwargs(**overrides):
    base = dict(
        context_contract_version="context-engine-v1",
        task_type="PRODUCE_LEARNING_OBJECT",
        target_object_id="OBJ-001",
        pinned_version_id=None,
        approved_objects=[{"object_id": "OBJ-001", "object_type": None, "version": 1, "status": "APPROVED_CONTENT", "data": {}}],
        relations=[{"from_id": "OBJ-001", "to_id": "OBJ-ACT", "relation_type": "PARENT_ACTIVITY", "status": "RESOLVED"}],
        evidence=[{"source_id": "EV-1", "evidence_class": "E1", "resolution_status": "FOUND"}],
        dependency_status=[{"dependency_id": "PARENT_ACTIVITY", "status": "SATISFIED"}],
        gaps=[],
    )
    base.update(overrides)
    return base


def test_identical_input_produces_identical_hash():
    h1 = compute_context_package_hash(**_sample_kwargs())
    h2 = compute_context_package_hash(**_sample_kwargs())
    assert h1 == h2


def test_list_order_does_not_affect_hash():
    kwargs_a = _sample_kwargs(
        approved_objects=[
            {"object_id": "OBJ-002", "object_type": None, "version": 1, "status": "APPROVED_CONTENT", "data": {}},
            {"object_id": "OBJ-001", "object_type": None, "version": 1, "status": "APPROVED_CONTENT", "data": {}},
        ]
    )
    kwargs_b = _sample_kwargs(
        approved_objects=[
            {"object_id": "OBJ-001", "object_type": None, "version": 1, "status": "APPROVED_CONTENT", "data": {}},
            {"object_id": "OBJ-002", "object_type": None, "version": 1, "status": "APPROVED_CONTENT", "data": {}},
        ]
    )
    assert compute_context_package_hash(**kwargs_a) == compute_context_package_hash(**kwargs_b)


def test_different_contract_version_changes_hash():
    h1 = compute_context_package_hash(**_sample_kwargs(context_contract_version="context-engine-v1"))
    h2 = compute_context_package_hash(**_sample_kwargs(context_contract_version="context-engine-v2"))
    assert h1 != h2  # CR-38: context_contract_version participa del hash semantico


def test_different_content_changes_hash():
    h1 = compute_context_package_hash(**_sample_kwargs())
    h2 = compute_context_package_hash(**_sample_kwargs(gaps=[{"gap_id": "X", "missing_object_or_value": "x", "blocking": True}]))
    assert h1 != h2


def test_request_idempotency_hash_ignores_contract_version_by_design():
    """
    CR-38: el hash de identidad de la SOLICITUD (distinto del hash de
    contenido del Context Package) no depende de context_contract_version
    en absoluto - ni siquiera lo recibe como parametro.
    """
    v = uuid.uuid4()
    h1 = compute_request_idempotency_hash("PRODUCE_LEARNING_OBJECT", "OBJ-001", v)
    h2 = compute_request_idempotency_hash("PRODUCE_LEARNING_OBJECT", "OBJ-001", v)
    assert h1 == h2


def test_request_idempotency_hash_changes_with_target_object():
    v = uuid.uuid4()
    h1 = compute_request_idempotency_hash("PRODUCE_LEARNING_OBJECT", "OBJ-001", v)
    h2 = compute_request_idempotency_hash("PRODUCE_LEARNING_OBJECT", "OBJ-002", v)
    assert h1 != h2


def test_request_idempotency_hash_none_pinned_version_is_stable():
    h1 = compute_request_idempotency_hash("PRODUCE_LEARNING_OBJECT", "OBJ-001", None)
    h2 = compute_request_idempotency_hash("PRODUCE_LEARNING_OBJECT", "OBJ-001", None)
    assert h1 == h2
