import uuid

from src.persistence.repositories.approved_state import compute_idempotency_payload_hash


def test_same_inputs_produce_same_hash():
    object_id = "OBJ-REQ-TEST-001"
    version_id = uuid.uuid4()
    expected_prev = uuid.uuid4()

    h1 = compute_idempotency_payload_hash(object_id, version_id, expected_prev)
    h2 = compute_idempotency_payload_hash(object_id, version_id, expected_prev)

    assert h1 == h2


def test_different_expected_previous_version_changes_hash():
    object_id = "OBJ-REQ-TEST-001"
    version_id = uuid.uuid4()

    h1 = compute_idempotency_payload_hash(object_id, version_id, uuid.uuid4())
    h2 = compute_idempotency_payload_hash(object_id, version_id, uuid.uuid4())

    assert h1 != h2


def test_none_expected_previous_version_is_stable():
    object_id = "OBJ-REQ-TEST-001"
    version_id = uuid.uuid4()

    h1 = compute_idempotency_payload_hash(object_id, version_id, None)
    h2 = compute_idempotency_payload_hash(object_id, version_id, None)

    assert h1 == h2


def test_different_object_id_changes_hash():
    version_id = uuid.uuid4()
    expected_prev = uuid.uuid4()

    h1 = compute_idempotency_payload_hash("OBJ-A", version_id, expected_prev)
    h2 = compute_idempotency_payload_hash("OBJ-B", version_id, expected_prev)

    assert h1 != h2
