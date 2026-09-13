import uuid

from src.persistence.repositories.orchestrator import compute_producer_request_hash


def _args(**overrides):
    base = dict(
        task_type="PRODUCE_LEARNING_OBJECT",
        target_object_id="OBJ-1",
        context_package_id=uuid.uuid4(),
        provider="anthropic",
        model_identifier="claude-x",
        instructions_version="v1",
        output_schema_version="producer-output-v1",
    )
    base.update(overrides)
    return base


def test_identical_input_same_hash():
    args = _args()
    assert compute_producer_request_hash(**args) == compute_producer_request_hash(**args)


def test_different_provider_changes_hash():
    args = _args()
    h1 = compute_producer_request_hash(**args)
    args["provider"] = "openai"
    h2 = compute_producer_request_hash(**args)
    assert h1 != h2


def test_different_model_changes_hash():
    args = _args()
    h1 = compute_producer_request_hash(**args)
    args["model_identifier"] = "claude-y"
    h2 = compute_producer_request_hash(**args)
    assert h1 != h2


def test_different_instructions_version_changes_hash():
    args = _args()
    h1 = compute_producer_request_hash(**args)
    args["instructions_version"] = "v2"
    h2 = compute_producer_request_hash(**args)
    assert h1 != h2


def test_different_context_package_id_changes_hash():
    args = _args()
    h1 = compute_producer_request_hash(**args)
    args["context_package_id"] = uuid.uuid4()
    h2 = compute_producer_request_hash(**args)
    assert h1 != h2
