import json
import uuid
from types import SimpleNamespace

import pytest

from src.persistence.repositories.producer_executor import (
    AnthropicProducerAdapter,
    ExecutionContract,
    _build_prompt,
    _extract_structured_output,
)


def _contract(**overrides) -> ExecutionContract:
    base = dict(
        execution_contract_version="execution-contract-v1",
        role="PRODUCER",
        task_type="PRODUCE_LEARNING_OBJECT",
        context_package_id=uuid.uuid4(),
        context_package_hash="hash-real-123",
        provider="anthropic",
        model_identifier="claude-test-model",
        instructions_version="instructions-v1",
        output_schema_version="producer-output-v1",
        provider_timeout_seconds=30,
    )
    base.update(overrides)
    return ExecutionContract(**base)


def _context_package(**overrides) -> dict:
    base = {
        "context_package_id": "cp-1",
        "context_package_hash": "hash-real-123",
        "context_contract_version": "context-engine-v1",
        "approved_objects": [{"object_id": "OBJ-1", "object_type": "ACTIVITY", "version": 1, "status": "APPROVED_CONTENT", "data": {}}],
        "relations": [],
        "evidence": [],
        "dependency_status": [],
        "gaps": [],
    }
    base.update(overrides)
    return base


class _FakeAnthropicMessage:
    def __init__(self, text: str):
        self.content = [SimpleNamespace(text=text)]


class _FakeClientOK:
    def __init__(self):
        self.calls = []

        class _Messages:
            def create(inner_self, **kwargs):
                self.calls.append(kwargs)
                return _FakeAnthropicMessage(
                    json.dumps(
                        {
                            "schema_version": "producer-output-v1",
                            "object_type": "ACTIVITY",
                            "title": "Titulo",
                            "body": "Cuerpo",
                            "learning_outcome_refs": [],
                            "metadata": {},
                        }
                    )
                )

        self.messages = _Messages()


class _FakeClientMalformed:
    class messages:
        @staticmethod
        def create(**kwargs):
            return _FakeAnthropicMessage("esto no es JSON valido {{{")


class _FakeClientNoContent:
    class messages:
        @staticmethod
        def create(**kwargs):
            return SimpleNamespace(content=[])


class _FakeClientRaises:
    class messages:
        @staticmethod
        def create(**kwargs):
            raise TimeoutError("simulated provider timeout")


def test_build_prompt_includes_real_context_package_and_contract_fields():
    contract = _contract(instructions_version="instructions-v7")
    package = _context_package(context_package_id="cp-real-id", context_package_hash="hash-real-999")

    prompt = _build_prompt(contract, package)

    assert "cp-real-id" in prompt
    assert "hash-real-999" in prompt
    assert "instructions-v7" in prompt
    assert "producer-output-v1" in prompt
    assert "OBJ-1" in prompt  # contenido real de approved_objects, no un placeholder
    assert "chain-of-thought" in prompt.lower()


def test_build_prompt_defines_explicit_output_contract():
    """
    CODE-CR-65: el prompt no puede limitarse a nombrar el
    schema_version por su nombre - debe declarar explicitamente el
    object_type autoritativo real, cada campo obligatorio, la
    prohibicion de campos adicionales, y las referencias permitidas.
    """
    contract = _contract(
        output_schema_version="producer-output-v1",
        instructions_version="instructions-v3",
        expected_object_type="LEARNING_OBJECT",
    )
    package = _context_package(
        approved_objects=[
            {"object_id": "ACT-1", "object_type": "ACTIVITY", "version": 1, "status": "APPROVED_CONTENT", "data": {}},
            {"object_id": "LO-1", "object_type": "LEARNING_OUTCOME", "version": 1, "status": "APPROVED_CONTENT", "data": {}},
        ]
    )

    prompt = _build_prompt(contract, package)

    # object_type autoritativo real, no solo el nombre del schema.
    assert "LEARNING_OBJECT" in prompt
    assert prompt.count("LEARNING_OBJECT") >= 1

    # Cada campo obligatorio del Producer Output Schema v1 declarado explicitamente.
    for field in ("schema_version", "object_type", "title", "body", "learning_outcome_refs", "metadata"):
        assert field in prompt

    # schema_version referenciado explicitamente por su valor exacto.
    assert "producer-output-v1" in prompt

    # Prohibicion explicita de campos adicionales.
    assert "additionalProperties" in prompt or "No additional" in prompt or "NO additional" in prompt

    # Prohibicion explicita de markdown fences y chain-of-thought.
    assert "chain-of-thought" in prompt.lower()
    assert "markdown" in prompt.lower()

    # Referencias permitidas listadas explicitamente (no solo mencionadas
    # dentro de APPROVED_OBJECTS crudo).
    assert "ALLOWED_REFERENCE_OBJECT_IDS" in prompt
    assert "LO-1" in prompt

    # CODE-CR-67 (Human Code Review de Build v3): ALLOWED_REFERENCE_OBJECT_IDS
    # debe listar EXCLUSIVAMENTE object_id de tipo LEARNING_OUTCOME -
    # ACT-1 (ACTIVITY) esta legitimamente presente en APPROVED_OBJECTS
    # (aparece en el dump completo mas abajo en el prompt, con fines de
    # contexto), pero NUNCA debe aparecer dentro de la whitelist de
    # referencias permitidas.
    allowed_ids_line = next(
        line for line in prompt.splitlines() if line.startswith("ALLOWED_REFERENCE_OBJECT_IDS")
    )
    assert "LO-1" in allowed_ids_line
    assert "ACT-1" not in allowed_ids_line

    # ContextPackage real (no placeholder) e instructions_version.
    assert package["context_package_id"] in prompt
    assert package["context_package_hash"] in prompt
    assert "instructions-v3" in prompt


def test_extract_structured_output_parses_valid_json():
    message = _FakeAnthropicMessage(json.dumps({"schema_version": "producer-output-v1"}))
    parsed = _extract_structured_output(message)
    assert parsed == {"schema_version": "producer-output-v1"}


def test_extract_structured_output_returns_none_for_malformed_json():
    message = _FakeAnthropicMessage("no es json {{{")
    assert _extract_structured_output(message) is None


def test_extract_structured_output_returns_none_for_non_object_json():
    message = _FakeAnthropicMessage(json.dumps([1, 2, 3]))
    assert _extract_structured_output(message) is None


def test_execute_happy_path_calls_client_with_correct_model_and_timeout():
    client = _FakeClientOK()
    adapter = AnthropicProducerAdapter(client_factory=lambda: client)
    contract = _contract(model_identifier="claude-specific-model", provider_timeout_seconds=17)
    package = _context_package()

    result = adapter.execute(contract, package)

    assert result.technical_error is None
    assert result.parsed_output["schema_version"] == "producer-output-v1"
    assert len(client.calls) == 1
    assert client.calls[0]["model"] == "claude-specific-model"
    assert client.calls[0]["timeout"] == 17
    assert "OBJ-1" in client.calls[0]["messages"][0]["content"]  # prompt con contexto real


def test_execute_malformed_response_is_nonparseable_not_silently_valid():
    adapter = AnthropicProducerAdapter(client_factory=lambda: _FakeClientMalformed())
    result = adapter.execute(_contract(), _context_package())

    assert result.parsed_output is None
    assert result.technical_error == "respuesta no parseable"


def test_execute_empty_content_is_nonparseable():
    adapter = AnthropicProducerAdapter(client_factory=lambda: _FakeClientNoContent())
    result = adapter.execute(_contract(), _context_package())

    assert result.parsed_output is None
    assert result.technical_error == "respuesta no parseable"


def test_execute_sdk_exception_yields_technical_error():
    adapter = AnthropicProducerAdapter(client_factory=lambda: _FakeClientRaises())
    result = adapter.execute(_contract(), _context_package())

    assert result.parsed_output is None
    assert result.technical_error is not None
    assert "timeout" in result.technical_error.lower()
