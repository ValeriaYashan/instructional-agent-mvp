from src.persistence.repositories.producer_output_schema import (
    PRODUCER_OUTPUT_SCHEMA_VERSION,
    validate_structural,
)

_ALLOWED = {"OBJ-TARGET", "ACT-1", "LO-1"}


def _valid_output(**overrides):
    base = {
        "schema_version": PRODUCER_OUTPUT_SCHEMA_VERSION,
        "object_type": "LEARNING_OBJECT",
        "title": "Introduccion a la gestion de riesgos",
        "body": "Contenido de ejemplo suficientemente largo.",
        "learning_outcome_refs": ["LO-1"],
        "metadata": {},
    }
    base.update(overrides)
    return base


def test_valid_output_passes():
    result = validate_structural(_valid_output(), "LEARNING_OBJECT", _ALLOWED)
    assert result.is_valid is True
    assert result.reason is None


def test_extra_field_invalidates_completely():
    result = validate_structural(
        _valid_output(reasoning="paso a paso interno..."), "LEARNING_OBJECT", _ALLOWED
    )
    assert result.is_valid is False
    assert "reasoning" in result.reason


def test_missing_required_field_invalidates():
    output = _valid_output()
    del output["body"]
    result = validate_structural(output, "LEARNING_OBJECT", _ALLOWED)
    assert result.is_valid is False


def test_empty_title_invalidates():
    result = validate_structural(_valid_output(title="   "), "LEARNING_OBJECT", _ALLOWED)
    assert result.is_valid is False


def test_wrong_schema_version_invalidates():
    result = validate_structural(_valid_output(schema_version="v0"), "LEARNING_OBJECT", _ALLOWED)
    assert result.is_valid is False


def test_object_type_mismatch_invalidates():
    result = validate_structural(_valid_output(), "ACTIVITY", _ALLOWED)
    assert result.is_valid is False


def test_reference_outside_context_package_invalidates():
    result = validate_structural(
        _valid_output(learning_outcome_refs=["LO-1", "LO-INVENTED"]), "LEARNING_OBJECT", _ALLOWED
    )
    assert result.is_valid is False
    assert "LO-INVENTED" in result.reason


def test_non_dict_output_invalidates():
    result = validate_structural("not a dict", "LEARNING_OBJECT", _ALLOWED)
    assert result.is_valid is False


def test_metadata_must_be_dict():
    result = validate_structural(_valid_output(metadata=[1, 2, 3]), "LEARNING_OBJECT", _ALLOWED)
    assert result.is_valid is False
