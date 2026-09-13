"""
CODE-CR-67 (Human Code Review de Build v3) - tests de
compute_allowed_learning_outcome_ids, la whitelist AUTORITATIVA UNICA
que tanto el prompt del proveedor (producer_executor._build_prompt)
como Structural Validation (orchestrator._phase_b_execute ->
producer_output_schema.validate_structural) deben usar identica -
nunca dos calculos independientes que puedan divergir.

Estos tests cubren los puntos (1), (2) y (3) de los 5 exigidos por el
Human Code Review de Build v3:
    1. LO valido en ContextPackage -> aceptado (incluido en la whitelist).
    2. ACTIVITY presente en ContextPackage -> rechazado si aparece en
       learning_outcome_refs (excluido de la whitelist, y por lo tanto
       rechazado por validate_structural).
    3. ID inexistente -> rechazado (no esta en la whitelist calculada
       a partir del ContextPackage real).

Los puntos (4) y (5) se cubren en tests/unit/test_anthropic_producer_adapter.py
(el prompt lista solo LearningOutcome IDs) y en
tests/integration/test_orchestrator.py (el mismo whitelist se usa en
Structural Validation, extremo a extremo contra un AgentRun real).
"""

from __future__ import annotations

from src.persistence.repositories.producer_executor import compute_allowed_learning_outcome_ids
from src.persistence.repositories.producer_output_schema import (
    PRODUCER_OUTPUT_SCHEMA_VERSION,
    validate_structural,
)


def _approved_objects() -> list[dict]:
    return [
        {"object_id": "OBJ-TARGET", "object_type": "LEARNING_OBJECT", "version": 1, "status": "APPROVED_CONTENT", "data": {}},
        {"object_id": "ACT-1", "object_type": "ACTIVITY", "version": 1, "status": "APPROVED_CONTENT", "data": {}},
        {"object_id": "LO-1", "object_type": "LEARNING_OUTCOME", "version": 1, "status": "APPROVED_CONTENT", "data": {}},
    ]


def _output(**overrides) -> dict:
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
# (1) LO valido en ContextPackage -> aceptado
# ---------------------------------------------------------------------------


def test_learning_outcome_id_is_included_in_whitelist():
    whitelist = compute_allowed_learning_outcome_ids(_approved_objects())
    assert whitelist == {"LO-1"}


def test_learning_outcome_ref_is_accepted_by_structural_validation():
    whitelist = compute_allowed_learning_outcome_ids(_approved_objects())
    result = validate_structural(
        _output(learning_outcome_refs=["LO-1"]), "LEARNING_OBJECT", whitelist
    )
    assert result.is_valid is True
    assert result.reason is None


# ---------------------------------------------------------------------------
# (2) ACTIVITY presente en ContextPackage -> rechazado si se referencia
# en learning_outcome_refs, aunque este legitimamente aprobado
# ---------------------------------------------------------------------------


def test_activity_id_excluded_from_whitelist_even_though_approved():
    whitelist = compute_allowed_learning_outcome_ids(_approved_objects())
    assert "ACT-1" not in whitelist
    assert "OBJ-TARGET" not in whitelist  # tampoco el propio target (LEARNING_OBJECT)


def test_activity_ref_in_learning_outcome_refs_is_rejected():
    whitelist = compute_allowed_learning_outcome_ids(_approved_objects())
    result = validate_structural(
        _output(learning_outcome_refs=["ACT-1"]), "LEARNING_OBJECT", whitelist
    )
    assert result.is_valid is False
    assert "ACT-1" in result.reason


# ---------------------------------------------------------------------------
# (3) ID inexistente en el ContextPackage -> rechazado
# ---------------------------------------------------------------------------


def test_nonexistent_id_not_in_whitelist_and_rejected():
    whitelist = compute_allowed_learning_outcome_ids(_approved_objects())
    result = validate_structural(
        _output(learning_outcome_refs=["LO-INVENTED"]), "LEARNING_OBJECT", whitelist
    )
    assert result.is_valid is False
    assert "LO-INVENTED" in result.reason


# ---------------------------------------------------------------------------
# Cobertura adicional: mezcla de tipos, solo LEARNING_OUTCOME sobrevive
# ---------------------------------------------------------------------------


def test_mixed_object_types_only_learning_outcomes_survive_filter():
    approved = _approved_objects() + [
        {"object_id": "LO-2", "object_type": "LEARNING_OUTCOME", "version": 1, "status": "APPROVED_CONTENT", "data": {}},
        {"object_id": "CONTENT-1", "object_type": "CONTENT", "version": 1, "status": "APPROVED_CONTENT", "data": {}},
    ]
    whitelist = compute_allowed_learning_outcome_ids(approved)
    assert whitelist == {"LO-1", "LO-2"}


def test_empty_approved_objects_yields_empty_whitelist():
    assert compute_allowed_learning_outcome_ids([]) == set()
