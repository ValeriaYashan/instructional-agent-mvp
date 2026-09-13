"""
Producer Output Schema v1 y Structural Validation - Iteracion 4,
Proposal v0.5 S3/S12 (CR-50, CR-53).

"Structural Validation" significa EXCLUSIVAMENTE: validacion de schema,
de contrato (schema_version, object_type) y referencial
(learning_outcome_refs contra ContextPackage). NO es QA instruccional/
pedagogica (Iteracion 5), ni aprobacion de Routing, ni Human Approval.

additionalProperties=false estricto: ningun campo de razonamiento/
chain-of-thought es parte del contrato - su sola presencia invalida el
output completo. Un output invalido nunca se acepta parcialmente.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

PRODUCER_OUTPUT_SCHEMA_VERSION = "producer-output-v1"

_REQUIRED_FIELDS = {"schema_version", "object_type", "title", "body", "learning_outcome_refs", "metadata"}
_MAX_TITLE_LENGTH = 300


@dataclass(frozen=True)
class StructuralValidationResult:
    is_valid: bool
    reason: str | None = None  # motivo legible si is_valid=False


def validate_structural(
    raw_output: Any,
    expected_object_type: str,
    allowed_reference_object_ids: set[str],
) -> StructuralValidationResult:
    """
    Valida raw_output contra Producer Output Schema v1. allowed_reference_object_ids
    es el conjunto de object_id ya resueltos en el ContextPackage usado
    para esta ejecucion (approved_objects) - un learning_outcome_refs
    que referencie algo fuera de ese conjunto invalida el output
    completo (D10.5: el Producer no puede inventar relaciones
    autoritativas).
    """
    if not isinstance(raw_output, dict):
        return StructuralValidationResult(False, "output no es un objeto JSON")

    extra_keys = set(raw_output.keys()) - _REQUIRED_FIELDS
    if extra_keys:
        return StructuralValidationResult(
            False, f"campos no declarados en el schema: {sorted(extra_keys)}"
        )

    missing_keys = _REQUIRED_FIELDS - set(raw_output.keys())
    if missing_keys:
        return StructuralValidationResult(False, f"campos requeridos ausentes: {sorted(missing_keys)}")

    if raw_output["schema_version"] != PRODUCER_OUTPUT_SCHEMA_VERSION:
        return StructuralValidationResult(
            False,
            f"schema_version no coincide: esperado {PRODUCER_OUTPUT_SCHEMA_VERSION!r}, "
            f"recibido {raw_output['schema_version']!r}",
        )

    if raw_output["object_type"] != expected_object_type:
        return StructuralValidationResult(
            False,
            f"object_type no coincide con InstructionalObject.object_type ya declarado: "
            f"esperado {expected_object_type!r}, recibido {raw_output['object_type']!r}",
        )

    title = raw_output["title"]
    if not isinstance(title, str) or not title.strip():
        return StructuralValidationResult(False, "title vacio o de tipo incorrecto")
    if len(title) > _MAX_TITLE_LENGTH:
        return StructuralValidationResult(False, "title excede la longitud maxima permitida")

    body = raw_output["body"]
    if not isinstance(body, str) or not body.strip():
        return StructuralValidationResult(False, "body vacio o de tipo incorrecto")

    refs = raw_output["learning_outcome_refs"]
    if not isinstance(refs, list) or not all(isinstance(r, str) for r in refs):
        return StructuralValidationResult(False, "learning_outcome_refs no es una lista de strings")
    invalid_refs = [r for r in refs if r not in allowed_reference_object_ids]
    if invalid_refs:
        return StructuralValidationResult(
            False,
            f"learning_outcome_refs referencia objetos fuera del ContextPackage: {invalid_refs}",
        )

    metadata = raw_output["metadata"]
    if not isinstance(metadata, dict):
        return StructuralValidationResult(False, "metadata no es un objeto")

    return StructuralValidationResult(True, None)
