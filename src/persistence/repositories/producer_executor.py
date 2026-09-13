"""
ProducerExecutor - interfaz provider-neutral (Iteracion 4, Proposal v0.5
S12). Ninguna clase de dominio nombra un proveedor concreto salvo el
adaptador mismo, que es infraestructura, no dominio.

Cada implementacion DEBE aplicar un timeout duro <= execution_contract.
provider_timeout_seconds - nunca confiar en el timeout por defecto,
potencialmente no acotado, del SDK del proveedor (S8.10).

CODE-CR-60 (Human Code Review de Build v1): execute() recibe el
ContextPackage REAL (materializado por el Orchestrator, ver
orchestrator.py) como segundo argumento - nunca un dict vacio.

CODE-CR-62 (Human Code Review de Build v1): AnthropicProducerAdapter
implementa _build_prompt/_extract_structured_output de forma real y
determinística, testeable via client injection sin credenciales ni
llamadas de red reales.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol
from uuid import UUID


@dataclass(frozen=True)
class ExecutionContract:
    execution_contract_version: str
    role: str
    task_type: str
    context_package_id: UUID
    context_package_hash: str
    provider: str
    model_identifier: str
    instructions_version: str
    output_schema_version: str
    provider_timeout_seconds: int
    # CODE-CR-65 (Human Code Review de Build v2): identidad autoritativa
    # del InstructionalObject destino - metadata de ejecucion
    # provider-neutral, NUNCA se agrega al ContextPackage persistido
    # (que permanece inmutable e inalterado). El Producer nunca elige
    # este valor, solo lo recibe como parte del contrato de salida
    # exigido.
    expected_object_type: Optional[str] = None


@dataclass(frozen=True)
class ProducerExecutionResult:
    raw_response: Any
    parsed_output: Optional[dict]
    technical_error: Optional[str]


class ProducerExecutor(Protocol):
    def execute(
        self, execution_contract: ExecutionContract, context_package: dict
    ) -> ProducerExecutionResult:
        ...


class FakeProducerAdapter:
    """
    Adaptador de pruebas. `responses` es una lista de "guiones": cada
    elemento es o bien un dict (se devuelve como parsed_output exitoso),
    o bien una excepcion/callable que simula un fallo tecnico
    (incluido, si se desea, un retraso simulado via `delay_seconds` para
    ejercitar S8.10 sin depender de un proveedor real). Se consume un
    elemento por llamada a execute(); si se agota la lista, repite el
    ultimo.

    CODE-CR-60: registra el context_package REAL recibido en cada
    llamada (received_context_packages) para que los tests puedan
    verificar que el Producer efectivamente recibe el ContextPackage
    correcto - nunca un placeholder.
    """

    def __init__(self, responses: list, delay_seconds: float = 0.0):
        self._responses = list(responses)
        self._call_count = 0
        self._delay_seconds = delay_seconds
        self.received_execution_contracts: list[ExecutionContract] = []
        self.received_context_packages: list[dict] = []

    def execute(
        self, execution_contract: ExecutionContract, context_package: dict
    ) -> ProducerExecutionResult:
        self.received_execution_contracts.append(execution_contract)
        self.received_context_packages.append(context_package)

        if self._delay_seconds:
            time.sleep(min(self._delay_seconds, execution_contract.provider_timeout_seconds))

        index = min(self._call_count, len(self._responses) - 1)
        scripted = self._responses[index]
        self._call_count += 1

        if isinstance(scripted, Exception):
            return ProducerExecutionResult(
                raw_response=None, parsed_output=None, technical_error=str(scripted)
            )
        if callable(scripted):
            scripted = scripted()
        if scripted is None:
            return ProducerExecutionResult(
                raw_response=None, parsed_output=None, technical_error="respuesta no parseable"
            )
        return ProducerExecutionResult(raw_response=scripted, parsed_output=scripted, technical_error=None)


def compute_allowed_learning_outcome_ids(approved_objects: list[dict]) -> set[str]:
    """
    CODE-CR-67 (Human Code Review de Build v3): whitelist AUTORITATIVA
    UNICA de object_id que learning_outcome_refs puede referenciar.
    Filtra exclusivamente los objetos de approved_objects cuyo
    object_type == 'LEARNING_OUTCOME' - el contrato semantico de
    Producer Output Schema v1 es que learning_outcome_refs solo puede
    apuntar a LearningOutcome ya resueltos en el ContextPackage, nunca a
    ACTIVITY, CONTENT, ASSESSMENT ni ningun otro object_type presente en
    el mismo paquete, aunque ese otro objeto este legitimamente
    aprobado y presente.

    Esta es la UNICA funcion que calcula ese universo - tanto
    _build_prompt() (mas abajo, para ALLOWED_REFERENCE_OBJECT_IDS en el
    prompt del proveedor) como
    orchestrator._phase_b_execute()/producer_output_schema.validate_structural
    (Structural Validation) DEBEN llamar exactamente a esta funcion, no
    recalcular el filtro por su cuenta - evita que ambos calculos
    puedan divergir silenciosamente.
    """
    return {
        o["object_id"]
        for o in approved_objects
        if o.get("object_type") == "LEARNING_OUTCOME"
    }


def _build_prompt(execution_contract: ExecutionContract, context_package: dict) -> str:
    """
    CODE-CR-62/CODE-CR-65: usa el ContextPackage REAL (nunca un
    placeholder), respeta instructions_version, y define EXPLICITAMENTE
    el contrato de salida exigido - no basta con nombrar el
    schema_version, el modelo no lo conoce por su nombre. Incluye un
    ejemplo de forma exacta con el object_type autoritativo ya
    declarado (expected_object_type), la lista cerrada de
    learning_outcome_refs permitidas, y prohibiciones explicitas de
    campos adicionales, razonamiento, y cercos de markdown.

    CODE-CR-67: ALLOWED_REFERENCE_OBJECT_IDS lista EXCLUSIVAMENTE
    object_id de tipo LEARNING_OUTCOME (via
    compute_allowed_learning_outcome_ids) - nunca ACTIVITY ni otro tipo
    presente en approved_objects, aunque APPROVED_OBJECTS (el dump
    completo mas abajo) si los incluya a todos para dar contexto.
    """
    allowed_reference_ids = sorted(compute_allowed_learning_outcome_ids(context_package["approved_objects"]))
    example_shape = {
        "schema_version": execution_contract.output_schema_version,
        "object_type": execution_contract.expected_object_type,
        "title": "<non-empty string>",
        "body": "<non-empty string>",
        "learning_outcome_refs": ["<object_id from ALLOWED_REFERENCE_OBJECT_IDS below>"],
        "metadata": {},
    }

    return (
        f"CONTEXT_PACKAGE_ID: {context_package['context_package_id']}\n"
        f"CONTEXT_PACKAGE_HASH: {context_package['context_package_hash']}\n"
        f"CONTEXT_CONTRACT_VERSION: {context_package.get('context_contract_version', '')}\n"
        f"INSTRUCTIONS_VERSION: {execution_contract.instructions_version}\n\n"
        "OUTPUT CONTRACT - produce EXCLUSIVELY a single JSON object with "
        "EXACTLY the following six fields and no others:\n"
        f"{json.dumps(example_shape, indent=2)}\n\n"
        "Field rules (all mandatory, no exceptions):\n"
        f"- schema_version: MUST be exactly {execution_contract.output_schema_version!r}. "
        "Do not use any other value.\n"
        f"- object_type: MUST be exactly {execution_contract.expected_object_type!r} - "
        "this is the authoritative type already assigned to this object; "
        "you do not choose or infer it.\n"
        "- title: a non-empty string.\n"
        "- body: a non-empty string.\n"
        "- learning_outcome_refs: a list of object_id strings. Each value "
        "MUST be one of ALLOWED_REFERENCE_OBJECT_IDS below. Never invent, "
        "guess, or reference any object_id outside this list.\n"
        "- metadata: a JSON object (may be empty: {}).\n"
        "- NO additional top-level fields are allowed beyond these six - "
        "additionalProperties is forbidden.\n"
        "- Output ONLY the JSON object itself: no reasoning, no "
        "explanation, no preamble, no chain-of-thought of any kind, and "
        "no markdown code fences (no triple-backtick blocks of any kind) - "
        "nothing before or after the JSON object.\n\n"
        f"ALLOWED_REFERENCE_OBJECT_IDS: {json.dumps(allowed_reference_ids)}\n\n"
        f"APPROVED_OBJECTS: {json.dumps(context_package['approved_objects'])}\n"
        f"RELATIONS: {json.dumps(context_package['relations'])}\n"
        f"EVIDENCE: {json.dumps(context_package['evidence'])}\n"
        f"DEPENDENCY_STATUS: {json.dumps(context_package['dependency_status'])}\n"
        f"GAPS: {json.dumps(context_package['gaps'])}\n"
    )


def _extract_response_text(raw_response: Any) -> Optional[str]:
    """
    Extraccion deterministica del texto de una respuesta del SDK de
    Anthropic (bloques de contenido, cada uno con atributo .text) - o
    de cualquier objeto "fake" con la misma forma minima, para permitir
    testear sin el SDK real instalado (CODE-CR-62, client injection).
    """
    content = getattr(raw_response, "content", None)
    if not content:
        return None
    parts = [getattr(block, "text", None) for block in content]
    parts = [p for p in parts if p]
    if not parts:
        return None
    return "".join(parts)


def _extract_structured_output(raw_response: Any) -> Optional[dict]:
    """
    CODE-CR-62: obtiene el payload estructurado de forma deterministica.
    Un error de parseo (texto ausente, JSON invalido, o JSON que no es
    un objeto) devuelve None - NUNCA convierte silenciosamente texto
    arbitrario en un output valido; la validacion de contrato/schema
    ocurre despues, en producer_output_schema.validate_structural.
    """
    text = _extract_response_text(raw_response)
    if text is None:
        return None
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


class AnthropicProducerAdapter:
    """
    Adaptador real de infraestructura - requiere que se le inyecte un
    cliente ya configurado (credenciales gestionadas como secretos de
    entorno, fuera del dominio - S15). Aplica un timeout duro exacto
    igual a execution_contract.provider_timeout_seconds. Testeable sin
    credenciales ni red real mediante client injection - ver
    tests/unit/test_anthropic_producer_adapter.py.
    """

    def __init__(self, client_factory: Callable[[], Any], max_tokens: int = 4096):
        self._client_factory = client_factory
        self._max_tokens = max_tokens

    def execute(
        self, execution_contract: ExecutionContract, context_package: dict
    ) -> ProducerExecutionResult:
        client = self._client_factory()
        prompt = _build_prompt(execution_contract, context_package)
        try:
            raw_response = client.messages.create(
                model=execution_contract.model_identifier,
                max_tokens=self._max_tokens,
                timeout=execution_contract.provider_timeout_seconds,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as exc:  # noqa: BLE001 - cualquier error del SDK es TECHNICAL_FAILURE
            return ProducerExecutionResult(raw_response=None, parsed_output=None, technical_error=str(exc))

        parsed = _extract_structured_output(raw_response)
        if parsed is None:
            return ProducerExecutionResult(
                raw_response=raw_response, parsed_output=None, technical_error="respuesta no parseable"
            )
        return ProducerExecutionResult(raw_response=raw_response, parsed_output=parsed, technical_error=None)
