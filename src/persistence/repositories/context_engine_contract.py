"""
Constantes versionadas del contrato del Context Engine (Iteracion 3).

CONTEXT_CONTRACT_VERSION (CR-38): identifica la version del contrato
semantico determinístico Context Engine/Context Package bajo el cual se
ensambla un context_package. Participa del hash canonico
(context_package_hash) - ver context_engine.py.

TASK_TYPE_CONTEXT_MAPPING (Paso 2, §6.6): tabla de mapeo tarea -> tipos
de objeto requeridos, relaciones requeridas (cada una con el tipo de
objeto que su destino DEBE tener - CODE-CR-41... CODE-CR-39) y clases de
evidencia requeridas. Alcance de Iteracion 3: unicamente
'PRODUCE_LEARNING_OBJECT'.

CODE-CR-39: cada entrada de required_relations declara explicitamente
required_object_type - el tipo de InstructionalObject que el destino de
esa relacion DEBE tener para que la relacion cuente como satisfecha.
Una relacion que existe pero apunta a un objeto de tipo distinto (o sin
tipo declarado) NO satisface el requisito - se trata como
BLOCKED_BY_DEPENDENCY, igual que una relacion inexistente.

required_object_types permanece como el conjunto cerrado de tipos
validos para esta tarea; se usa en el Paso 2 para verificar que todo
required_object_type declarado en required_relations pertenece a ese
conjunto (consistencia interna del propio contrato, no dato sin uso).
"""

from __future__ import annotations

CONTEXT_CONTRACT_VERSION = "context-engine-v1"

TASK_TYPE_CONTEXT_MAPPING: dict[str, dict] = {
    "PRODUCE_LEARNING_OBJECT": {
        "required_object_types": ["LEARNING_OUTCOME", "ACTIVITY"],
        "required_relations": [
            {"relation_type": "PARENT_ACTIVITY", "required_object_type": "ACTIVITY"},
            {"relation_type": "LEARNING_OUTCOME", "required_object_type": "LEARNING_OUTCOME"},
        ],
        "required_evidence": [
            {
                "evidence_class": "E1",
                "requirement_level": "REQUIRED",
                "blocking_behavior": "BLOCKING",
            },
            {
                "evidence_class": "E3",
                "requirement_level": "OPTIONAL",
                "blocking_behavior": "NON_BLOCKING",
            },
        ],
    }
}
