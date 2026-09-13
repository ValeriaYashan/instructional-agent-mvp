"""
DependencyResolver - Paso 6 del Context Engine (§6.6), CR-34.

`PostgresDependencyResolver` consulta `object_relation` +
`instructional_object` + `approved_state_pointer`. No consulta ningun
grafo de dependencias de Gates 5-7 importado - alcance reducido
explicito (Iteration 3 Proposal v0.3).

CODE-CR-39: una relacion solo cuenta como SATISFIED si (a) existe una
fila en object_relation, (b) el objeto destino tiene
InstructionalObject.object_type EXACTAMENTE igual al
required_object_type declarado para esa relacion en el mapeo, y (c) ese
objeto destino tiene una version aprobada. Una relacion que existe pero
apunta a un objeto del tipo incorrecto (o sin tipo declarado) NO
satisface el requisito.

Cardinalidad (correccion secundaria del Human Code Review): cada
relation_type es singular por from_object_id, forzado a nivel de base
de datos por UNIQUE(from_object_id, relation_type) en object_relation
(ver models.py) - por eso `scalar_one_or_none()` es seguro aqui, la
base de datos garantiza que nunca hay mas de una fila que matchee.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.persistence.models import ApprovedStatePointer, InstructionalObject, ObjectRelation


@dataclass(frozen=True)
class DependencyResolution:
    dependency_id: str  # relation_type
    status: str  # 'SATISFIED' | 'BLOCKED_BY_DEPENDENCY'
    to_object_id: str | None  # objeto relacionado, si la relacion existe (sea o no del tipo correcto)


class DependencyResolver(Protocol):
    def resolve(
        self, session: Session, object_id: str, required_relations: list[dict]
    ) -> list[DependencyResolution]:
        ...


class PostgresDependencyResolver:
    def resolve(
        self, session: Session, object_id: str, required_relations: list[dict]
    ) -> list[DependencyResolution]:
        results: list[DependencyResolution] = []
        for requirement in required_relations:
            relation_type = requirement["relation_type"]
            required_object_type = requirement["required_object_type"]

            # Seguro por la UNIQUE(from_object_id, relation_type) de la
            # base de datos - nunca puede haber mas de una fila.
            relation = session.execute(
                select(ObjectRelation).where(
                    ObjectRelation.from_object_id == object_id,
                    ObjectRelation.relation_type == relation_type,
                )
            ).scalar_one_or_none()

            if relation is None:
                results.append(
                    DependencyResolution(
                        dependency_id=relation_type, status="BLOCKED_BY_DEPENDENCY", to_object_id=None
                    )
                )
                continue

            actual_object_type = session.execute(
                select(InstructionalObject.object_type).where(
                    InstructionalObject.object_id == relation.to_object_id
                )
            ).scalar_one_or_none()

            if actual_object_type != required_object_type:
                # CODE-CR-39: la relacion existe pero apunta al tipo
                # incorrecto (o sin tipo) - no satisface el requisito.
                results.append(
                    DependencyResolution(
                        dependency_id=relation_type,
                        status="BLOCKED_BY_DEPENDENCY",
                        to_object_id=relation.to_object_id,
                    )
                )
                continue

            approved_version_id = session.execute(
                select(ApprovedStatePointer.current_approved_version_id).where(
                    ApprovedStatePointer.object_id == relation.to_object_id
                )
            ).scalar_one_or_none()

            status = "SATISFIED" if approved_version_id is not None else "BLOCKED_BY_DEPENDENCY"
            results.append(
                DependencyResolution(
                    dependency_id=relation_type, status=status, to_object_id=relation.to_object_id
                )
            )
        return results
