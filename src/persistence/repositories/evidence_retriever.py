"""
EvidenceRetriever - Paso 7 del Context Engine (§6.6), CR-34.

`PostgresEvidenceRetriever` es el UNICO adaptador de Iteracion 3:
busqueda EXACTA por metadatos (object_id + evidence_class) sobre
`evidence_source`. Descrito con precision: esto NO es retrieval
semantico. `pgvector` / Knowledge Base completa quedan diferidos.

CODE-CR-41: EvidenceResolution preserva la tupla COMPLETA de Evidence
Sufficiency exigida por HAD-04/§6.11 (requirement_level,
blocking_behavior, resolution_status), no solo resolution_status.

CODE-CR-44 (Human Code Review): vocabulario de resolution_status
restaurado al aprobado en Iteration 3 Context Engine Proposal v0.3:
'FOUND' | 'MISSING' - NO 'SATISFIED'/'MISSING' (ese vocabulario es de
Dependency Resolution, un concepto independiente que no se confunde
con Evidence Retrieval).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.persistence.models import EvidenceSource


@dataclass(frozen=True)
class EvidenceResolution:
    evidence_class: str
    requirement_level: str  # 'REQUIRED' | 'OPTIONAL' - HAD-04/§6.11
    blocking_behavior: str  # 'BLOCKING' | 'NON_BLOCKING' - HAD-04/§6.11
    resolution_status: str  # 'FOUND' | 'MISSING' - CODE-CR-44
    source_id: str | None


class EvidenceRetriever(Protocol):
    def retrieve(
        self,
        session: Session,
        object_id: str,
        evidence_class: str,
        requirement_level: str,
        blocking_behavior: str,
    ) -> EvidenceResolution:
        ...


class PostgresEvidenceRetriever:
    """
    Busqueda exacta (igualdad de columnas) sobre evidence_source. Si
    existe mas de una fuente para el mismo (object_id, evidence_class),
    se toma la primera por source_id ascendente - determinismo simple,
    suficiente para este alcance reducido.
    """

    def retrieve(
        self,
        session: Session,
        object_id: str,
        evidence_class: str,
        requirement_level: str,
        blocking_behavior: str,
    ) -> EvidenceResolution:
        source = session.execute(
            select(EvidenceSource)
            .where(
                EvidenceSource.object_id == object_id,
                EvidenceSource.evidence_class == evidence_class,
            )
            .order_by(EvidenceSource.source_id.asc())
        ).scalars().first()

        resolution_status = "FOUND" if source is not None else "MISSING"
        return EvidenceResolution(
            evidence_class=evidence_class,
            requirement_level=requirement_level,
            blocking_behavior=blocking_behavior,
            resolution_status=resolution_status,
            source_id=source.source_id if source is not None else None,
        )
