"""
Context Engine - Iteracion 3 (build v2, corregido tras Human Code Review).

Implementa el contrato determinístico de 10 pasos (§6.6, Agent
Specification v1.1) para el unico task_type ya soportado por el
catalogo cerrado: PRODUCE_LEARNING_OBJECT. Ver
context_engine_contract.py para la tabla de mapeo versionada del Paso 2.

Interpretacion aplicada sobre que fallos del contrato de 10 pasos
IMPIDEN la creacion de un context_package versus cuales fluyen hacia
dentro de el como gaps/dependency_status/evidence:

  BLOQUEAN el ensamblado - la transaccion hace COMMIT de un resultado
  REJECTED en idempotency_operation (para permitir replay identico),
  pero NO se crea ningun context_package:
    - MAPPING_UNDEFINED       (Paso 2: "no se infiere")
    - NEWER_VERSION_AVAILABLE (Paso 4: "bloquea", explicito; comparado
      por version_number, no por desigualdad de UUID - CODE-CR-40)
    - INVALID_OBJECT_VERSION  (pinned_version_id no existe o pertenece
      a otro object_id - vocabulario ya aprobado, reutilizado de
      Iteracion 2 CR-26/CR-28, no inventado - CODE-CR-40)

  Caso especial - BLOQUEA pero NUNCA toca la base de datos:
    - TASK_TYPE_UNRESOLVED (Paso 1: Task Intake lo rechaza ANTES de
      cualquier acceso a idempotency_operation u otra tabla - igual que
      en Iteracion 2. NUNCA se persiste, a diferencia de los tres
      anteriores.)

  FLUYEN hacia dentro del context_package (el ensamblado SI se
  completa, con el problema anotado explicitamente como gap):
    - RELATION_UNRESOLVED    (Paso 3: "pasa a evaluacion de dependencia")
    - NO_APPROVED_VERSION    (Paso 4: "se convierte en GAP")
    - BLOCKED_BY_DEPENDENCY  (Paso 6, incluye CODE-CR-39: relacion que
      apunta a un objeto del tipo incorrecto)
    - APPROVAL_INSUFFICIENT (Paso 5: unicamente se ejercita para un
      pinned_version_id cuyo status no es APPROVED_CONTENT - CODE-CR-40;
      la resolucion via approved_state_pointer nunca la ejercita, por
      construccion de Iteracion 1)
    - evidencia MISSING      (Paso 7/8, Evidence Sufficiency §6.11)

CODE-CR-41: cada entrada de evidence[] preserva la tupla COMPLETA de
Evidence Sufficiency (requirement_level, blocking_behavior,
resolution_status), no solo resolution_status.

CR-35 (idempotencia de la solicitud != frescura del contexto): mismo
idempotency_key + mismo payload semantico de solicitud SIEMPRE hace
replay del mismo context_package_id ya ensamblado.

CR-37 (consistencia de instantanea): todo el ensamblado ocurre dentro
de una unica transaccion en aislamiento REPEATABLE READ. Ante un
CONFLICTO DE SERIALIZACION real (SQLSTATE 40001/40P01), se reintenta el
intento completo - CUALQUIER OTRO OperationalError se propaga (no se
retiene como si fuera un conflicto de concurrencia; correccion
secundaria del Human Code Review).

CR-38: el hash canonico incluye context_contract_version + task_type +
target_object_id + pinned_version_id + los resultados canonicos (orden
deterministico por clave natural) de los Pasos 2-8. Excluye
context_package_id, assembled_at y cualquier otro metadato de ejecucion.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Optional

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from src.domain.context_result import ContextAssemblyOutcome, ContextAssemblyResult
from src.persistence.models import (
    ApprovedStatePointer,
    AuditEvent,
    ContextPackage,
    IdempotencyOperation,
    InstructionalObject,
    ObjectVersion,
)
from src.persistence.repositories.context_engine_contract import (
    CONTEXT_CONTRACT_VERSION,
    TASK_TYPE_CONTEXT_MAPPING,
)
from src.persistence.repositories.dependency_resolver import DependencyResolver, PostgresDependencyResolver
from src.persistence.repositories.evidence_retriever import EvidenceRetriever, PostgresEvidenceRetriever
from src.persistence.repositories.task_intake import is_known_task_type

_MAX_OWNERSHIP_RETRIES = 5
_OPERATION_TYPE = "CONTEXT_ASSEMBLY"

# Correccion secundaria: SOLO estos SQLSTATE de PostgreSQL se tratan
# como conflictos de concurrencia reintentables bajo REPEATABLE READ
# (CR-37). Cualquier otro OperationalError se propaga tal cual.
_RETRYABLE_POSTGRES_SQLSTATES = {"40001", "40P01"}  # serialization_failure, deadlock_detected


def _is_retryable_serialization_error(exc: OperationalError) -> bool:
    sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
    return sqlstate in _RETRYABLE_POSTGRES_SQLSTATES


def compute_request_idempotency_hash(
    task_type: str, target_object_id: str, pinned_version_id: Optional[uuid.UUID]
) -> str:
    """Hash de IDENTIDAD DE LA SOLICITUD (CR-35/CR-18) - ver docstring del modulo."""
    raw = "|".join(
        [task_type, target_object_id, str(pinned_version_id) if pinned_version_id else "NULL"]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_context_package_hash(
    context_contract_version: str,
    task_type: str,
    target_object_id: str,
    pinned_version_id: Optional[uuid.UUID],
    approved_objects: list[dict],
    relations: list[dict],
    evidence: list[dict],
    dependency_status: list[dict],
    gaps: list[dict],
) -> str:
    """Hash CANONICO SEMANTICO del contenido (CR-36/CR-38) - ver docstring del modulo."""
    canonical = {
        "context_contract_version": context_contract_version,
        "task_type": task_type,
        "target_object_id": target_object_id,
        "pinned_version_id": str(pinned_version_id) if pinned_version_id else None,
        "approved_objects": sorted(approved_objects, key=lambda o: o["object_id"]),
        "relations": sorted(
            relations, key=lambda r: (r["from_id"], r["to_id"] or "", r["relation_type"])
        ),
        "evidence": sorted(evidence, key=lambda e: (e["evidence_class"], e["source_id"] or "")),
        "dependency_status": sorted(dependency_status, key=lambda d: d["dependency_id"]),
        "gaps": sorted(gaps, key=lambda g: g["gap_id"]),
    }
    serialized = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def assemble_context(
    session: Session,
    task_type: str,
    target_object_id: str,
    idempotency_key: str,
    pinned_version_id: Optional[uuid.UUID] = None,
    dependency_resolver: Optional[DependencyResolver] = None,
    evidence_retriever: Optional[EvidenceRetriever] = None,
) -> ContextAssemblyResult:
    dependency_resolver = dependency_resolver or PostgresDependencyResolver()
    evidence_retriever = evidence_retriever or PostgresEvidenceRetriever()

    # Paso 1 (TASK): Task Intake rechaza ANTES de tocar cualquier tabla.
    # Nunca se persiste - no es un "replay", es un rechazo puramente
    # sincrono de entrada (igual que en Iteracion 2).
    if not is_known_task_type(task_type):
        return ContextAssemblyResult(outcome=ContextAssemblyOutcome.TASK_TYPE_UNRESOLVED)

    request_hash = compute_request_idempotency_hash(task_type, target_object_id, pinned_version_id)

    # CODE-CR-46 / CR-37:
    # assemble_context owns its transaction boundary.  If the caller has
    # already started a transaction, REPEATABLE READ cannot be guaranteed
    # without altering or discarding caller-owned work.
    if session.in_transaction():
        raise RuntimeError(
            "assemble_context requires a Session with no active transaction "
            "so CR-37 REPEATABLE READ can be guaranteed."
        )

    for _ in range(_MAX_OWNERSHIP_RETRIES):
        try:
            # First PostgreSQL statement of every attempt.  This configures
            # the transaction itself rather than trying to mutate an already
            # established SQLAlchemy Connection.
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))

            reservation_won = session.execute(
                pg_insert(IdempotencyOperation)
                .values(
                    operation_type=_OPERATION_TYPE,
                    idempotency_key=idempotency_key,
                    idempotency_payload_hash=request_hash,
                    status="CLAIMED",
                    commit_id=None,
                    transition_id=None,
                    context_package_id=None,
                    rejected_reason=None,
                )
                .on_conflict_do_nothing(index_elements=["operation_type", "idempotency_key"])
                .returning(IdempotencyOperation.idempotency_key)
            ).scalar_one_or_none()

            if reservation_won is not None:
                return _proceed_as_owner(
                    session=session,
                    task_type=task_type,
                    target_object_id=target_object_id,
                    pinned_version_id=pinned_version_id,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    dependency_resolver=dependency_resolver,
                    evidence_retriever=evidence_retriever,
                )

            session.rollback()
            existing = session.execute(
                select(IdempotencyOperation).where(
                    IdempotencyOperation.operation_type == _OPERATION_TYPE,
                    IdempotencyOperation.idempotency_key == idempotency_key,
                )
            ).scalar_one_or_none()

            if existing is None:
                # The SELECT above autobegan a transaction.  End it before
                # retrying so the next attempt can establish REPEATABLE READ
                # as its first PostgreSQL statement.
                session.rollback()
                continue

            if existing.idempotency_payload_hash != request_hash:
                session.rollback()
                return ContextAssemblyResult(outcome=ContextAssemblyOutcome.IDEMPOTENCY_CONFLICT)

            result = _replay_result(session, existing)
            session.rollback()
            return result

        except OperationalError as exc:
            session.rollback()
            if _is_retryable_serialization_error(exc):
                continue
            raise

    raise RuntimeError(
        "No se pudo resolver la propiedad de la idempotency_key tras "
        f"{_MAX_OWNERSHIP_RETRIES} intentos; posible contencion sostenida."
    )


def _replay_result(session: Session, existing: IdempotencyOperation) -> ContextAssemblyResult:
    if existing.status == "COMMITTED":
        package = session.execute(
            select(ContextPackage).where(
                ContextPackage.context_package_id == existing.context_package_id
            )
        ).scalar_one()
        return ContextAssemblyResult(
            outcome=ContextAssemblyOutcome.COMMITTED,
            context_package_id=package.context_package_id,
            context_package_hash=package.context_package_hash,
        )
    return ContextAssemblyResult(outcome=ContextAssemblyOutcome(existing.rejected_reason))


def _reject(session: Session, idempotency_key: str, reason: ContextAssemblyOutcome) -> ContextAssemblyResult:
    session.execute(
        update(IdempotencyOperation)
        .where(
            IdempotencyOperation.operation_type == _OPERATION_TYPE,
            IdempotencyOperation.idempotency_key == idempotency_key,
        )
        .values(status="REJECTED", rejected_reason=reason.value)
    )
    session.commit()
    return ContextAssemblyResult(outcome=reason)


def _resolve_object_type(session: Session, object_id: str) -> Optional[str]:
    return session.execute(
        select(InstructionalObject.object_type).where(InstructionalObject.object_id == object_id)
    ).scalar_one_or_none()


def _resolve_approved_object(session: Session, object_id: str) -> Optional[dict]:
    approved_version_id = session.execute(
        select(ApprovedStatePointer.current_approved_version_id).where(
            ApprovedStatePointer.object_id == object_id
        )
    ).scalar_one_or_none()

    if approved_version_id is None:
        return None

    version_row = session.execute(
        select(ObjectVersion.version_number, ObjectVersion.status).where(
            ObjectVersion.object_version_id == approved_version_id
        )
    ).one()

    return {
        "object_id": object_id,
        "object_type": _resolve_object_type(session, object_id),
        "version": version_row.version_number,
        "status": version_row.status,
        "data": {},
    }


def _resolve_pinned_version(
    session: Session, target_object_id: str, pinned_version_id: uuid.UUID
) -> tuple[Optional[ContextAssemblyOutcome], Optional[dict]]:
    """
    CODE-CR-40: validacion completa de pinned_version_id.

    Retorna (outcome_de_rechazo_o_None, gap_o_None):
      - (INVALID_OBJECT_VERSION, None): no existe, o pertenece a otro
        object_id -> el llamador debe rechazar de inmediato.
      - (None, gap_dict): existe y pertenece al object_id correcto,
        pero su status no es APPROVED_CONTENT -> APPROVAL_INSUFFICIENT
        como GAP, no como rechazo.
      - (None, None): pinned_version_id es valido y su status es
        APPROVED_CONTENT - el llamador procede a compararlo por
        version_number contra el puntero aprobado actual.
    """
    version_row = session.execute(
        select(ObjectVersion.object_id, ObjectVersion.status).where(
            ObjectVersion.object_version_id == pinned_version_id
        )
    ).one_or_none()

    if version_row is None or version_row.object_id != target_object_id:
        return ContextAssemblyOutcome.INVALID_OBJECT_VERSION, None

    if version_row.status != "APPROVED_CONTENT":
        return None, {
            "gap_id": f"APPROVAL_INSUFFICIENT:{pinned_version_id}",
            "missing_object_or_value": str(pinned_version_id),
            "blocking": True,
        }

    return None, None


def _proceed_as_owner(
    session: Session,
    task_type: str,
    target_object_id: str,
    pinned_version_id: Optional[uuid.UUID],
    idempotency_key: str,
    request_hash: str,
    dependency_resolver: DependencyResolver,
    evidence_retriever: EvidenceRetriever,
) -> ContextAssemblyResult:
    # Paso 2 (REQUIRED OBJECT TYPES / mapeo).
    mapping = TASK_TYPE_CONTEXT_MAPPING.get(task_type)
    if mapping is None:
        return _reject(session, idempotency_key, ContextAssemblyOutcome.MAPPING_UNDEFINED)

    # Consistencia interna del propio contrato: todo required_object_type
    # declarado en required_relations debe pertenecer al conjunto cerrado
    # required_object_types (CODE-CR-39: required_object_types deja de
    # ser dato sin uso).
    declared_types = {r["required_object_type"] for r in mapping["required_relations"]}
    if not declared_types.issubset(set(mapping["required_object_types"])):
        return _reject(session, idempotency_key, ContextAssemblyOutcome.MAPPING_UNDEFINED)

    gaps: list[dict] = []

    # Paso 4/5 (VERSION RESOLUTION / APPROVAL RESOLUTION) para el
    # objeto destino - CODE-CR-40.
    target_approved_version_id = session.execute(
        select(ApprovedStatePointer.current_approved_version_id).where(
            ApprovedStatePointer.object_id == target_object_id
        )
    ).scalar_one_or_none()

    approved_objects: list[dict] = []
    effective_version_id: Optional[uuid.UUID] = None

    if pinned_version_id is not None:
        rejection, pinned_gap = _resolve_pinned_version(session, target_object_id, pinned_version_id)
        if rejection is not None:
            return _reject(session, idempotency_key, rejection)

        if pinned_gap is not None:
            gaps.append(pinned_gap)  # APPROVAL_INSUFFICIENT - no entra a approved_objects
        else:
            # pinned_version_id es APPROVED_CONTENT valido - comparar por
            # version_number (semantica de version), NUNCA por
            # desigualdad de UUID (CODE-CR-40).
            pinned_version_number = session.execute(
                select(ObjectVersion.version_number).where(
                    ObjectVersion.object_version_id == pinned_version_id
                )
            ).scalar_one()

            if target_approved_version_id is not None:
                current_version_number = session.execute(
                    select(ObjectVersion.version_number).where(
                        ObjectVersion.object_version_id == target_approved_version_id
                    )
                ).scalar_one()
                if current_version_number > pinned_version_number:
                    return _reject(
                        session, idempotency_key, ContextAssemblyOutcome.NEWER_VERSION_AVAILABLE
                    )

            effective_version_id = pinned_version_id
    else:
        effective_version_id = target_approved_version_id

    if effective_version_id is not None:
        version_row = session.execute(
            select(ObjectVersion.version_number, ObjectVersion.status).where(
                ObjectVersion.object_version_id == effective_version_id
            )
        ).one()
        approved_objects.append(
            {
                "object_id": target_object_id,
                "object_type": _resolve_object_type(session, target_object_id),
                "version": version_row.version_number,
                "status": version_row.status,
                "data": {},
            }
        )
    elif not gaps:  # no duplicar gap si ya se anoto APPROVAL_INSUFFICIENT
        gaps.append(
            {
                "gap_id": f"NO_APPROVED_VERSION:{target_object_id}",
                "missing_object_or_value": target_object_id,
                "blocking": True,
            }
        )

    # Pasos 3 + 6 (REQUIRED RELATIONS + DEPENDENCY RESOLUTION) - incluye
    # validacion de object_type (CODE-CR-39).
    relations: list[dict] = []
    dependency_status: list[dict] = []
    dependency_resolutions = dependency_resolver.resolve(
        session, target_object_id, mapping["required_relations"]
    )
    for dr in dependency_resolutions:
        relations.append(
            {
                "from_id": target_object_id,
                "to_id": dr.to_object_id,
                "relation_type": dr.dependency_id,
                "status": "RESOLVED" if dr.status == "SATISFIED" else "UNRESOLVED",
            }
        )
        dependency_status.append({"dependency_id": dr.dependency_id, "status": dr.status})

        if dr.status == "BLOCKED_BY_DEPENDENCY":
            gaps.append(
                {
                    "gap_id": f"BLOCKED_BY_DEPENDENCY:{dr.dependency_id}",
                    "missing_object_or_value": dr.dependency_id,
                    "blocking": True,
                }
            )
        else:
            resolved = _resolve_approved_object(session, dr.to_object_id)
            if resolved is not None:
                approved_objects.append(resolved)

    # Pasos 7 + 8 (EVIDENCE RETRIEVAL + EVIDENCE SUFFICIENCY, §6.11) -
    # preserva la tupla completa (CODE-CR-41).
    evidence: list[dict] = []
    for req in mapping["required_evidence"]:
        resolution = evidence_retriever.retrieve(
            session,
            target_object_id,
            req["evidence_class"],
            req["requirement_level"],
            req["blocking_behavior"],
        )
        evidence.append(
            {
                "source_id": resolution.source_id,
                "evidence_class": resolution.evidence_class,
                "requirement_level": resolution.requirement_level,
                "blocking_behavior": resolution.blocking_behavior,
                "resolution_status": resolution.resolution_status,
            }
        )
        if resolution.resolution_status == "MISSING":
            gaps.append(
                {
                    "gap_id": f"EVIDENCE_MISSING:{req['evidence_class']}",
                    "missing_object_or_value": req["evidence_class"],
                    "blocking": resolution.blocking_behavior == "BLOCKING",
                }
            )

    # Paso 9 (CONTEXT PACKAGE).
    context_package_hash = compute_context_package_hash(
        context_contract_version=CONTEXT_CONTRACT_VERSION,
        task_type=task_type,
        target_object_id=target_object_id,
        pinned_version_id=pinned_version_id,
        approved_objects=approved_objects,
        relations=relations,
        evidence=evidence,
        dependency_status=dependency_status,
        gaps=gaps,
    )

    context_package_id = uuid.uuid4()
    session.add(
        ContextPackage(
            context_package_id=context_package_id,
            context_package_hash=context_package_hash,
            context_contract_version=CONTEXT_CONTRACT_VERSION,
            task_type=task_type,
            target_object_id=target_object_id,
            pinned_version_id=pinned_version_id,
            idempotency_key=idempotency_key,
            approved_objects=approved_objects,
            relations=relations,
            evidence=evidence,
            dependency_status=dependency_status,
            gaps=gaps,
            run_metadata={"context_engine_version": CONTEXT_CONTRACT_VERSION},
        )
    )

    # CODE-CR-45: materialize the ContextPackage parent row before
    # AuditEvent / IdempotencyOperation reference it in the same transaction.
    # flush() does NOT commit; atomicity is preserved.
    session.flush()

    session.add(
        AuditEvent(
            event_type="CONTEXT_PACKAGE_ASSEMBLED",
            object_id=target_object_id,
            object_version_id=effective_version_id,
            context_package_id=context_package_id,
            payload={"context_package_hash": context_package_hash, "task_type": task_type},
        )
    )

    session.execute(
        update(IdempotencyOperation)
        .where(
            IdempotencyOperation.operation_type == _OPERATION_TYPE,
            IdempotencyOperation.idempotency_key == idempotency_key,
        )
        .values(status="COMMITTED", context_package_id=context_package_id)
    )

    session.commit()

    return ContextAssemblyResult(
        outcome=ContextAssemblyOutcome.COMMITTED,
        context_package_id=context_package_id,
        context_package_hash=context_package_hash,
    )
