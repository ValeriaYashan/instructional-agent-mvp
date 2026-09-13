"""
Orchestrator - Iteracion 4, Proposal v0.5 S10. Secuencia exacta de
Fase A (reserva + Version Creation serializada + AgentRun, transaccion
corta), Fase B (ejecucion, sin transaccion abierta durante la llamada
externa; adquisicion/same-owner-retry/reclamacion) y Fase C (una
transaccion PostgreSQL atomica de finalizacion, con fencing con
vigencia temporal - CR-59).

No reimplementa ni duplica logica de Workflow Transition Manager
(ONLY_WORKFLOW_MANAGER_OWNS_OBJECT_LIFECYCLE_TRANSITIONS) - reutiliza
apply_transition_in_transaction directamente.

CODE-CR-60 (Human Code Review de Build v1): el Producer debe recibir el
ContextPackage INMUTABLE real correspondiente a agent_run.
context_package_id - nunca un dict vacio. Se materializa UNA sola vez
por ejecucion de Fase B (_fetch_context_package_payload), antes del
bucle de intentos, y se reutiliza identico en cada llamada al proveedor
- nunca se vuelve a consultar estado mutable para reemplazarlo.
execution_contract.context_package_hash se puebla con el hash
realmente persistido en la fila, no con un valor vacio.

CODE-CR-61 (Human Code Review de Build v1): ninguna llamada a
ProducerExecutor.execute() puede ocurrir con una transaccion
PostgreSQL abierta (autobegin de SQLAlchemy 2.x). Toda lectura previa
necesaria para construir el execution_contract y el context_package
termina explicitamente en session.rollback()/commit() ANTES del bucle
de intentos; _ensure_no_open_transaction() es una guarda defensiva
adicional, verificada en runtime, inmediatamente antes de cada llamada
al proveedor - no basta con comentarios.

CODE-CR-62: cada AgentRunAttempt persiste provider/model_identifier/
instructions_version/output_schema_version en el momento de su
creacion (create_attempt), no como actualizacion posterior.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.domain.agent_run_result import AgentRunOutcome, AgentRunResult
from src.persistence.models import (
    AgentRun,
    AgentRunAttempt,
    AuditEvent,
    ContextPackage,
    IdempotencyOperation,
    InstructionalObject,
    ObjectVersionContent,
)
from src.persistence.repositories import agent_run_manager as arm
from src.domain.context_result import ContextAssemblyOutcome
from src.persistence.repositories.context_engine import assemble_context
from src.persistence.repositories.producer_executor import (
    ExecutionContract,
    ProducerExecutor,
    compute_allowed_learning_outcome_ids,
)
from src.persistence.repositories.producer_output_schema import (
    PRODUCER_OUTPUT_SCHEMA_VERSION,
    validate_structural,
)
from src.persistence.repositories.task_intake import is_known_task_type
from src.persistence.repositories.version_creation import (
    lock_instructional_object,
    resolve_or_create_object_version,
)
from src.persistence.repositories.workflow_transition import (
    apply_transition_in_transaction,
    compute_transition_payload_hash,
)

_OPERATION_TYPE = "PRODUCER_EXECUTION"
_MAX_OWNERSHIP_RETRIES = 5
_POLL_INTERVAL_SECONDS = 0.05
_POLL_MAX_WAIT_SECONDS = 5.0

_TASK_TYPE = "PRODUCE_LEARNING_OBJECT"

EXECUTION_CONTRACT_VERSION = "execution-contract-v1"


def compute_producer_request_hash(
    task_type: str,
    target_object_id: str,
    context_package_id: uuid.UUID,
    provider: str,
    model_identifier: str,
    instructions_version: str,
    output_schema_version: str,
) -> str:
    """
    Hash semantico COMPLETO de una ejecucion PRODUCER_EXECUTION ya
    resuelta (incluye context_package_id). Excluye correlation_id,
    timestamps, attempt_number, claimed_by, lease_generation,
    lease_expires_at, provider_timeout_seconds, identificadores de
    respuesta, tokens/costo.

    CODE-CR-71/CODE-CR-72 (Human Code Review de Build v5, BLOCKER):
    esta funcion YA NO se usa para resolver la reserva de idempotencia
    PRODUCER_EXECUTION - ver compute_producer_request_identity_hash
    para eso. Razon: context_package_id no existe todavia en el
    instante en que la reserva debe resolverse (Context Assembly ni
    siquiera se intento aun para una clave nueva), y usarlo como parte
    del hash de reserva obligaba a ensamblar el ContextPackage ANTES de
    saber si la idempotency_key era un conflicto o un replay -
    exactamente el defecto de orquestacion que CR-71 identifico. Se
    conserva sin cambios (firma, semantica, y su suite de tests
    dedicada) como el hash semantico completo de una ejecucion ya
    resuelta, disponible para uso futuro de auditoria/depuracion.
    """
    raw = "|".join(
        [
            task_type,
            target_object_id,
            str(context_package_id),
            provider,
            model_identifier,
            instructions_version,
            output_schema_version,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_producer_request_identity_hash(
    task_type: str,
    target_object_id: str,
    provider: str,
    model_identifier: str,
    instructions_version: str,
    output_schema_version: str,
) -> str:
    """
    CODE-CR-71/CODE-CR-72 (Human Code Review de Build v5, BLOCKER):
    identidad de PRE-CONTEXTO de una solicitud PRODUCER_EXECUTION -
    exactamente lo que se conoce ANTES de intentar Context Assembly.
    Deliberadamente NUNCA incluye context_package_id.

    Esta es la UNICA identidad usada para resolver la reserva de
    idempotencia PRODUCER_EXECUTION (ganarla, o comparar contra una ya
    existente) - separa la identidad de RESERVA (esta funcion) de la
    identidad de EJECUCION PERSISTIDA, que vive exclusivamente en
    AgentRun.context_package_id (autoritativa post-resolucion, CR-63) -
    nunca en un segundo hash de idempotencia derivado del contexto.

    Dos solicitudes con la misma idempotency_key pero distinto
    target_object_id (u otro campo incluido aqui) deben resolver en
    IDEMPOTENCY_CONFLICT inmediatamente, antes de que Context Assembly
    se intente siquiera una vez (CODE-CR-71).
    """
    raw = "|".join(
        [task_type, target_object_id, provider, model_identifier, instructions_version, output_schema_version]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _ensure_no_open_transaction(session: Session) -> None:
    """
    CODE-CR-61 + correccion de guarda transaccional (Human Code Review
    de Build v3): guarda FAIL-FAST verificada en runtime, no solo por
    disciplina de codigo. Si por cualquier motivo quedo una transaccion
    PostgreSQL abierta (autobegin de SQLAlchemy 2.x tras un
    SELECT/UPDATE sin commit/rollback posterior) en el instante exacto
    anterior a invocar al proveedor, esta funcion NUNCA la cierra
    implicitamente con rollback() - un rollback() automatico aqui
    podria ocultar un bug real y descartar trabajo no commiteado sin
    que nadie se entere - y NUNCA depende de assert como unica
    garantia (se desactiva por completo con `python -O`). Toda
    transaccion previa debe haber sido cerrada EXPLICITAMENTE por el
    codigo que la abrio, antes de llegar a este punto; si no lo fue,
    eso es un bug real que debe fallar de forma ruidosa e inmediata.
    """
    if session.in_transaction():
        raise RuntimeError(
            "Invariante de Proposal v0.5 violada: ProducerExecutor.execute() "
            "esta a punto de invocarse con una transaccion PostgreSQL "
            "todavia abierta. El codigo que precede a esta llamada debe "
            "cerrar su transaccion explicitamente (commit/rollback) antes "
            "de este punto - esta guarda nunca hace rollback implicito ni "
            "descarta trabajo silenciosamente."
        )


def _fetch_context_package_payload(session: Session, context_package_id: uuid.UUID) -> tuple[dict, str]:
    """
    CODE-CR-60: materializa el ContextPackage INMUTABLE real como un
    payload provider-neutral (dict), UNA sola vez por ejecucion de
    Fase B. Como context_package es inmutable por diseño (Iteracion 3 -
    nunca se actualiza tras su insercion), este payload es el mismo
    snapshot autoritativo en cada intento de la misma ejecucion, sin
    importar que el estado aprobado subyacente cambie despues (CR-35).
    """
    row = session.execute(
        select(ContextPackage).where(ContextPackage.context_package_id == context_package_id)
    ).scalar_one()
    payload = {
        "context_package_id": str(row.context_package_id),
        "context_package_hash": row.context_package_hash,
        "context_contract_version": row.context_contract_version,
        "task_type": row.task_type,
        "target_object_id": row.target_object_id,
        "pinned_version_id": str(row.pinned_version_id) if row.pinned_version_id else None,
        "approved_objects": row.approved_objects,
        "relations": row.relations,
        "evidence": row.evidence,
        "dependency_status": row.dependency_status,
        "gaps": row.gaps,
    }
    return payload, row.context_package_hash


def request_producer_execution(
    session: Session,
    target_object_id: str,
    requested_by: str,
    idempotency_key: str,
    provider: str,
    model_identifier: str,
    instructions_version: str,
    worker_id: str,
    producer_executor: ProducerExecutor,
    output_schema_version: str = PRODUCER_OUTPUT_SCHEMA_VERSION,
    max_attempts: int = 3,
    correlation_id: Optional[str] = None,
) -> AgentRunResult:
    """
    Punto de entrada unico del Orchestrator para PRODUCE_LEARNING_OBJECT.

    CODE-CR-74 (Human Code Review de Build v6, BLOCKER): Build v6
    ganaba la reserva PRODUCER_EXECUTION (INSERT ... CLAIMED) y llamaba
    a assemble_context() SIN cerrar esa transaccion - imposible por
    construccion, dado que Context Engine (CR-37) exige
    session.in_transaction() == False como precondicion dura. La
    secuencia correcta, verificada contra las garantias exigidas en la
    revision (CR-37, CR-71, CR-72, CR-51,
    AT_MOST_ONE_LOGICAL_AGENT_RUN_PER_IDEMPOTENCY_KEY, deteccion
    deterministica de IDEMPOTENCY_CONFLICT, no debilitar la guarda de
    Context Engine, no mantener una transaccion abierta solo para
    sostener un lock de reserva) es:

      1. PREFLIGHT de solo lectura sobre la fila PRODUCER_EXECUTION ya
         existente (si la hay), usando EXCLUSIVAMENTE la identidad de
         PRE-CONTEXTO (compute_producer_request_identity_hash, nunca
         incluye context_package_id - CR-71/CR-72 sin cambios). Esa
         lectura SIEMPRE termina en session.rollback() antes de
         continuar - nunca deja una transaccion abierta.
      2. Si la clave es NUEVA (no existe fila): Context Assembly se
         intenta directamente, SIN ninguna reserva previa - no se
         commitea ningun CLAIMED antes (CODE-CR-74, punto 8 de la
         revision: un CLAIMED durable ahi exigiria recuperacion de
         crash no diseñada). assemble_context() se invoca, por tanto,
         SIEMPRE con session.in_transaction() == False (CR-37 se
         cumple por construccion, no por disciplina de codigo).
      3. Si Context Engine devuelve su PROPIO
         ContextAssemblyOutcome.IDEMPOTENCY_CONFLICT (la
         context_idempotency_key derivada,
         f"context-for:{idempotency_key}", ya esta tomada por una
         solicitud PRODUCER_EXECUTION distinta que comparte la misma
         idempotency_key de nivel superior pero difiere en
         target_object_id u otro campo de identidad de pre-contexto):
         esto se traduce DIRECTAMENTE a AgentRunOutcome.IDEMPOTENCY_CONFLICT
         - NUNCA a INVALID_CONTEXT_PACKAGE (bug de clasificacion
         identificado en la misma revision: antes de esta correccion,
         cualquier outcome de Context Engine distinto de COMMITTED se
         colapsaba a INVALID_CONTEXT_PACKAGE sin distinguir esta
         rama). No se escribe ninguna fila PRODUCER_EXECUTION para
         esta idempotency_key - la solicitud ganadora, si la hay, la
         crea ella misma.
      4. Si Context Engine rechaza por un fallo real del contrato de 10
         pasos (MAPPING_UNDEFINED / NEWER_VERSION_AVAILABLE /
         INVALID_OBJECT_VERSION): se persiste REJECTED bajo NUESTRA
         propia idempotency_key (CR-27, replay identico futuro sin
         volver a tocar Context Engine) - sin cambio de fondo respecto
         a Build v6, solo reordenado.
      5. Si Context Assembly tuvo exito: la reserva PRODUCER_EXECUTION
         + el AgentRun se crean y se compite por ellos ATOMICAMENTE, EN
         UNA SOLA transaccion corta, DESPUES de que Context Assembly ya
         cerro la suya (CODE-CR-74: la reserva ocurre DESPUES de
         Context Assembly, nunca antes - nunca se mantiene una
         transaccion abierta solo para proteger un lock de reserva,
         punto 9 de la revision). Si se pierde esa carrera (otra
         solicitud resolvio la misma idempotency_key entre nuestro
         preflight y este punto), se hace ROLLBACK completo de nuestro
         AgentRun/ObjectVersion recien creados - nunca se commitea un
         AgentRun duplicado
         (AT_MOST_ONE_LOGICAL_AGENT_RUN_PER_IDEMPOTENCY_KEY) - y se
         reintenta el preflight, que ahora encontrara la fila ganadora
         y resolvera replay/conflicto de forma deterministica.

    CODE-CR-71/CODE-CR-72 (Human Code Review de Build v5, BLOCKER, sin
    cambios de fondo en esta correccion): la reserva de idempotencia
    PRODUCER_EXECUTION se resuelve usando exclusivamente la identidad
    de PRE-CONTEXTO. Una vez que logical_run_id esta resuelto (recien
    creado O replay de una reserva ya COMMITTED), AgentRun es la UNICA
    autoridad para ese logical_run - nunca se reutilizan
    context_package_id/max_attempts de esta invocacion particular de
    request_producer_execution (CR-63).
    """
    if not is_known_task_type(_TASK_TYPE):
        return AgentRunResult(outcome=AgentRunOutcome.TASK_TYPE_UNRESOLVED)

    correlation_id = correlation_id or str(uuid.uuid4())
    request_identity_hash = compute_producer_request_identity_hash(
        _TASK_TYPE, target_object_id, provider, model_identifier, instructions_version, output_schema_version,
    )

    logical_run_id: Optional[uuid.UUID] = None

    for _ in range(_MAX_OWNERSHIP_RETRIES):
        # --- PREFLIGHT: lectura corta y SIEMPRE cerrada. Nunca abre una
        # reserva - solo mira si esta idempotency_key ya fue resuelta
        # por alguien mas (CODE-CR-74). ---
        existing = session.execute(
            select(IdempotencyOperation).where(
                IdempotencyOperation.operation_type == _OPERATION_TYPE,
                IdempotencyOperation.idempotency_key == idempotency_key,
            )
        ).scalar_one_or_none()
        session.rollback()  # CODE-CR-74: cierra SIEMPRE antes de decidir el siguiente paso

        if existing is not None:
            if existing.idempotency_payload_hash != request_identity_hash:
                return AgentRunResult(outcome=AgentRunOutcome.IDEMPOTENCY_CONFLICT)
            if existing.status == "REJECTED":
                # Replay identico del mismo rechazo (CR-27) - Context
                # Engine NUNCA se vuelve a tocar para esta idempotency_key.
                return AgentRunResult(outcome=AgentRunOutcome.INVALID_CONTEXT_PACKAGE)
            # COMMITTED (unico otro valor final posible - CLAIMED nunca
            # sobrevive de forma durable, CODE-CR-74/CR-51): replay
            # puro.
            logical_run_id = existing.agent_run_id
            break

        # --- Clave nunca vista: Context Assembly PRIMERO, SIN ninguna
        # reserva previa y SIN transaccion abierta (CODE-CR-74 / CR-37). ---
        context_idempotency_key = f"context-for:{idempotency_key}"
        context_result = assemble_context(
            session,
            task_type=_TASK_TYPE,
            target_object_id=target_object_id,
            idempotency_key=context_idempotency_key,
        )

        if context_result.outcome == ContextAssemblyOutcome.IDEMPOTENCY_CONFLICT:
            # CODE-CR-74: conflicto de idempotencia del PROPIO Context
            # Engine sobre context-for:{idempotency_key} - NUNCA se
            # reclasifica como INVALID_CONTEXT_PACKAGE. No se escribe
            # ninguna fila PRODUCER_EXECUTION para esta clave.
            return AgentRunResult(outcome=AgentRunOutcome.IDEMPOTENCY_CONFLICT)

        if context_result.outcome != ContextAssemblyOutcome.COMMITTED:
            # MAPPING_UNDEFINED / NEWER_VERSION_AVAILABLE /
            # INVALID_OBJECT_VERSION: fallo real del contrato de 10
            # pasos - se persiste REJECTED bajo nuestra propia
            # idempotency_key (CR-27).
            claimed_rejected = session.execute(
                pg_insert(IdempotencyOperation)
                .values(
                    operation_type=_OPERATION_TYPE,
                    idempotency_key=idempotency_key,
                    idempotency_payload_hash=request_identity_hash,
                    status="REJECTED",
                    commit_id=None,
                    transition_id=None,
                    context_package_id=None,
                    agent_run_id=None,
                    rejected_reason="INVALID_CONTEXT_PACKAGE",
                )
                .on_conflict_do_nothing(index_elements=["operation_type", "idempotency_key"])
                .returning(IdempotencyOperation.idempotency_key)
            ).scalar_one_or_none()
            session.commit()
            if claimed_rejected is not None:
                return AgentRunResult(outcome=AgentRunOutcome.INVALID_CONTEXT_PACKAGE)
            # Perdimos la carrera de escritura del REJECTED contra otra
            # solicitud concurrente para la MISMA clave - el preflight
            # del proximo intento del bucle resuelve replay/conflicto
            # sin volver a tocar Context Engine.
            continue

        # === Context Assembly exitoso: se compite ATOMICAMENTE por la
        # reserva PRODUCER_EXECUTION + el AgentRun, EN UNA transaccion
        # corta, DESPUES de que Context Assembly ya cerro la suya
        # (CODE-CR-74). ===
        logical_run_id, _target_object_version_id = _phase_a_create_run(
            session=session,
            target_object_id=target_object_id,
            context_package_id=context_result.context_package_id,
            requested_by=requested_by,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
            max_attempts=max_attempts,
        )
        reservation_won = session.execute(
            pg_insert(IdempotencyOperation)
            .values(
                operation_type=_OPERATION_TYPE,
                idempotency_key=idempotency_key,
                idempotency_payload_hash=request_identity_hash,
                status="COMMITTED",
                commit_id=None,
                transition_id=None,
                context_package_id=None,  # CR-63: NUNCA aqui - solo en AgentRun
                agent_run_id=logical_run_id,
                rejected_reason=None,
            )
            .on_conflict_do_nothing(index_elements=["operation_type", "idempotency_key"])
            .returning(IdempotencyOperation.idempotency_key)
        ).scalar_one_or_none()

        if reservation_won is not None:
            session.commit()
            break

        # Perdimos la carrera POST-contexto: otra solicitud ya resolvio
        # esta idempotency_key entre nuestro preflight y este punto.
        # ROLLBACK completo - nunca se commitea un AgentRun duplicado
        # (AT_MOST_ONE_LOGICAL_AGENT_RUN_PER_IDEMPOTENCY_KEY).
        session.rollback()
        logical_run_id = None
        continue
    else:
        raise RuntimeError(
            "No se pudo resolver la propiedad de la idempotency_key tras "
            f"{_MAX_OWNERSHIP_RETRIES} intentos; posible contencion sostenida."
        )

    # CODE-CR-63: AgentRun es la UNICA autoridad para context_package_id/
    # max_attempts de este logical_run - lectura corta y CERRADA.
    run_snapshot = session.execute(
        select(
            AgentRun.target_object_version_id,
            AgentRun.context_package_id,
            AgentRun.max_attempts,
        ).where(AgentRun.logical_run_id == logical_run_id)
    ).one()
    session.rollback()

    # === FASE B ===
    _phase_b_execute(
        session=session,
        logical_run_id=logical_run_id,
        target_object_id=target_object_id,
        target_object_version_id=run_snapshot.target_object_version_id,
        worker_id=worker_id,
        producer_executor=producer_executor,
        provider=provider,
        model_identifier=model_identifier,
        instructions_version=instructions_version,
        output_schema_version=output_schema_version,
        context_package_id=run_snapshot.context_package_id,  # AUTORITATIVO
        max_attempts=run_snapshot.max_attempts,  # AUTORITATIVO
    )

    return _poll_terminal_result(session, logical_run_id)


def _phase_a_create_run(
    session: Session,
    target_object_id: str,
    context_package_id: uuid.UUID,
    requested_by: str,
    correlation_id: str,
    idempotency_key: str,
    max_attempts: int,
) -> tuple[uuid.UUID, uuid.UUID]:
    lock_instructional_object(session, target_object_id)
    target_object_version_id = resolve_or_create_object_version(session, target_object_id)
    logical_run_id = arm.create_agent_run(
        session,
        role="PRODUCER",
        task_type=_TASK_TYPE,
        target_object_version_id=target_object_version_id,
        context_package_id=context_package_id,
        requested_by=requested_by,
        correlation_id=correlation_id,
        idempotency_key=idempotency_key,
        max_attempts=max_attempts,
    )
    return logical_run_id, target_object_version_id


def _phase_b_execute(
    session: Session,
    logical_run_id: uuid.UUID,
    target_object_id: str,
    target_object_version_id: uuid.UUID,
    worker_id: str,
    producer_executor: ProducerExecutor,
    provider: str,
    model_identifier: str,
    instructions_version: str,
    output_schema_version: str,
    context_package_id: uuid.UUID,
    max_attempts: int,
) -> None:
    """
    CODE-CR-66 (Human Code Review de Build v3, BLOCKER): la autorizacion
    de ownership (adquisicion inicial / reclamacion / same-owner retry)
    y la creacion del siguiente AgentRunAttempt ya NO estan separadas
    por COMMITs independientes - cada paso usa una de las tres
    operaciones fusionadas de agent_run_manager
    (acquire_and_create_first_attempt / reclaim_and_create_attempt /
    refresh_and_create_retry_attempt), cada una resuelta en UNA sola
    transaccion corta, con UN UNICO session.commit() inmediatamente
    despues. Ningun worker puede insertar un AgentRunAttempt despues de
    haber perdido ownership - la autoridad y el Attempt se otorgan/crean
    atomicamente o ninguno de los dos ocurre.
    """
    _attempt_kwargs = dict(
        provider=provider,
        model_identifier=model_identifier,
        instructions_version=instructions_version,
        output_schema_version=output_schema_version,
    )

    # --- Adquisicion inicial (fusionada con Attempt 1) o reclamacion
    # (fusionada con el siguiente Attempt) - CODE-CR-66 ---
    auth = arm.acquire_and_create_first_attempt(session, logical_run_id, worker_id, **_attempt_kwargs)
    if auth.generation is None:
        session.rollback()
        auth = arm.reclaim_and_create_attempt(
            session, logical_run_id, worker_id, max_attempts, **_attempt_kwargs
        )
        session.commit()
        if auth.generation is None:
            return  # otro worker ya es dueno (o ya termino) - resultado via poll
        if auth.failed_max_attempts:
            return  # FAILED ya marcado atomicamente; ningun Attempt nuevo
    else:
        session.commit()

    generation = auth.generation
    attempt_id = auth.attempt_id

    # CODE-CR-60: ContextPackage REAL, materializado UNA vez, reutilizado
    # identico en cada intento de esta ejecucion.
    context_payload, context_package_hash = _fetch_context_package_payload(session, context_package_id)
    session.rollback()  # CODE-CR-61: cierra la transaccion de lectura

    # object_type del target para Structural Validation - lectura corta
    # y CERRADA, independiente de la llamada al proveedor.
    target_object_type = session.execute(
        select(InstructionalObject.object_type).where(InstructionalObject.object_id == target_object_id)
    ).scalar_one_or_none()
    session.rollback()

    # CODE-CR-67: whitelist AUTORITATIVA UNICA - identica a la que usa
    # el prompt del proveedor (producer_executor._build_prompt), ambas
    # via compute_allowed_learning_outcome_ids. Nunca dos calculos
    # independientes que puedan divergir.
    allowed_learning_outcome_ids = compute_allowed_learning_outcome_ids(context_payload["approved_objects"])

    execution_contract = ExecutionContract(
        execution_contract_version=EXECUTION_CONTRACT_VERSION,
        role="PRODUCER",
        task_type=_TASK_TYPE,
        context_package_id=context_package_id,
        context_package_hash=context_package_hash,  # CODE-CR-60: hash real
        provider=provider,
        model_identifier=model_identifier,
        instructions_version=instructions_version,
        output_schema_version=output_schema_version,
        provider_timeout_seconds=arm.PROVIDER_TIMEOUT_SECONDS,
        expected_object_type=target_object_type,  # CODE-CR-65
    )

    while True:
        # === FASE B: llamada externa, SIN transaccion PostgreSQL abierta ===
        _ensure_no_open_transaction(session)  # CODE-CR-61: verificado en runtime, fail-fast
        result = producer_executor.execute(execution_contract, context_payload)

        if result.technical_error is not None:
            arm.close_attempt(session, attempt_id, outcome="TECHNICAL_FAILURE", failure_detail=result.technical_error)
            session.commit()
        else:
            validation = validate_structural(result.parsed_output, target_object_type, allowed_learning_outcome_ids)
            if not validation.is_valid:
                arm.close_attempt(
                    session, attempt_id, outcome="INVALID_OUTPUT", failure_detail=validation.reason,
                    output_payload=result.parsed_output,
                )
                session.commit()
            else:
                _phase_c_finalize(
                    session, logical_run_id, target_object_id, target_object_version_id,
                    worker_id, generation, attempt_id, result.parsed_output,
                )
                return  # terminal, exitoso o CLAIM_LOST

        # --- SAME-OWNER RETRY (fusionado con el siguiente Attempt) o
        # RECLAIM (fusionado con el siguiente Attempt) - CODE-CR-66 ---
        auth = arm.refresh_and_create_retry_attempt(
            session, logical_run_id, worker_id, generation, max_attempts, **_attempt_kwargs
        )
        if auth.generation is not None:
            session.commit()  # SIN CAMBIOS de generation (CR-58)
        else:
            session.rollback()
            auth = arm.reclaim_and_create_attempt(
                session, logical_run_id, worker_id, max_attempts, **_attempt_kwargs
            )
            session.commit()
            if auth.generation is None:
                return  # perdimos ownership definitivamente para este ciclo

        if auth.failed_max_attempts:
            return  # FAILED ya marcado atomicamente; ningun Attempt nuevo

        generation = auth.generation
        attempt_id = auth.attempt_id


def _phase_c_finalize(
    session: Session,
    logical_run_id: uuid.UUID,
    target_object_id: str,
    target_object_version_id: uuid.UUID,
    worker_id: str,
    generation: int,
    attempt_id: uuid.UUID,
    parsed_output: dict,
) -> bool:
    """
    Proposal v0.5 S10, pasos 20-25. Devuelve True si finalizo con
    exito; False si perdio el fencing (CLAIM_LOST, S8.7 - CR-59).
    """
    authorized = arm.authorize_terminal_success(session, logical_run_id, worker_id, generation)
    if not authorized:
        session.rollback()
        arm.close_attempt(session, attempt_id, outcome="CLAIM_LOST")
        session.commit()
        return False

    run = session.execute(
        select(AgentRun.requested_by, AgentRun.correlation_id).where(
            AgentRun.logical_run_id == logical_run_id
        )
    ).one()

    session.add(
        ObjectVersionContent(
            object_version_id=target_object_version_id,
            schema_version=parsed_output["schema_version"],
            title=parsed_output["title"],
            body=parsed_output["body"],
            learning_outcome_refs=parsed_output["learning_outcome_refs"],
            metadata_=parsed_output["metadata"],
            produced_by_attempt_id=attempt_id,
        )
    )

    transition_requested_by = f"agent_run:{logical_run_id}"
    transition_payload_hash = compute_transition_payload_hash(
        target_object_id, target_object_version_id, "ELIGIBLE_PENDING", "DRAFT", transition_requested_by
    )
    transition_result = apply_transition_in_transaction(
        session=session,
        target_object_id=target_object_id,
        target_object_version_id=target_object_version_id,
        from_state="ELIGIBLE_PENDING",
        to_state="DRAFT",
        requested_by=transition_requested_by,
        correlation_id=run.correlation_id,
        idempotency_key=f"producer-transition:{logical_run_id}",
        payload_hash=transition_payload_hash,
    )
    if transition_result.outcome.value != "COMMITTED":
        session.rollback()
        arm.close_attempt(session, attempt_id, outcome="TECHNICAL_FAILURE", failure_detail="transition_rejected")
        session.commit()
        return False

    arm.close_attempt(session, attempt_id, outcome="SUCCESS", output_payload=parsed_output)

    session.add(
        AuditEvent(
            event_type="PRODUCER_CONTENT_PERSISTED",
            object_id=target_object_id,
            object_version_id=target_object_version_id,
            agent_run_id=logical_run_id,
            payload={"attempt_id": str(attempt_id)},
        )
    )

    session.commit()
    return True


def _poll_terminal_result(session: Session, logical_run_id: uuid.UUID) -> AgentRunResult:
    deadline = time.monotonic() + _POLL_MAX_WAIT_SECONDS
    while time.monotonic() < deadline:
        session.rollback()  # asegura una lectura fresca, no una transaccion vieja
        status, target_version_id = session.execute(
            select(AgentRun.status, AgentRun.target_object_version_id).where(
                AgentRun.logical_run_id == logical_run_id
            )
        ).one()
        session.rollback()
        if status == "SUCCEEDED":
            return AgentRunResult(
                outcome=AgentRunOutcome.SUCCEEDED,
                logical_run_id=logical_run_id,
                object_version_id=target_version_id,
                object_version_content_id=target_version_id,
            )
        if status == "FAILED":
            return AgentRunResult(outcome=AgentRunOutcome.FAILED, logical_run_id=logical_run_id)
        time.sleep(_POLL_INTERVAL_SECONDS)
    return AgentRunResult(outcome=AgentRunOutcome.PENDING_POLL_TIMEOUT, logical_run_id=logical_run_id)
