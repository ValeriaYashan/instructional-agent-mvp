"""
Agent Run Manager - Iteracion 4, Proposal v0.5 S8 (CR-56, CR-58, CR-59).

Dueno exclusivo de agent_run / agent_run_attempt: adquisicion,
reclamacion, reintento bajo ownership vigente (same-owner retry), y
autorizacion de finalizacion (exitosa o por max_attempts). Toda
autoridad se resuelve en un UNICO UPDATE condicional de PostgreSQL -
nunca mediante un SELECT previo desacoplado de la escritura
(check-then-act evitado por construccion en cada funcion de este
modulo).

Convencion temporal canonica UNICA (CR-59 S8.1, corregida por CODE-CR-69
- Human Code Review de Build v4), usada identicamente por
adquisicion/reclamacion, same-owner retry y autorizacion de
finalizacion - nunca se mezclan las dos direcciones sin la garantia de
que son exactamente complementarias:

    LEASE_IS_VALID    ==  lease_expires_at > clock_timestamp()
    LEASE_IS_EXPIRED  ==  lease_expires_at <= clock_timestamp()

CODE-CR-69: la autoridad temporal es EXCLUSIVAMENTE clock_timestamp()
de PostgreSQL - NUNCA now()/CURRENT_TIMESTAMP (estable durante toda la
transaccion, no avanza mientras un UPDATE espera un row lock, lo que
permitiria que una transaccion bloqueada en un lock siguiera viendo un
lease como vigente despues de haber expirado en tiempo real) y NUNCA
datetime.now() del proceso Python (reloj de aplicacion, potencialmente
desincronizado del reloj de la base). Tanto la EVALUACION de vigencia
(LEASE_IS_VALID/LEASE_IS_EXPIRED) como el CALCULO del proximo
lease_expires_at en adquisicion/reclamacion/refresco usan
exclusivamente clock_timestamp() - nunca se mezclan ambos relojes.

LEASE_EXPIRED NUNCA se almacena como estado - es un predicado derivado,
evaluado siempre dentro del mismo UPDATE que intenta actuar bajo esa
condicion. No se introduce ningun estado nuevo del Instructional Object
Lifecycle ni de AgentRun.status para representarlo.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import func, select, text, update
from sqlalchemy.orm import Session

from src.persistence.models import AgentRun, AgentRunAttempt

# Proposal v0.5 S8.10: LEASE_DURATION > PROVIDER_TIMEOUT + SAFETY_MARGIN.
# Valores concretos - TECHNICAL_DESIGN_DECISION, no arquitectonica.
PROVIDER_TIMEOUT_SECONDS = 30
SAFETY_MARGIN_SECONDS = 15
LEASE_DURATION_SECONDS = 60

assert LEASE_DURATION_SECONDS > PROVIDER_TIMEOUT_SECONDS + SAFETY_MARGIN_SECONDS, (
    "Violacion de la invariante de Proposal v0.5 S8.10: "
    "LEASE_DURATION_SECONDS debe ser estrictamente mayor que "
    "PROVIDER_TIMEOUT_SECONDS + SAFETY_MARGIN_SECONDS."
)


def _new_lease_expiry_expr():
    """
    CODE-CR-69 (Human Code Review de Build v4, BLOCKER): expresion SQL
    - NUNCA un valor datetime calculado en Python - que computa el
    proximo vencimiento del lease usando EXCLUSIVAMENTE el reloj de
    PostgreSQL evaluado en el instante de ejecucion del UPDATE
    (clock_timestamp()), nunca now()/CURRENT_TIMESTAMP (estable durante
    toda la transaccion) ni datetime.now() de la aplicacion. Se usa
    identica en acquire()/reclaim()/refresh_lease_for_same_owner_retry()
    - un unico punto de calculo, igual que allowed_learning_outcome_ids
    (CR-67) es un unico punto de calculo para la whitelist.

    LEASE_DURATION_SECONDS es una constante de modulo (no input del
    usuario) - interpolarla en el texto del intervalo es seguro.
    """
    return func.clock_timestamp() + text(f"interval '{LEASE_DURATION_SECONDS} seconds'")


def _lock_agent_run_row_for_update(session: Session, logical_run_id: uuid.UUID) -> bool:
    """
    CODE-CR-69 REABIERTO (Human Code Review de Build v5, BLOCKER):
    fuerza a que la ESPERA de un row lock ocurra AQUI, en un SELECT
    ... FOR UPDATE explicito y separado, SIN ningun predicado
    dependiente del tiempo (solo logical_run_id) - antes de evaluar
    LEASE_IS_VALID en cualquier UPDATE posterior.

    POR QUE clock_timestamp() SOLO EN EL WHERE DE UN UPDATE PLANO NO
    ALCANZA (root cause verificado contra PostgreSQL 16 real,
    test_lock_wait_crossing_expiry_uses_wall_clock_not_transaction_start):
    para un UPDATE plano, PostgreSQL evalua el calificador WHERE
    (incluido cualquier clock_timestamp() en el) ANTES de intentar
    tomar el row lock sobre la tupla candidata - el bloqueo ocurre
    DENTRO de heap_update, DESPUES de que el calificador ya fue
    evaluado. Si la transaccion que retiene el lock NUNCA comitea un
    cambio real sobre esa fila (por ejemplo, solo la bloquea con
    SELECT...FOR UPDATE y luego hace ROLLBACK - exactamente el
    escenario de carrera real que este mismo test reproduce), no
    existe una version nueva de la tupla para que EvalPlanQual dispare
    una re-evaluacion tras el desbloqueo: PostgreSQL simplemente
    reanuda el UPDATE usando el resultado de clock_timestamp() ya
    calculado ANTES de la espera - que puede corresponder a un
    instante anterior al vencimiento real del lease. Reemplazar
    now() por clock_timestamp() en el WHERE, por si solo, NO cierra
    esta carrera - de ahi que Build v5 siguiera fallando ese test pese
    a usar clock_timestamp() en todas partes.

    Este SELECT...FOR UPDATE separado resuelve la espera del lock COMO
    SU PROPIO PASO, sin ningun predicado de tiempo en su propio WHERE -
    por lo tanto no hay ningun resultado de clock_timestamp() "viejo"
    que reutilizar cuando se desbloquea. Una vez que esta funcion
    retorna, ESTA transaccion ya posee el row lock; el UPDATE que sigue
    a continuacion, en la MISMA transaccion, sobre la MISMA fila, jamas
    vuelve a esperar ese lock (una transaccion nunca se bloquea sobre
    un lock que ella misma ya sostiene) - por lo tanto su propia
    evaluacion de clock_timestamp() en el WHERE ocurre sin demora
    adicional, reflejando el tiempo real en el instante exacto en que
    la autoridad efectivamente se ejerce.

    Devuelve False (sin lanzar) si la fila no existe - el llamador debe
    tratarlo identico a un rows_affected=0 de la logica previa, sin
    intentar el UPDATE subsiguiente.
    """
    row_id = session.execute(
        select(AgentRun.logical_run_id).where(AgentRun.logical_run_id == logical_run_id).with_for_update()
    ).scalar_one_or_none()
    return row_id is not None


@dataclass(frozen=True)
class AttemptAuthorization:
    """
    CODE-CR-66 (Human Code Review de Build v3, BLOCKER): resultado de
    una operacion fusionada de autorizacion de ownership + creacion del
    siguiente AgentRunAttempt, ejecutada dentro de UNA UNICA transaccion
    corta (ningun COMMIT intermedio entre el UPDATE que otorga/renueva
    ownership y el INSERT del Attempt). El llamador hace session.commit()
    UNA sola vez, inmediatamente despues de recibir este resultado -
    nunca entre los pasos internos de las funciones que lo producen.

    generation is None
        -> la autorizacion de ownership fallo (lease vigente en manos de
           otro worker, o AgentRun ya en estado terminal). attempt_id es
           siempre None en este caso. NINGUN Attempt fue creado.

    failed_max_attempts=True
        -> la autorizacion de ownership tuvo exito, pero max_attempts ya
           esta agotado bajo esa autoridad; AgentRun.status paso a
           'FAILED' atomicamente en la MISMA transaccion. attempt_id es
           siempre None - NINGUN Attempt nuevo fue creado.

    generation is not None and failed_max_attempts=False
        -> exito pleno: nueva/renovada autoridad Y el siguiente
           AgentRunAttempt fueron creados coherentemente, bajo la MISMA
           generacion devuelta. attempt_id identifica ese Attempt.
    """

    generation: Optional[int]
    attempt_id: Optional[uuid.UUID]
    failed_max_attempts: bool = False


def create_agent_run(
    session: Session,
    role: str,
    task_type: str,
    target_object_version_id: uuid.UUID,
    context_package_id: uuid.UUID,
    requested_by: str,
    correlation_id: str,
    idempotency_key: str,
    max_attempts: int,
) -> uuid.UUID:
    """Fase A, dentro de la transaccion corta de Version Creation."""
    logical_run_id = uuid.uuid4()
    session.add(
        AgentRun(
            logical_run_id=logical_run_id,
            role=role,
            producer_logical_run_id=None,
            task_type=task_type,
            target_object_version_id=target_object_version_id,
            context_package_id=context_package_id,
            requested_by=requested_by,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
            status="PENDING",
            claimed_by=None,
            lease_expires_at=None,
            lease_generation=0,
            max_attempts=max_attempts,
        )
    )
    session.flush()
    return logical_run_id


def acquire(session: Session, logical_run_id: uuid.UUID, worker_id: str) -> Optional[int]:
    """PENDING -> IN_PROGRESS. Devuelve la nueva lease_generation, o
    None si otro worker ya lo tomo (rows_affected=0)."""
    result = session.execute(
        update(AgentRun)
        .where(AgentRun.logical_run_id == logical_run_id, AgentRun.status == "PENDING")
        .values(
            status="IN_PROGRESS",
            claimed_by=worker_id,
            lease_expires_at=_new_lease_expiry_expr(),
            lease_generation=AgentRun.lease_generation + 1,
        )
        .returning(AgentRun.lease_generation)
    )
    return result.scalar_one_or_none()


def reclaim(session: Session, logical_run_id: uuid.UUID, worker_id: str) -> Optional[int]:
    """
    IN_PROGRESS + LEASE_IS_EXPIRED -> nuevo dueno, lease_generation+1.
    Devuelve la nueva generacion, o None si el lease en realidad sigue
    vigente o ya fue reclamado por otro worker (CR-59 + CODE-CR-69:
    condicion lease_expires_at <= clock_timestamp(), complementaria
    exacta de la usada en same-owner retry y en la autorizacion de
    finalizacion - clock_timestamp(), nunca now()/CURRENT_TIMESTAMP).

    CODE-CR-69 REABIERTO: a diferencia de
    refresh_lease_for_same_owner_retry()/mark_failed_if_owner()/
    authorize_terminal_success() (que SI necesitan
    _lock_agent_run_row_for_update() primero), esta funcion NO lo
    necesita: su predicado es LEASE_IS_EXPIRED (<=), no LEASE_IS_VALID
    (>). La expiracion es monotona en el tiempo - si el lease ya estaba
    vencido en el instante en que el WHERE se evaluo (incluso si eso
    fue ANTES de esperar un row lock retenido por otra transaccion),
    sigue vencido en cualquier instante posterior, sin excepcion. Un
    resultado "vencido" evaluado en un instante anterior nunca puede
    volverse falso mas tarde - por eso reutilizar un clock_timestamp()
    "viejo" aqui jamas produce una autorizacion incorrecta, al reves de
    lo que ocurre con LEASE_IS_VALID.

    CODE-CR-64 (Human Code Review de Build v2): si la reclamacion
    tiene exito, cierra atomicamente - en la MISMA transaccion que el
    llamador comitea a continuacion - cualquier AgentRunAttempt de este
    logical_run_id que haya quedado abierto (outcome IS NULL) por el
    worker anterior desaparecido, marcandolo CLAIM_LOST con
    finished_at. Por construccion (cada intento se cierra antes de
    crear el siguiente - ver create_attempt/close_attempt), a lo sumo
    existe UN intento abierto por logical_run_id en cualquier momento,
    por lo que no hace falta identificar la generacion especifica del
    intento abandonado. Sin check-then-act: el cierre solo se ejecuta
    si la reclamacion misma tuvo exito (mismo control de flujo,
    gateado por el UPDATE condicional anterior), y nunca afecta un
    intento que todavia no existe (el de la nueva generacion se crea
    DESPUES de este reclaim, en una llamada posterior a create_attempt).
    """
    result = session.execute(
        update(AgentRun)
        .where(
            AgentRun.logical_run_id == logical_run_id,
            AgentRun.status == "IN_PROGRESS",
            AgentRun.lease_expires_at <= func.clock_timestamp(),  # LEASE_IS_EXPIRED (CODE-CR-69)
        )
        .values(
            claimed_by=worker_id,
            lease_expires_at=_new_lease_expiry_expr(),
            lease_generation=AgentRun.lease_generation + 1,
        )
        .returning(AgentRun.lease_generation)
    )
    new_generation = result.scalar_one_or_none()

    if new_generation is not None:
        session.execute(
            update(AgentRunAttempt)
            .where(
                AgentRunAttempt.logical_run_id == logical_run_id,
                AgentRunAttempt.outcome.is_(None),
            )
            .values(outcome="CLAIM_LOST", finished_at=func.now())
        )

    return new_generation


def refresh_lease_for_same_owner_retry(
    session: Session, logical_run_id: uuid.UUID, worker_id: str, generation: int
) -> Optional[int]:
    """
    CR-58: verificacion atomica de ownership + vigencia + refresco del
    lease, SIN incrementar lease_generation. Devuelve la MISMA
    generation si tiene exito, o None si el lease ya expiro o el
    ownership cambio (el llamador debe entonces pasar por reclaim()).

    CODE-CR-69 REABIERTO: _lock_agent_run_row_for_update() se llama
    PRIMERO, en su propio statement sin predicado de tiempo, forzando a
    que cualquier espera de row lock ocurra ANTES de evaluar
    LEASE_IS_VALID - ver el docstring de _lock_agent_run_row_for_update()
    para la explicacion completa de por que esto es necesario (el
    UPDATE que sigue, ya con el lock en mano, evalua clock_timestamp()
    sin demora adicional).
    """
    if not _lock_agent_run_row_for_update(session, logical_run_id):
        return None

    result = session.execute(
        update(AgentRun)
        .where(
            AgentRun.logical_run_id == logical_run_id,
            AgentRun.claimed_by == worker_id,
            AgentRun.lease_generation == generation,
            AgentRun.status == "IN_PROGRESS",
            AgentRun.lease_expires_at > func.clock_timestamp(),  # LEASE_IS_VALID (CODE-CR-69)
        )
        .values(
            lease_expires_at=_new_lease_expiry_expr()
        )
        .returning(AgentRun.lease_generation)
    )
    return result.scalar_one_or_none()


def next_attempt_number(session: Session, logical_run_id: uuid.UUID) -> int:
    current_max = session.execute(
        select(func.coalesce(func.max(AgentRunAttempt.attempt_number), 0)).where(
            AgentRunAttempt.logical_run_id == logical_run_id
        )
    ).scalar_one()
    return current_max + 1


def create_attempt(
    session: Session,
    logical_run_id: uuid.UUID,
    attempt_number: int,
    lease_generation: int,
    provider: Optional[str] = None,
    model_identifier: Optional[str] = None,
    instructions_version: Optional[str] = None,
    output_schema_version: Optional[str] = None,
) -> uuid.UUID:
    """
    CODE-CR-62 (Human Code Review de Build v1): provider/model_identifier/
    instructions_version/output_schema_version se persisten en el
    momento de CREACION del intento (no como actualizacion posterior) -
    cada AgentRunAttempt registra exactamente los valores de la
    ejecucion real que le dio origen.
    """
    attempt_id = uuid.uuid4()
    session.add(
        AgentRunAttempt(
            attempt_id=attempt_id,
            logical_run_id=logical_run_id,
            attempt_number=attempt_number,
            lease_generation_at_creation=lease_generation,
            outcome=None,
            provider=provider,
            model_identifier=model_identifier,
            instructions_version=instructions_version,
            output_schema_version=output_schema_version,
        )
    )
    session.flush()
    return attempt_id


def close_attempt(
    session: Session,
    attempt_id: uuid.UUID,
    outcome: str,
    failure_detail: Optional[str] = None,
    output_payload: Optional[dict] = None,
) -> None:
    session.execute(
        update(AgentRunAttempt)
        .where(AgentRunAttempt.attempt_id == attempt_id, AgentRunAttempt.outcome.is_(None))
        .values(
            outcome=outcome,
            failure_detail=failure_detail,
            output_payload=output_payload,
            finished_at=func.now(),
        )
    )


def mark_failed_if_owner(
    session: Session, logical_run_id: uuid.UUID, worker_id: str, generation: int
) -> bool:
    """
    CR-58 + CR-59: marca agent_run.status='FAILED' por agotamiento de
    max_attempts, SOLO si quien lo solicita todavia posee
    ownership+generation+lease vigente en el MISMO statement. Un worker
    obsoleto o expirado nunca puede marcar FAILED
    (STALE_OR_EXPIRED_OWNER_CANNOT_COMMIT_FAILURE). Devuelve True si
    tuvo exito.

    CODE-CR-69 REABIERTO: _lock_agent_run_row_for_update() se llama
    PRIMERO - ver su docstring para la explicacion completa de por que
    clock_timestamp() solo en el WHERE de un UPDATE plano no basta
    cuando el statement debe esperar un row lock.
    """
    if not _lock_agent_run_row_for_update(session, logical_run_id):
        return False

    result = session.execute(
        update(AgentRun)
        .where(
            AgentRun.logical_run_id == logical_run_id,
            AgentRun.claimed_by == worker_id,
            AgentRun.lease_generation == generation,
            AgentRun.status == "IN_PROGRESS",
            AgentRun.lease_expires_at > func.clock_timestamp(),  # LEASE_IS_VALID (CODE-CR-69)
        )
        .values(status="FAILED")
        .returning(AgentRun.logical_run_id)
    )
    return result.scalar_one_or_none() is not None


def authorize_terminal_success(
    session: Session, logical_run_id: uuid.UUID, worker_id: str, generation: int
) -> bool:
    """
    CR-59, EL FIX: primer statement de Fase C. Exige atomicamente
    ownership + generation + status + LEASE_IS_VALID en el MISMO
    UPDATE que otorga la autoridad terminal. rows_affected=0 -> el
    llamador debe tratarlo como CLAIM_LOST y abortar el resto de Fase C
    sin excepcion - no existe ningun camino especial que permita
    finalizar solo porque todavia nadie reclamo un lease vencido.

    CODE-CR-69 REABIERTO (Human Code Review de Build v5, BLOCKER): esta
    es la funcion que expuso el defecto real -
    test_lock_wait_crossing_expiry_uses_wall_clock_not_transaction_start
    seguia fallando en Build v5 pese a que el WHERE ya usaba
    clock_timestamp() en vez de now(). _lock_agent_run_row_for_update()
    se llama PRIMERO, en su propio statement sin predicado de tiempo -
    ver su docstring para la explicacion completa de la causa raiz
    (PostgreSQL evalua el WHERE de un UPDATE plano ANTES de esperar el
    row lock; si la transaccion bloqueante nunca comitea un cambio real
    sobre la fila, no hay version nueva que dispare una re-evaluacion
    via EvalPlanQual) y de por que forzar la espera del lock en un
    SELECT...FOR UPDATE separado la cierra.
    """
    if not _lock_agent_run_row_for_update(session, logical_run_id):
        return False

    result = session.execute(
        update(AgentRun)
        .where(
            AgentRun.logical_run_id == logical_run_id,
            AgentRun.claimed_by == worker_id,
            AgentRun.lease_generation == generation,
            AgentRun.status == "IN_PROGRESS",
            AgentRun.lease_expires_at > func.clock_timestamp(),  # LEASE_IS_VALID (CODE-CR-69)
        )
        .values(status="SUCCEEDED")
        .returning(AgentRun.logical_run_id)
    )
    return result.scalar_one_or_none() is not None


# ---------------------------------------------------------------------------
# CODE-CR-66 (Human Code Review de Build v3, BLOCKER) - operaciones
# fusionadas de autorizacion de ownership + creacion del siguiente
# AgentRunAttempt. Reemplazan el patron previo de Build v3 donde
# acquire/reclaim/refresh_lease_for_same_owner_retry y create_attempt
# estaban separados por COMMITs independientes en el llamador
# (orchestrator._phase_b_execute), lo que permitia que un worker
# perdiera ownership en el hueco entre ambos COMMITs e insertara de
# todos modos un AgentRunAttempt bajo una generacion ya vencida/perdida
# (reabriendo CODE-CR-64: ese Attempt tardio quedaba huerfano, con
# outcome IS NULL, sin que nadie lo cerrara).
#
# Cada funcion de esta seccion reutiliza las primitivas ya existentes y
# ya testeadas arriba (acquire/reclaim/refresh_lease_for_same_owner_retry/
# next_attempt_number/create_attempt/mark_failed_if_owner) SIN llamar a
# session.commit() en ningun punto intermedio entre ellas - el
# UPDATE que otorga/renueva la autoridad y el INSERT del siguiente
# Attempt (o el UPDATE que marca FAILED por agotamiento de
# max_attempts) ocurren dentro de la MISMA transaccion PostgreSQL
# abierta. El llamador es responsable de un UNICO session.commit()
# inmediatamente despues de recibir el AttemptAuthorization resultante.
#
# UNIQUE(logical_run_id, attempt_number) (ya declarada en el modelo,
# ver models.py) se preserva intacta como defensa estructural
# ADICIONAL - el mecanismo primario de fencing sigue siendo el UPDATE
# condicional sobre agent_run (claimed_by + lease_generation + status +
# LEASE_IS_VALID/LEASE_IS_EXPIRED), nunca esa constraint por si sola.
# ---------------------------------------------------------------------------


def acquire_and_create_first_attempt(
    session: Session,
    logical_run_id: uuid.UUID,
    worker_id: str,
    provider: Optional[str] = None,
    model_identifier: Optional[str] = None,
    instructions_version: Optional[str] = None,
    output_schema_version: Optional[str] = None,
) -> AttemptAuthorization:
    """
    PENDING -> IN_PROGRESS (acquire) + INSERT del Attempt 1, sin COMMIT
    intermedio. Si acquire() falla (AgentRun ya no esta PENDING - otro
    worker gano la adquisicion primero, o ya esta en un estado
    posterior), NINGUN Attempt se crea y generation=None - el llamador
    debe entonces intentar reclaim_and_create_attempt() para el camino
    de reclamacion sobre un lease ya vencido.
    """
    generation = acquire(session, logical_run_id, worker_id)
    if generation is None:
        return AttemptAuthorization(generation=None, attempt_id=None)

    attempt_id = create_attempt(
        session,
        logical_run_id,
        attempt_number=1,
        lease_generation=generation,
        provider=provider,
        model_identifier=model_identifier,
        instructions_version=instructions_version,
        output_schema_version=output_schema_version,
    )
    return AttemptAuthorization(generation=generation, attempt_id=attempt_id)


def reclaim_and_create_attempt(
    session: Session,
    logical_run_id: uuid.UUID,
    worker_id: str,
    max_attempts: int,
    provider: Optional[str] = None,
    model_identifier: Optional[str] = None,
    instructions_version: Optional[str] = None,
    output_schema_version: Optional[str] = None,
) -> AttemptAuthorization:
    """
    Reclamacion condicional (IN_PROGRESS + LEASE_IS_EXPIRED) + cierre
    de cualquier AgentRunAttempt abandonado como CLAIM_LOST (CODE-CR-64,
    ya implementado dentro de reclaim() y ahora fusionado aqui) +
    validacion de max_attempts + INSERT del siguiente Attempt bajo la
    NUEVA generacion - todo sin COMMIT intermedio.

    Si reclaim() falla (el lease en realidad sigue vigente, o ya fue
    reclamado por otro worker primero), generation=None y NINGUN
    Attempt se toca.

    Si la reclamacion tiene exito pero el siguiente attempt_number
    excederia max_attempts, AgentRun.status pasa a 'FAILED'
    atomicamente (misma transaccion, misma generacion recien
    adquirida) y NINGUN Attempt nuevo se crea -
    failed_max_attempts=True se lo senaliza al llamador.
    """
    generation = reclaim(session, logical_run_id, worker_id)
    if generation is None:
        return AttemptAuthorization(generation=None, attempt_id=None)

    next_number = next_attempt_number(session, logical_run_id)
    if next_number > max_attempts:
        mark_failed_if_owner(session, logical_run_id, worker_id, generation)
        return AttemptAuthorization(generation=generation, attempt_id=None, failed_max_attempts=True)

    attempt_id = create_attempt(
        session,
        logical_run_id,
        attempt_number=next_number,
        lease_generation=generation,
        provider=provider,
        model_identifier=model_identifier,
        instructions_version=instructions_version,
        output_schema_version=output_schema_version,
    )
    return AttemptAuthorization(generation=generation, attempt_id=attempt_id)


def refresh_and_create_retry_attempt(
    session: Session,
    logical_run_id: uuid.UUID,
    worker_id: str,
    generation: int,
    max_attempts: int,
    provider: Optional[str] = None,
    model_identifier: Optional[str] = None,
    instructions_version: Optional[str] = None,
    output_schema_version: Optional[str] = None,
) -> AttemptAuthorization:
    """
    Validacion atomica de ownership + generation + status +
    LEASE_IS_VALID (CR-58), refresco del lease SIN incrementar
    lease_generation, + validacion de max_attempts + INSERT del
    siguiente Attempt bajo la MISMA generacion - todo sin COMMIT
    intermedio.

    Si refresh_lease_for_same_owner_retry() falla (el lease ya vencio o
    el ownership cambio de mano), generation=None y NINGUN Attempt se
    toca - el llamador debe pasar a reclaim_and_create_attempt().

    Si el refresco tiene exito pero el siguiente attempt_number
    excederia max_attempts, AgentRun.status pasa a 'FAILED'
    atomicamente bajo la MISMA generacion vigente, sin crear ningun
    Attempt nuevo.
    """
    refreshed_generation = refresh_lease_for_same_owner_retry(
        session, logical_run_id, worker_id, generation
    )
    if refreshed_generation is None:
        return AttemptAuthorization(generation=None, attempt_id=None)

    next_number = next_attempt_number(session, logical_run_id)
    if next_number > max_attempts:
        mark_failed_if_owner(session, logical_run_id, worker_id, refreshed_generation)
        return AttemptAuthorization(
            generation=refreshed_generation, attempt_id=None, failed_max_attempts=True
        )

    attempt_id = create_attempt(
        session,
        logical_run_id,
        attempt_number=next_number,
        lease_generation=refreshed_generation,
        provider=provider,
        model_identifier=model_identifier,
        instructions_version=instructions_version,
        output_schema_version=output_schema_version,
    )
    return AttemptAuthorization(generation=refreshed_generation, attempt_id=attempt_id)
