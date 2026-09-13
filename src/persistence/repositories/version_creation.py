"""
Version Creation - Iteracion 4, Proposal v0.5 S9 (CR-57).

Unica responsabilidad autorizada para crear filas nuevas en
object_version en el flujo de Producer Execution, o para decidir la
reutilizacion de una fila ELIGIBLE_PENDING virgen existente. Ni
Producer ni Workflow Transition Manager tienen este poder (CR-49).

Serializa por object_id mediante SELECT ... FOR UPDATE sobre
instructional_object, aislamiento READ COMMITTED (no REPEATABLE READ -
esa garantia es especifica de Context Engine/I3 y no aplica aqui, la
consistencia proviene del lock de fila, no de aislamiento de
instantanea). Debe llamarse dentro de la transaccion corta de Fase A
del Orchestrator (ver orchestrator.py), nunca fuera de ella.

Un solo recurso bloqueable (InstructionalObject) por transaccion -
sin ciclos posibles, sin necesidad de mecanismo de prevencion de
deadlock adicional (Proposal v0.5 S9.3).
"""

from __future__ import annotations

import uuid

from sqlalchemy import func, not_, select
from sqlalchemy.orm import Session

from src.persistence.models import AgentRun, InstructionalObject, ObjectVersion, ObjectVersionContent


def lock_instructional_object(session: Session, object_id: str) -> None:
    """
    Primer paso obligatorio de Fase A (Proposal v0.5 S9.2). Debe
    llamarse antes de cualquier lectura/decision de reutilizacion o
    creacion - leer antes de tomar el lock no sirve, el lock no
    refresca una lectura ya hecha.
    """
    session.execute(
        select(InstructionalObject.object_id)
        .where(InstructionalObject.object_id == object_id)
        .with_for_update()
    ).scalar_one()


def resolve_or_create_object_version(session: Session, object_id: str) -> uuid.UUID:
    """
    Debe llamarse INMEDIATAMENTE DESPUES de lock_instructional_object,
    dentro de la MISMA transaccion. Re-consulta bajo el lock (S9.2 paso
    1) - nunca reutiliza una lectura hecha antes del lock.
    """
    active_run_subquery = (
        select(AgentRun.logical_run_id)
        .where(
            AgentRun.target_object_version_id == ObjectVersion.object_version_id,
            AgentRun.status.in_(("PENDING", "IN_PROGRESS")),
        )
        .exists()
    )
    content_subquery = (
        select(ObjectVersionContent.object_version_id)
        .where(ObjectVersionContent.object_version_id == ObjectVersion.object_version_id)
        .exists()
    )

    reusable = session.execute(
        select(ObjectVersion.object_version_id)
        .where(
            ObjectVersion.object_id == object_id,
            ObjectVersion.status == "ELIGIBLE_PENDING",
            not_(content_subquery),
            not_(active_run_subquery),
        )
        .order_by(ObjectVersion.version_number.asc())
        .limit(1)
    ).scalar_one_or_none()

    if reusable is not None:
        return reusable

    next_version_number = session.execute(
        select(func.coalesce(func.max(ObjectVersion.version_number), 0) + 1).where(
            ObjectVersion.object_id == object_id
        )
    ).scalar_one()

    new_version_id = uuid.uuid4()
    session.add(
        ObjectVersion(
            object_version_id=new_version_id,
            object_id=object_id,
            version_number=next_version_number,
            status="ELIGIBLE_PENDING",
        )
    )
    # Materializar la fila padre antes de que agent_run la referencie en
    # la misma transaccion (mismo patron que CODE-CR-45, Iteracion 3).
    session.flush()
    return new_version_id
