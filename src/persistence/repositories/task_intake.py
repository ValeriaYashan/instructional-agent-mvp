"""
Task Intake (Iteracion 2).

Valida `task_type` contra el catalogo cerrado ANTES de cualquier
interaccion con la base de datos - ni siquiera toca
idempotency_operation. Un task_type invalido no es un intento de
operacion legitimo, por lo que no consume ni contamina el mecanismo de
idempotencia de Workflow Transition.

Catalogo cerrado de esta iteracion: unicamente 'PRODUCE_LEARNING_OBJECT'
(mismo unico task_type usado en Iteracion 1 para Approved State
Commit). Agregar un task_type nuevo requeriria una decision explicita
fuera de este alcance.
"""

from __future__ import annotations

KNOWN_TASK_TYPES: tuple[str, ...] = ("PRODUCE_LEARNING_OBJECT",)


def is_known_task_type(task_type: str) -> bool:
    return task_type in KNOWN_TASK_TYPES
