"""
Correccion de guarda transaccional (Human Code Review de Build v3, no
es un CODE-CR numerado pero es obligatoria en v4):
_ensure_no_open_transaction() debe ser FAIL-FAST (raise RuntimeError),
nunca hacer rollback() implicito (puede ocultar un bug real y
descartar trabajo no commiteado sin que nadie se entere), y no
depender de assert como unica garantia (se desactiva con `python -O`).

Estos tests usan un doble de prueba minimo (no una Session real de
SQLAlchemy/PostgreSQL) porque _ensure_no_open_transaction solo llama a
session.in_transaction() y, en la version PREVIA (ya corregida), a
session.rollback() - exactamente la superficie que hace falta
verificar aqui, sin depender de una base de datos real.
"""

from __future__ import annotations

import pytest

from src.persistence.repositories.orchestrator import _ensure_no_open_transaction


class _FakeSessionWithOpenTransaction:
    """Simula una Session con una transaccion PostgreSQL todavia abierta
    en el instante exacto en que se la interroga."""

    def __init__(self):
        self.rollback_called = False
        self.commit_called = False

    def in_transaction(self) -> bool:
        return True

    def rollback(self) -> None:
        self.rollback_called = True

    def commit(self) -> None:
        self.commit_called = True


class _FakeSessionWithoutOpenTransaction:
    def in_transaction(self) -> bool:
        return False


def test_ensure_no_open_transaction_raises_when_transaction_is_open():
    """Fail-fast: si hay una transaccion abierta, la funcion debe
    lanzar RuntimeError - no debe permitir que la ejecucion continue
    silenciosamente hacia la llamada al proveedor."""
    session = _FakeSessionWithOpenTransaction()

    with pytest.raises(RuntimeError):
        _ensure_no_open_transaction(session)


def test_ensure_no_open_transaction_never_calls_rollback_implicitly():
    """La correccion exigida por el Human Code Review: un rollback()
    automatico dentro de esta guarda podria ocultar un bug real y
    descartar trabajo no commiteado sin que nadie se entere - por lo
    tanto esta funcion NUNCA debe invocar session.rollback() ni
    session.commit() por su cuenta, bajo ninguna circunstancia."""
    session = _FakeSessionWithOpenTransaction()

    with pytest.raises(RuntimeError):
        _ensure_no_open_transaction(session)

    assert session.rollback_called is False
    assert session.commit_called is False


def test_ensure_no_open_transaction_passes_silently_when_already_closed():
    """Camino feliz: si no hay ninguna transaccion abierta, la funcion
    no debe lanzar ni hacer nada - execute() puede invocarse con
    seguridad."""
    session = _FakeSessionWithoutOpenTransaction()

    _ensure_no_open_transaction(session)  # no debe lanzar


def test_ensure_no_open_transaction_error_message_names_the_real_cause():
    """El mensaje de error debe orientar a corregir la causa real (una
    transaccion previa que no se cerro explicitamente) y no sugerir
    ningun mecanismo de recuperacion automatica implicita."""
    session = _FakeSessionWithOpenTransaction()

    with pytest.raises(RuntimeError, match="transaccion PostgreSQL"):
        _ensure_no_open_transaction(session)
