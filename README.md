# MVP Instructional Agent — Iteracion 1: Fundacion de persistencia y concurrencia

Slice implementado: `PRODUCE_LEARNING_OBJECT` — unicamente la fundacion de
Approved State Pointer / Approved State Commit. No incluye Producer,
QA, Context Engine, Routing Resolution ni Human Approval funcional.

Documentos autoritativos:
- `MVP TECHNICAL DESIGN — PHASE 1/2/3` (HUMAN_APPROVED)
- `MVP IMPLEMENTATION — ITERATION 1: IMPLEMENTATION PROPOSAL v0.2` (HUMAN_APPROVED)

## Requisitos

- Python 3.12
- [uv](https://docs.astral.sh/uv/) (gestor de dependencias aprobado)
- Docker + Docker Compose (para PostgreSQL local y para los tests de
  integracion via testcontainers)

## Instalacion

```bash
uv sync --all-extras
```

Esto instala `sqlalchemy`, `psycopg[binary]`, `alembic` (dependencias de
runtime) y `pytest` + `testcontainers[postgres]` (dependencias de
desarrollo, definidas en `[tool.uv].dev-dependencies`).

## Levantar PostgreSQL local

```bash
docker compose up -d
cp .env.example .env
```

Verificar que esta saludable:

```bash
docker compose ps
```

## Aplicar migraciones

```bash
export $(cat .env | xargs)
uv run alembic upgrade head
```

Esto crea las tablas `object_version`, `approved_state_pointer`,
`approved_state_commit` y `audit_event`, con las foreign keys
compuestas que garantizan la integridad puntero/version y
commit/version (CR-19, CR-20).

## Ejecutar tests

Tests unitarios (no requieren base de datos):

```bash
uv run pytest tests/unit -v
```

Tests de integracion (requieren Docker corriendo — levantan su propio
PostgreSQL 16 efímero via testcontainers, independiente del de
`docker compose`, y aplican las migraciones de Alembic contra ese
contenedor antes de correr):

```bash
uv run pytest tests/integration -v
```

Todos los tests:

```bash
uv run pytest -v
```

## Que cubre la suite de Iteracion 1

- Primera aprobacion desde `NULL` (CR-14)
- Segunda aprobacion valida, avance del puntero
- `STATE_CONFLICT` ante version previa esperada obsoleta, sin fila
  huerfana en `approved_state_commit` (CR-13)
- Idempotencia: mismo `idempotency_key` + mismo payload → mismo
  resultado; mismo `idempotency_key` + payload distinto →
  `IDEMPOTENCY_CONFLICT` (CR-18)
- Integridad relacional cruzada: un puntero o un commit no pueden
  referenciar una version de un `object_id` distinto — forzado a
  nivel de base de datos por foreign keys compuestas (CR-19, CR-20)
- Atomicidad: todo commit exitoso tiene exactamente un `audit_event`
  correspondiente; todo `STATE_CONFLICT` no deja ni commit ni audit
  event huerfanos (CR-13, CR-17)
- Concurrencia: primera aprobacion concurrente (solo una tiene exito);
  la misma `idempotency_key` nunca antes vista, solicitada de forma
  concurrente con el mismo payload, produce exactamente un commit, una
  mutacion de puntero y un audit event — nunca duplicados

## Correccion aplicada tras Human Code Review (CODE-CR-01)

La primera entrega de esta iteracion tenia un defecto de atomicidad:
la reserva de concurrencia para `idempotency_key` se confirmaba
(`COMMIT`) en una transaccion separada, antes de saber si la mutacion
del puntero tendria exito — lo que podia dejar una fila `PENDING`
persistida en `approved_state_commit` sin su mutacion de puntero ni su
`audit_event` correspondientes, violando CR-13.

Correccion: se introdujo una tabla nueva y aislada,
`idempotency_operation`, exclusivamente para la reserva de
concurrencia. Esa reserva se inserta como el **primer statement de la
misma transaccion atomica** que despues valida, muta el puntero,
inserta el `ApprovedStateCommit` final e inserta el `AuditEvent`.
`approved_state_commit.outcome` ahora tiene un `CHECK` que solo
permite `'COMMITTED'` — ya no existe ningun valor `'PENDING'` posible
en esa tabla. El detalle completo del mecanismo de bloqueo esta
documentado en el docstring de `src/persistence/repositories/approved_state.py`.

## Limitaciones conocidas de esta iteracion

- No existe todavia Producer, QA, Context Engine, Routing Resolution
  ni Human Approval Boundary funcional — llegan en iteraciones
  posteriores sobre esta misma fundacion.
- El modelo de `audit_event` es minimo (ver docstring en
  `src/persistence/models.py`), no el modelo completo de Audit Trail
  de Fase 3 (`correlation_id`, `causation_id`, `actor_type`, `run_id`).
- **Los tests de integracion (`tests/integration/`) no han sido
  ejecutados contra un PostgreSQL real en el entorno donde se generó
  este repositorio, porque ese entorno no tiene Docker disponible.**
  Se verificaron estaticamente (recoleccion sin errores de import,
  compilacion de todo el DDL y de las sentencias SQL criticas contra
  el dialecto PostgreSQL), pero la ejecucion real —incluyendo las
  pruebas de concurrencia, que son las que mas importan aqui— debe
  correrse en una maquina o CI con Docker antes de dar por cerrada la
  Iteracion 1. Comando exacto: `uv run pytest tests/integration -v`.
