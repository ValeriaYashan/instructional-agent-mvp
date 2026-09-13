# MVP Instructional Agent

Slice implementado: `PRODUCE_LEARNING_OBJECT`.

- **Iteracion 1** (HUMAN_APPROVED_CLOSED): Approved State Pointer / Commit.
- **Iteracion 2** (HUMAN_APPROVED_CLOSED): Task Intake, Workflow/State
  Transition Manager, `workflow_transition`, generalización de
  `idempotency_operation`.
- **Iteracion 3** (este build): Context Engine — Pasos 1-9 del contrato
  determinístico (§6.6), `instructional_object` (identidad estable,
  CR-33), `object_relation`, `evidence_source`, `context_package`,
  `DependencyResolver`/`EvidenceRetriever` con adaptadores PostgreSQL
  mínimos (CR-34), idempotencia de solicitud distinta de frescura de
  contexto (CR-35), hash canónico con `context_contract_version`
  (CR-36/CR-38), consistencia de instantánea vía `REPEATABLE READ`
  (CR-37).

No incluye Routing Contract, Producer/QA, AgentRun, pgvector, retrieval
semántico, LMS ni Human Approval Boundary — quedan para iteraciones
posteriores.

Documentos autoritativos:
- `MVP TECHNICAL DESIGN — PHASE 1/2/3` (HUMAN_APPROVED)
- `MVP IMPLEMENTATION — ITERATION 1: IMPLEMENTATION PROPOSAL v0.2` (HUMAN_APPROVED_CLOSED)
- `MVP IMPLEMENTATION — ITERATION 2: PROPOSAL v0.4 + IC-31` (HUMAN_APPROVED_CLOSED)
- `MVP IMPLEMENTATION — ITERATION 3: CONTEXT ENGINE PROPOSAL v0.3 — FINAL BASELINE CANDIDATE` (HUMAN_APPROVED)

## Iteracion 3 — que se agregó

- **`instructional_object`**: identidad estable del objeto instruccional
  (CR-33) — `object_type` es invariante de esta identidad, no de una
  versión particular. `object_version.object_id` es ahora FK hacia
  esta tabla, sin cambiar el significado ya aprobado de `object_version`.
- **Trigger de compatibilidad** (`ensure_instructional_object_exists`):
  auto-provisiona la fila de `instructional_object` (con
  `object_type=NULL`) antes de cada INSERT en `object_version` cuando
  no existe todavía — necesario porque el código de prueba de
  Iteraciones 1-2 inserta `ObjectVersion` directamente sin pasar por
  ningún repositorio, y esos tests debían permanecer sin modificar.
  Documentado como desviación explícita respecto a la Proposal v0.3.
- **`object_relation`** / **`evidence_source`**: adaptadores mínimos y
  explícitamente no semánticos para los Pasos 3/6 y 7 del Context Engine.
- **`context_package`**: salida inmutable del Paso 9, con
  `context_package_hash` canónico (excluye `context_package_id` y
  `assembled_at`; incluye `context_contract_version`, CR-38).
- **`idempotency_operation`** amplía `operation_type` a un tercer valor,
  `CONTEXT_ASSEMBLY`, con su propia semántica de replay (CR-35): mismo
  `idempotency_key` + mismo payload de solicitud siempre devuelve el
  mismo `context_package_id`, sin importar si el estado aprobado
  cambió después.
- Migración `0003`, compatible hacia atrás con una base ya poblada por
  Iteraciones 1-2 (`0001` y `0002` permanecen inmutables).

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

Aplica `0001` (Iteración 1), `0002` (Iteración 2) y `0003` (Iteración 3:
`instructional_object` con backfill automático desde `object_version`
existente, trigger de compatibilidad, `object_relation`,
`evidence_source`, `context_package`, ampliación de
`idempotency_operation` a tres `operation_type`). `0003` puede
aplicarse tanto sobre una base vacía como sobre una ya poblada por
Iteraciones 1-2.

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

Todos los tests (incluye la regresión completa de Iteraciones 1 y 2
contra el esquema post-`0003`):

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

## Correcciones aplicadas tras Human Code Review (build v2)

- **CODE-CR-39**: cada relación requerida ahora declara explícitamente
  el `object_type` que su destino debe tener. Una relación que existe
  pero apunta a un objeto de tipo incorrecto (o sin tipo, legacy) ya
  no satisface el requisito — se trata como `BLOCKED_BY_DEPENDENCY`,
  igual que una relación inexistente.
- **CODE-CR-40**: `pinned_version_id` se valida completamente antes de
  usarse — existencia, pertenencia a `target_object_id`
  (`INVALID_OBJECT_VERSION`, vocabulario reutilizado de Iteración 2,
  no inventado), status suficiente (`APPROVAL_INSUFFICIENT` como gap,
  no como rechazo), y comparación contra el puntero aprobado por
  `version_number` — nunca por desigualdad de UUID.
- **CODE-CR-41**: `evidence[]` en el Context Package preserva la tupla
  completa de Evidence Sufficiency (`requirement_level`,
  `blocking_behavior`, `resolution_status`), no solo el resultado.
- **CODE-CR-42**: se retiró por completo el trigger de
  auto-provisionamiento de `instructional_object`. El backfill de la
  migración `0003` sigue existiendo (evento único para filas legacy de
  Iteraciones 1-2), pero no hay ningún mecanismo permanente que cree
  identidades implícitamente — todo `object_id` nuevo requiere su
  `InstructionalObject` creado explícitamente antes de cualquier
  `ObjectVersion`. Los *fixtures* de los tests de Iteración 1/2 se
  actualizaron para reflejar este orden correcto (autorizado
  explícitamente por esta revisión — no cambia ninguna aserción ni
  comportamiento aprobado, solo la preparación de datos).
- **Corrección secundaria**: `object_relation` ahora tiene
  `UNIQUE(from_object_id, relation_type)` — cada tipo de relación es
  singular por objeto origen, forzado a nivel de base de datos, lo que
  hace seguro el uso de `scalar_one_or_none()` en el resolver.
- **Corrección secundaria**: el reintento por conflicto de
  serialización bajo `REPEATABLE READ` (CR-37) ahora se limita
  exclusivamente a los SQLSTATE `40001`/`40P01` — cualquier otro
  `OperationalError` se propaga, no se reintenta como si fuera
  contención de concurrencia.
- **Corrección de documentación**: `TASK_TYPE_UNRESOLVED` se describe
  correctamente como un rechazo de Task Intake previo a cualquier
  acceso a base de datos — nunca se persiste, a diferencia de
  `MAPPING_UNDEFINED`, `NEWER_VERSION_AVAILABLE` e
  `INVALID_OBJECT_VERSION`.

## Limitaciones conocidas de esta iteracion

- No existe todavia Producer, QA, Context Engine, Routing Resolution
  ni Human Approval Boundary funcional — llegan en iteraciones
  posteriores sobre esta misma fundacion.
- El modelo de `audit_event` es minimo (ver docstring en
  `src/persistence/models.py`), no el modelo completo de Audit Trail
  de Fase 3 (`correlation_id`, `causation_id`, `actor_type`, `run_id`).
- **Los tests nuevos de Iteración 3 (`tests/unit/test_context_package_hash.py`,
  `tests/integration/test_context_engine.py`) no han sido ejecutados
  contra un PostgreSQL real en el entorno donde se generó este build**,
  por la misma razón que en Iteraciones 1 y 2: sin Docker disponible.
  Los 7 tests unitarios de hash SÍ corrieron realmente en este entorno
  (no requieren base de datos) y pasaron. Los 11 tests de integración
  del Context Engine se verificaron estáticamente (recolección junto
  con los 31 tests de Iteraciones 1-2 — 42 en total —, compilación de
  todo el DDL nuevo y de las sentencias SQL críticas). La suite
  completa, incluida la regresión de Iteraciones 1-2 contra el esquema
  post-`0003`, debe correrse en una máquina o CI con Docker antes de
  dar por cerrada la Iteración 3. Comando exacto: `uv run pytest -v`.
- Alcance reducido documentado explícitamente (Iteration 3 Proposal
  v0.3, no oculto): `DependencyResolver`/`EvidenceRetriever` resuelven
  contra `object_relation`/`evidence_source` poblados manualmente —
  no importan el grafo completo de Gates 5-7 ni usan retrieval
  semántico (pgvector), ambos diferidos a una iteración futura.
- `APPROVAL_INSUFFICIENT` (Paso 5) no se ejercita en este alcance:
  todo objeto resuelto vía `approved_state_pointer` es, por
  construcción de Iteración 1, `APPROVED_CONTENT` — no hay ninguna
  tarea en el mapeo de esta iteración que acepte `QA_PASS_PROVISIONAL`
  como suficiente.
