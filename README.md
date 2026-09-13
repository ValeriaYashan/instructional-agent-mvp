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
  Todo `object_id` nuevo requiere su `InstructionalObject` creado
  explícitamente antes de cualquier `ObjectVersion` que lo referencie —
  no existe ningún trigger ni mecanismo de auto-provisionamiento
  permanente (`CODE-CR-42`).
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
- Los 63 tests (11 unitarios + 52 de integración) de Iteración 3 fueron
  ejecutados realmente contra PostgreSQL 16 en GitHub Codespaces y
  pasaron (`63 passed, 1 warning`). Único warning no bloqueante:
  deprecación de `path_separator` de Alembic.
- **`CODE-CR-45`**: se corrigió un problema de materialización de la
  fila padre de `ContextPackage` — se agregó `session.flush()`
  inmediatamente después de `session.add(ContextPackage(...))`, antes
  de que `AuditEvent`/`IdempotencyOperation` la referenciaran en la
  misma transacción. El `flush()` no hace commit; la atomicidad se
  preserva.
- **`CODE-CR-46`**: se corrigió la garantía real de `REPEATABLE READ`
  (CR-37) — `session.connection(execution_options={"isolation_level":
  "REPEATABLE READ"})` no tenía efecto si la conexión ya tenía una
  transacción establecida (`SAWarning`). Se reemplazó por
  `session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE
  READ"))` como primer statement real de cada intento, con una guarda
  explícita (`session.in_transaction()`) que exige una `Session` sin
  transacción activa. Se agregó un test de integración que verifica
  `SHOW transaction_isolation` desde dentro de los Pasos del Context
  Engine.
- **Iteración 3 quedó `HUMAN_APPROVED_CLOSED`** tras esa verificación.
- Alcance reducido documentado explícitamente (Iteration 3 Proposal
  v0.3, no oculto): `DependencyResolver`/`EvidenceRetriever` resuelven
  contra `object_relation`/`evidence_source` poblados manualmente —
  no importan el grafo completo de Gates 5-7 ni usan retrieval
  semántico (pgvector), ambos diferidos a una iteración futura.
- `APPROVAL_INSUFFICIENT` (Paso 5) no se ejercita en el alcance de
  Context Engine: todo objeto resuelto vía `approved_state_pointer` es,
  por construcción de Iteración 1, `APPROVED_CONTENT`. Sí se ejercita
  en Iteración 4 para `pinned_version_id` con status insuficiente (ver
  más abajo).

## Iteracion 4 — Producer Execution & Agent Run Lifecycle

Implementa el vertical slice desde `ContextPackage` hasta contenido
generado, validado estructuralmente y persistido, con Agent Run
lifecycle real y protegido contra condiciones de carrera — sin QA
independiente, sin Routing, sin Human Approval, sin invocar Approved
State Commit. Diseño completo en `MVP IMPLEMENTATION — ITERATION 4:
PRODUCER EXECUTION & AGENT RUN LIFECYCLE — PROPOSAL v0.5 — FINAL
BASELINE CANDIDATE` (`HUMAN_APPROVED`, CR-47 a CR-59).

- **`agent_run` / `agent_run_attempt`**: unidad lógica de trabajo con
  *lease* y *fencing* por `lease_generation` (CR-56, CR-59). A lo sumo
  un intento exitoso por `logical_run_id`, forzado por índice único
  parcial de PostgreSQL (CR-47). Reintento técnico bajo *ownership*
  vigente sin incrementar `lease_generation` (CR-58); reclamación tras
  vencimiento sí la incrementa. Autorización de finalización exige
  atómicamente `claimed_by` + `lease_generation` + `status` +
  `lease_expires_at > clock_timestamp()` en el mismo `UPDATE` (CR-59,
  corregido por CODE-CR-69) — un worker con *lease* vencido nunca puede
  finalizar, aunque nadie lo haya reclamado todavía. La autoridad
  temporal es exclusivamente el reloj de PostgreSQL
  (`clock_timestamp()`, nunca `now()`/`CURRENT_TIMESTAMP` ni el reloj
  de la aplicación) — y, para `authorize_terminal_success`/
  `mark_failed_if_owner`/`refresh_lease_for_same_owner_retry`, precedida
  por un `SELECT ... FOR UPDATE` explícito que fuerza cualquier espera
  de *row lock* a resolverse antes de evaluar la vigencia (CODE-CR-69).
- **`object_version_content`**: payload de contenido generado, 1:0..1
  con `ObjectVersion` existente — no se introdujo `ArtifactVersion`
  como entidad separada (CR-48, rechazado explícitamente).
- **`Version Creation`**: serializada por `object_id` vía
  `SELECT ... FOR UPDATE` sobre `InstructionalObject` (CR-57).
  `UNIQUE(object_id, version_number)` agregada como extensión aditiva.
- **`Workflow Transition Manager`**: extendido con
  `apply_transition_in_transaction`, sin `commit()` propio, reutilizada
  por el Orchestrator dentro de su transacción atómica de Fase C
  (CR-55). `request_workflow_transition` (API pública de Iteración 2)
  preservada con backward compatibility total.
- **`idempotency_operation`**: cuarto `operation_type`,
  `PRODUCER_EXECUTION`, resuelve hacia `agent_run_id` — nunca hacia el
  contenido directamente (CR-54). Las invariantes de los tres tipos
  anteriores quedan sin modificación.
- **`ProducerExecutor`**: interfaz provider-neutral, con adaptador
  `FakeProducerAdapter` (tests) y `AnthropicProducerAdapter` (no
  ejercitado sin credenciales reales en este build).
- Migración `0004`, compatible hacia atrás, con guardas de downgrade.
- Build v1: 25 tests unitarios ejecutados realmente (`25 passed`); 66
  tests de integración recolectados sin errores de import.
- **Build v2 — correcciones de Human Code Review**:
  - **`CODE-CR-60`**: el Producer recibía un `dict` vacío en vez del
    `ContextPackage` real. Se corrigió materializando el
    `ContextPackage` inmutable correspondiente a
    `agent_run.context_package_id` una sola vez por ejecución de
    Fase B, antes del bucle de intentos, y reutilizándolo idéntico en
    cada llamada al proveedor. `execution_contract.context_package_hash`
    ahora se puebla con el hash realmente persistido.
  - **`CODE-CR-61`**: existía una transacción PostgreSQL abierta
    (`autobegin` de SQLAlchemy 2.x) durante la primera llamada al
    proveedor. Se corrigió cerrando explícitamente (`rollback()`) cada
    lectura previa a la construcción del `execution_contract`, más una
    guarda `_ensure_no_open_transaction()` verificada en runtime
    inmediatamente antes de cada llamada a `ProducerExecutor.execute()`.
  - **`CODE-CR-62`**: `AnthropicProducerAdapter._build_prompt`/
    `_extract_structured_output` implementados de forma real y
    determinística (antes levantaban `NotImplementedError`), testeables
    mediante *client injection* sin credenciales ni red real.
    `agent_run_manager.create_attempt` ahora persiste
    `provider`/`model_identifier`/`instructions_version`/
    `output_schema_version` en el momento de creación de cada intento.
  - Build v2: 33 tests unitarios ejecutados realmente (`33 passed`); 71
    tests de integración recolectados sin errores de import — no
    ejecutados contra PostgreSQL real en el entorno donde se generó
    este build (sin Docker disponible).
- **Build v3 — correcciones de Human Code Review**:
  - **`CODE-CR-63`**: `context_package_id` y `max_attempts` se pasaban
    a Fase B desde variables locales de la solicitud actual, en vez de
    desde `AgentRun` (la única autoridad para un `logical_run` ya
    creado). Se corrigió leyendo, en una transacción corta y cerrada,
    el snapshot completo (`target_object_version_id`,
    `context_package_id`, `max_attempts`) directamente desde la fila
    `AgentRun` inmediatamente después de resolver `logical_run_id` en
    Fase A — antes de invocar Fase B, en ambas ramas (creación nueva o
    *replay*).
  - **`CODE-CR-64`**: `reclaim()` ahora cierra atómicamente, dentro de
    la misma operación condicional, cualquier `AgentRunAttempt` que
    haya quedado abierto (`outcome IS NULL`) del *worker* anterior
    desaparecido, marcándolo `CLAIM_LOST` con `finished_at` — nunca
    queda un intento histórico sin cerrar.
  - **`CODE-CR-65`**: el prompt del `AnthropicProducerAdapter` ahora
    declara explícitamente el contrato de salida completo (los cinco
    campos exactos, el `object_type` autoritativo real —no solo el
    nombre del schema—, la lista cerrada de referencias permitidas, la
    prohibición de campos adicionales y de *markdown fences*). Se
    agregó `ExecutionContract.expected_object_type` como metadata de
    ejecución provider-neutral, sin modificar el `ContextPackage`
    persistido.
  - Cobertura de aceptación ampliada: rollback total de Fase C ante
    fallo de transición o de auditoría, concurrencia real de
    reclamaciones y de creación de versión con `idempotency_key`
    distintas.
  - Build v3: 34 tests unitarios ejecutados realmente (`34 passed`); 78
    tests de integración recolectados sin errores de import — no
    ejecutados contra PostgreSQL real en este entorno (sin Docker).
    Ningún cambio de esquema respecto a `0004`.
