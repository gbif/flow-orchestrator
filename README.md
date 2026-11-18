# flow-orchestrator

Workflow orchestration service for GBIF validation and ingestion workflows.

The orchestrator owns workflow definitions, execution state and step transitions. It dispatches
work to isolated processing workers over RabbitMQ and interprets their completion messages.
Workers do not manage workflow state.

This repository replaces the former `validator-orchestrator` module in the pipelines repository.
The redesign is tracked in [docs/architecture.md](docs/architecture.md).

**Status:** Phase A, round 5. Engine, message contracts, persistence, START and step-completion
handling, reports, RabbitMQ messaging, per-step requests, report interpretation (rejections) and
the DwC-DP analyser adapter are in place; the pipelines DwC-DP validator jobs reply to the
orchestrator. Retry, rerun and recovery come in round 6. A local environment is in
[local-test/USAGE.md](local-test/USAGE.md).

## Modules

| Module | Purpose |
|---|---|
| `flow-orchestrator-api` | Message contracts as plain records (commands, step completions, workflow events, report types). Jackson only; gbif-api `provided`. |
| `flow-orchestrator-engine` | Pure Java workflow engine: DAG definitions, join policies, transition decisions. No Spring, DB or messaging. |
| `flow-orchestrator-app` | Spring Boot service: configuration, persistence, command and completion handling, messaging (postal), Actuator. |

## Build

Requires Java 17 and Maven. Until postal-service 2.1.16 is released, install its snapshot first
(`mvn install` in the postal-service repository); it adds the publisher-confirm and dead-letter
builders the orchestrator uses.

```bash
mvn clean verify         # build and run all tests
mvn spotless:apply       # format (CI runs spotless:check)
```

The runnable artifact is `flow-orchestrator-app/target/flow-orchestrator-app-<version>-shaded.jar`.

It needs the registry PostgreSQL database (with registry changesets 189/190 applied by hand),
RabbitMQ, an instance domain and a messaging prefix:

```bash
DB_URL=jdbc:postgresql://localhost:5432/registry DB_USERNAME=orchestrator DB_PASSWORD=… \
ORCHESTRATOR_DOMAIN=VALIDATION ORCHESTRATOR_MESSAGING_PREFIX=validation \
RABBIT_HOST=localhost RABBIT_USERNAME=guest RABBIT_PASSWORD=guest \
  java -jar flow-orchestrator-app/target/flow-orchestrator-app-*-shaded.jar
curl localhost:8080/actuator/health
```

## Docker image

```bash
mvn clean package -DskipTests
build/docker-image.sh docker.gbif.org/flow-orchestrator:<version>
```

CI (`Jenkinsfile`) publishes `docker.gbif.org/flow-orchestrator:<version>`.

## Configuration

See `flow-orchestrator-app/src/main/resources/application.yml`.

| Setting | Meaning |
|---|---|
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` | Registry PostgreSQL database. |
| `orchestrator.domain` (`ORCHESTRATOR_DOMAIN`) | The single domain this instance processes: `VALIDATION` or `INGESTION`. Required. |
| `orchestrator.workflows.<name>` | Workflow definitions: `domain`, `match` (command parameters that select it), `nodes` as a list of `{step, dependencies, join-policy}`. |
| `orchestrator.steps.<StepType>.request` | `exchange` and `routing-key` for each step request. |
| `RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USERNAME`, `RABBIT_PASSWORD`, `RABBIT_VHOST` | RabbitMQ connection (`orchestrator.rabbitmq.*`). |
| `ORCHESTRATOR_MESSAGING_PREFIX` | Per-instance prefix of queue and routing-key names, for example `validation`. Required (also names the `WorkflowFinished` routing key, which is written to the outbox even without a broker). |
| `ORCHESTRATOR_MESSAGING_ENABLED` | `false` runs without RabbitMQ (nothing is consumed or published). Default `true`. |
| `orchestrator.messaging.*` | Dead-letter exchange, the `commands` and `completions` queues (exchange, routing key, queue, threads) and the `workflow-finished` destination. Defaults in `application.yml`. |
| `orchestrator.relay.*` | Outbox relay: `poll-interval`, `batch-size`, `confirm-timeout`, `max-backoff`. |

Invalid workflow or messaging configuration (cycles, unknown dependencies, identical matches, steps
without routing, missing or empty-prefixed queue names) stops the application at startup. See [docs/architecture.md](docs/architecture.md) §5.
