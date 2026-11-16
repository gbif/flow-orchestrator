# flow-orchestrator

Workflow orchestration service for GBIF validation and ingestion workflows.

The orchestrator owns workflow definitions, execution state and step transitions. It dispatches
work to isolated processing workers over RabbitMQ and interprets their completion messages.
Workers do not manage workflow state.

This repository replaces the former `validator-orchestrator` module in the pipelines repository.
The redesign is tracked in [docs/architecture.md](docs/architecture.md).

**Status:** Phase A, round 2. The engine, the message contracts and a skeleton service are in place. Persistence,
messaging and step integrations are not ported yet.

## Modules

| Module | Purpose |
|---|---|
| `flow-orchestrator-api` | Message contracts as plain records (commands, step completions, workflow events, report types). Jackson only; gbif-api `provided`. |
| `flow-orchestrator-engine` | Pure Java workflow engine: DAG definitions, join policies, transition decisions. No Spring, DB or messaging. |
| `flow-orchestrator-app` | Spring Boot service: configuration, Actuator, and later persistence, messaging and step integrations. |

## Build

Requires Java 17 and Maven.

```bash
mvn clean verify         # build and run all tests
mvn spotless:apply       # format (CI runs spotless:check)
```

The runnable artifact is `flow-orchestrator-app/target/flow-orchestrator-app-<version>-shaded.jar`.

```bash
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

See `flow-orchestrator-app/src/main/resources/application.yml`. The workflow DAG is configured
under `orchestrator.workflow`. Node keys are gbif-api `StepType` names, and invalid DAGs fail at
startup.
