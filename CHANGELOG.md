# Changelog

## 0.0.1 (unreleased)

- Multi-module repository: `flow-orchestrator-engine` and `flow-orchestrator-app`.
- Ported the DAG workflow engine from pipelines `validator-orchestrator` (commit `dffcecb0b`).
  - Removed `SequentialWorkflowEngine`; use `DagDefinition.linear(...)`. On failure, downstream
    steps are now `ABORTED`.
  - Renamed `SET_VALIDATION_STATUS` to `SET_WORKFLOW_STATUS`.
- Spring Boot service skeleton with Actuator probes, Prometheus metrics, and DAG validation at startup.
- Docker image and Jenkins pipeline.
- New `flow-orchestrator-api` module with the message contracts: `WorkflowCommand`, `StepHeader`,
  `StepCompleted` (outcome, outputs, reports), `WorkflowFinished`, `ReportTypes` and the
  `OrchestratorJson` helper. Golden JSON fixtures lock the wire format.
