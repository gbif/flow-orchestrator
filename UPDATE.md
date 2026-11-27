# Orchestrator status update

*As of 2026-11-27.*

The validator-orchestrator in pipelines has become **flow-orchestrator**, a standalone workflow
engine with its own repository. The old validator-ws is now `flow-orchestrator-ws`, a module of
the same repository. Every validation format that validator-ws supported now runs as an
orchestrated workflow. That code is written and has been tested on dev; the cut-over decision
(below) is still open.

Further reading: [`docs/overview.md`](docs/overview.md) is a short summary for people. [`docs/architecture.md`](docs/architecture.md) has the design, decisions and
round plans. [`docs/deployment.md`](docs/deployment.md) is the deployment checklist.

## Where we are

| Phase / round | Scope | Status |
|---|---|---|
| Rounds 1–4 | Engine port, message contracts (`flow-orchestrator-api`), persistence (`orchestrator` schema), messaging (outbox, listeners, dead-letter queues) | Done |
| Round 5 | DwC-DP validation chain end to end: analyser adapter, report interpreters, pipelines DwC-DP validator jobs in orchestrator mode | Done |
| Round A | validator-ws moved into `flow-orchestrator-ws`; DwC-DP validations through the engine; validator-ws data migration | Done, tested on minikube and dev |
| Round B (B1–B5) | DwC-A, Taxon-core DwC-A, tabular, spreadsheet and ColDP through the engine; ChecklistBank as an explicit step (`VALIDATOR_CHECKLISTBANK`); shared EML validation (`gbif-eml-validation`) | Done. Tested on dev with DwC-DP and DwC-A packages. Checklist, ColDP, tabular and spreadsheet still to be confirmed with the smoke tests in `docs/deployment.md` §9 |
| Round B6 | Remove the legacy validator-ws reporting and modules from pipelines | After dev has run the new path for every format |
| Next | Retry, rerun and recovery (`docs/architecture.md` §7) | Proposed |
| Phase C | Crawl and ingestion through the orchestrator (§9a) | Direction agreed, not started |

### Open decision: where flow-orchestrator-ws runs

ChecklistBank has to reach the ws's callback (`POST /validation/clbValidationCallback/{key}`), and
the frontend reaches the API through the gateway (`/v1/validation*`). There are two options:

1. **Kubernetes**, with the engine and the analyser. The chart now has an optional Ingress
   (`ingress.enabled`, host `flow-orchestrator-ws.<domain>`). It raises nginx's request body
   limit to 1 GB for uploads. Varnish needs a rule that sends the traffic to `kube.backend`. For
   `/v1/validation*` on the API host, that rule also has to strip `/v1` and set the Host header
   to the Ingress host.
2. **The VM where validator-ws ran.** Varnish routing, the callback URL and the frontend stay
   unchanged. It needs Java 17, the `flow-orchestrator-ws-<version>-shaded.jar` (the executable
   jar), a new `application.yml` (the structure differs from validator-ws's), and the shared
   validation store mounted on the VM. The workers and the analyser read that store.

Either way, the ws must write to the **same** validation store that the pipelines workers and the
validation analyser read.

## Repositories and branches

All branches are pushed and nothing is waiting to be pulled. Feature branches get
`<version>-FEATURE-ORCHESTRATION-SNAPSHOT` builds from Jenkins.

| Repository | Branch | Role in the orchestrator work | Version consumed |
|---|---|---|---|
| [gbif/flow-orchestrator](https://github.com/gbif/flow-orchestrator) | `dev` | The engine (`flow-orchestrator-app`), the contracts (`flow-orchestrator-api`), the ws (`flow-orchestrator-ws`), the `orchestrator` schema (`db/`), Helm charts, minikube local test | images `flow-orchestrator`, `flow-orchestrator-ws` from `dev` |
| [gbif/pipelines](https://github.com/gbif/pipelines) | `feature/orchestration` | Validator workers in orchestrator mode (archive validator, DwC-A to verbatim, checklist validator, Spark validator steps), new `sdks/orchestrator-client`, balancer keeps the step header. The old validator-orchestrator was removed. 42 commits ahead of `dev` | `4.0.55-FEATURE-ORCHESTRATION-SNAPSHOT` |
| [gbif/gbif-api](https://github.com/gbif/gbif-api) | `feature/orchestration` | `StepType` additions (DwC-DP validation steps, `VALIDATOR_CHECKLISTBANK`) | `2.4.30-FEATURE-ORCHESTRATION-SNAPSHOT` |
| [gbif/postal-service](https://github.com/gbif/postal-service) | `feature/orchestration` | Message builders, the step header on pipelines messages | `2.1.16-FEATURE-ORCHESTRATION-SNAPSHOT` |
| [gbif/crawler](https://github.com/gbif/crawler) | `feature/orchestration` | Dependency bump for the feature builds, CI for feature versions, DwC-DP metasync fix | feature snapshot |
| [gbif/dwc-dp-analyser-service](https://github.com/gbif/dwc-dp-analyser-service) | `dev` | Orchestration compatibility (binds its input queue to the `orchestrator` exchange), analyser 0.0.17 | image `0.0.9-SNAPSHOT` |
| [gbif/dwc-dp-analyser](https://github.com/gbif/dwc-dp-analyser) | `dev` | EML validation through `gbif-eml-validation` | release `0.0.17` |
| [gbif/gbif-eml-validation](https://github.com/gbif/gbif-eml-validation) | `dev` | Shared EML validation, moved out of pipelines; used by the archive validator and the analyser | release `1.0.0` |
| [gbif/registry](https://github.com/gbif/registry) | `dev` | No orchestrator changes. Validation no longer goes through registry, and validator-ws's tables moved to the `orchestrator` schema | — |

The pipelines work started from `fix/validator-ownership-and-upload` (the validator-ws security
fixes, gbif/security-issues#46). Those fixes are carried into `flow-orchestrator-ws`.

**Release order** when the feature branches are merged and released: gbif-api, postal-service,
flow-orchestrator-api → pipelines, dwc-dp-analyser-service → flow-orchestrator-ws and the
engine.

## Deployment state (dev)

- The `orchestrator` schema is applied by hand (`db/001`–`004`), and the validator-ws data copy is
  described in `db/migration/validator-ws-data.md`. Schema changes are never automatic.
- The engine (`helm/flow-orchestrator`) runs as one release per domain; for validation it uses
  `domain: VALIDATION` and `messagingPrefix: validation`.
- The validation analyser is its own deployment, separate from the crawler's.
- The pipelines validator workers need `--orchestrator-completion-routing-key
  validation.step.completed`. The archive validator's `--clb-callback-url` must point at the ws,
  so it depends on the open decision above.
- Environment values (hosts, secrets) live in the private Helm configuration repository. This
  repository only has defaults, local values and `values-dev.yaml.example`.

## Next steps

1. Decide Kubernetes or VM for flow-orchestrator-ws, then set up the routing for the callback and
   the frontend.
2. Run the remaining smoke tests on dev: Taxon-core DwC-A, ColDP, tabular, spreadsheet, cancel.
3. Round B6: remove the legacy validator-ws code from pipelines.
4. Merge and release the `feature/orchestration` branches in the release order above. Then deploy
   to the next environment with the same checklist.
5. Retry, rerun and recovery, then Phase C (ingestion).
