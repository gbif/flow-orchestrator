/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.orchestrator.api.command;

/** What a {@link WorkflowCommand} asks the orchestrator to do. */
public enum CommandType {
  /** Start a new workflow execution. Requires {@link WorkflowCommand#domain()}. */
  START,
  /** Retry a failed step with a new attempt. Requires {@link WorkflowCommand#step()}. */
  RETRY_STEP,
  /** Rerun a step and everything downstream of it. Requires {@link WorkflowCommand#step()}. */
  RERUN_FROM,
  /** Start the whole workflow again as a new execution. */
  RESTART,
  /**
   * End the latest run of a running workflow: unfinished steps and the workflow become {@code
   * ABORTED}. Requires {@link WorkflowCommand#domain()}. Workers are not stopped; their late
   * completions are ignored.
   */
  CANCEL
}
