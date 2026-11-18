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
package org.gbif.orchestrator.api.step;

/**
 * What happened to the work of a step attempt. It says nothing about what runs next, and nothing
 * about whether the data is acceptable: a step that ran correctly on invalid data {@link
 * #SUCCEEDED}, and its reports say what is wrong. The orchestrator interprets the reports and
 * decides (for example to reject the workflow).
 */
public enum Outcome {
  /** The work completed. */
  SUCCEEDED,
  /** The worker could not do the work (exception, infrastructure problem). Retrying may help. */
  FAILED
}
