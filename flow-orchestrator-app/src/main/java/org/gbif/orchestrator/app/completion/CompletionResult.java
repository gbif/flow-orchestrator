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
package org.gbif.orchestrator.app.completion;

/** What happened to a step completion. */
public enum CompletionResult {
  /** The completion was applied and the workflow advanced. */
  PROCESSED,
  /**
   * The completion was for an attempt that is no longer current or already ended (a redelivery or
   * a late worker); nothing changed.
   */
  IGNORED
}
