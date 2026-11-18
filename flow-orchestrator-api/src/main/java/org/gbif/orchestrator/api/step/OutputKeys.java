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
 * Names of {@link StepCompleted#outputs()} values that the orchestrator reads to build later
 * requests. Values are strings; numbers are written in decimal.
 */
public final class OutputKeys {

  /**
   * What a conversion step produced, as a gbif-api {@code DatasetType} name ({@code OCCURRENCE} or
   * {@code SAMPLING_EVENT}). Written by VALIDATOR_DWCDP_TO_VERBATIM.
   */
  public static final String DATASET_TYPE = "datasetType";

  /** Number of occurrence records produced. Written by VALIDATOR_DWCDP_TO_VERBATIM. */
  public static final String OCCURRENCE_RECORDS = "occurrenceRecords";

  /** Number of event records produced. Written by VALIDATOR_DWCDP_TO_VERBATIM. */
  public static final String EVENT_RECORDS = "eventRecords";

  private OutputKeys() {}
}
