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
   * The data's shape, as a gbif-api {@code DatasetType} name ({@code OCCURRENCE}, {@code
   * SAMPLING_EVENT} or {@code CHECKLIST}). Written by VALIDATOR_VALIDATE_ARCHIVE (from the core row
   * type) and by the conversion steps VALIDATOR_DWCA_TO_VERBATIM and VALIDATOR_DWCDP_TO_VERBATIM
   * (what they produced); the latest wins.
   */
  public static final String DATASET_TYPE = "datasetType";

  /**
   * Number of occurrence records produced. Written by the conversion steps
   * VALIDATOR_DWCA_TO_VERBATIM and VALIDATOR_DWCDP_TO_VERBATIM.
   */
  public static final String OCCURRENCE_RECORDS = "occurrenceRecords";

  /**
   * Number of event records produced. Written by the conversion steps VALIDATOR_DWCA_TO_VERBATIM and
   * VALIDATOR_DWCDP_TO_VERBATIM.
   */
  public static final String EVENT_RECORDS = "eventRecords";

  /**
   * Comma-separated interpretation types for the interpretation steps (pipelines {@code
   * interpretTypes}). Written by VALIDATOR_DWCA_TO_VERBATIM; without it the orchestrator asks for
   * {@code ALL}.
   */
  public static final String INTERPRET_TYPES = "interpretTypes";

  /**
   * {@code true} or {@code false}: whether occurrence triplets may identify records. Written by
   * VALIDATOR_DWCA_TO_VERBATIM.
   */
  public static final String TRIPLET_VALID = "tripletValid";

  /**
   * {@code true} or {@code false}: whether occurrenceIDs may identify records. Written by
   * VALIDATOR_DWCA_TO_VERBATIM.
   */
  public static final String OCCURRENCE_ID_VALID = "occurrenceIdValid";

  /**
   * The ChecklistBank dataset key a checklist was submitted to. Written by
   * VALIDATOR_VALIDATE_ARCHIVE for checklists (Taxon-core DwC-A, ColDP); read by
   * VALIDATOR_CHECKLISTBANK.
   */
  public static final String CLB_DATASET_KEY = "clbDatasetKey";

  private OutputKeys() {}
}
