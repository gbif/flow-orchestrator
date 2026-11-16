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
package org.gbif.orchestrator.api.report;

/**
 * Known report types. Report types are plain strings so that a new type does not require every
 * consumer to upgrade this library.
 */
public final class ReportTypes {

  /** DwC-DP analysis: validity and per-resource results (dwc-dp-analyser). */
  public static final String DWCDP_ANALYSIS = "DWCDP_ANALYSIS";

  /** Archive checks: EML issues, core issues, term counts (archive validation step). */
  public static final String ARCHIVE_VALIDATION = "ARCHIVE_VALIDATION";

  /** ChecklistBank results and import status (checklist validation). */
  public static final String CHECKLIST = "CHECKLIST";

  /** DwC-DP to verbatim conversion: compact, detailed and statistics reports. */
  public static final String DWCDP_CONVERSION = "DWCDP_CONVERSION";

  /** Interpreted metrics: indexed counts, term counts, samples, issues, indexeable verdict. */
  public static final String INTERPRETATION_METRICS = "INTERPRETATION_METRICS";

  /** Combined view built by the orchestrator from the partial reports when a workflow ends. */
  public static final String SUMMARY = "SUMMARY";

  private ReportTypes() {}
}
