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
package org.gbif.orchestrator.ws.api;

/** File type in the context of DarwinCore. */
public enum DwcFileType {
  META_DESCRIPTOR(false), // meta.xml
  METADATA(false), // eml.xml
  CORE(true),
  EXTENSION(true);

  final boolean dataBased;

  DwcFileType(boolean dataBased) {
    this.dataBased = dataBased;
  }

  /**
   * Indicates if a {@link DwcFileType} is data based or not. Data based means related to data as
   * opposed to related to metadata.
   */
  public boolean isDataBased() {
    return dataBased;
  }
}
