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
package org.gbif.orchestrator.ws.file;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveCoreTypeTest {

  @TempDir Path archive;

  private void meta(String xml) throws Exception {
    Files.writeString(archive.resolve("meta.xml"), xml);
  }

  @Test
  void taxonCoreIsAChecklist() throws Exception {
    meta(
        "<archive xmlns=\"http://rs.tdwg.org/dwc/text/\"><core rowType="
            + "\"http://rs.tdwg.org/dwc/terms/Taxon\"/></archive>");
    assertTrue(ArchiveCoreType.isChecklist(archive));
  }

  @Test
  void occurrenceAndEventCoresAreNot() throws Exception {
    meta("<archive><core rowType=\"http://rs.tdwg.org/dwc/terms/Occurrence\"/></archive>");
    assertFalse(ArchiveCoreType.isChecklist(archive));
    meta("<archive><core rowType=\"http://rs.tdwg.org/dwc/terms/Event\"/></archive>");
    assertFalse(ArchiveCoreType.isChecklist(archive));
  }

  @Test
  void withoutMetaXmlItIsNot() {
    assertFalse(ArchiveCoreType.isChecklist(archive));
  }

  @Test
  void aDoctypeIsRefusedNotFollowed() throws Exception {
    meta(
        "<?xml version=\"1.0\"?><!DOCTYPE archive [<!ENTITY t SYSTEM \"file:///etc/passwd\">]>"
            + "<archive><core rowType=\"&t;\"/></archive>");
    assertFalse(ArchiveCoreType.isChecklist(archive));
  }

  @Test
  void prefixedAndBareRowTypesAreRecognised() {
    assertTrue(ArchiveCoreType.isTaxon("dwc:Taxon"));
    assertTrue(ArchiveCoreType.isTaxon("Taxon"));
    assertFalse(ArchiveCoreType.isTaxon("http://rs.tdwg.org/dwc/terms/MeasurementOrFact"));
    assertFalse(ArchiveCoreType.isTaxon(null));
  }
}
