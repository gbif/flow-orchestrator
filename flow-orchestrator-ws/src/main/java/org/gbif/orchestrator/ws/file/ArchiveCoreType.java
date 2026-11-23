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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import lombok.extern.slf4j.Slf4j;

/**
 * Whether an unpacked DwC-A is a checklist: its {@code meta.xml} core row type is {@code Taxon}.
 * This chooses the validation workflow (a checklist waits for ChecklistBank), as the legacy archive
 * validator chose its validator from the core row type. An archive without a readable {@code
 * meta.xml} is not a checklist, also as before.
 */
@Slf4j
public final class ArchiveCoreType {

  private static final String META_XML = "meta.xml";

  private ArchiveCoreType() {}

  public static boolean isChecklist(Path archiveFolder) {
    Path meta = archiveFolder.resolve(META_XML);
    if (!Files.isRegularFile(meta)) {
      return false;
    }
    try (InputStream in = Files.newInputStream(meta)) {
      Document document = parser().newDocumentBuilder().parse(in);
      NodeList cores = document.getElementsByTagNameNS("*", "core");
      if (cores.getLength() == 0) {
        return false;
      }
      return isTaxon(((Element) cores.item(0)).getAttribute("rowType"));
    } catch (Exception e) {
      log.info("Unreadable {} in {}, not treated as a checklist: {}", META_XML, archiveFolder, e.getMessage());
      return false;
    }
  }

  /** {@code http://rs.tdwg.org/dwc/terms/Taxon}, also prefixed ({@code dwc:Taxon}) or bare. */
  static boolean isTaxon(String rowType) {
    String type = rowType == null ? "" : rowType.trim().toLowerCase(Locale.ROOT);
    return type.equals("taxon") || type.endsWith("/taxon") || type.endsWith(":taxon");
  }

  /** Namespace aware, without DTDs or external entities: the file comes from the uploader. */
  private static DocumentBuilderFactory parser() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    factory.setExpandEntityReferences(false);
    factory.setXIncludeAware(false);
    return factory;
  }
}
