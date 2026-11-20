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
package org.gbif.orchestrator.ws.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.springframework.mock.web.MockMultipartFile;

/** Small archives for submissions. */
public final class Archives {

  private Archives() {}

  public static MockMultipartFile dwcDp(String name) throws IOException {
    Map<String, String> files = new LinkedHashMap<>();
    files.put("datapackage.json", "{\"profile\":\"dwc-dp\",\"resources\":[{\"name\":\"occurrence\"}]}");
    files.put("occurrence.csv", "occurrenceID,scientificName\n1,Puma concolor\n");
    return zip(name, files);
  }

  /** A DwC-DP as published: descriptor at the root, the tables in a subfolder. */
  public static MockMultipartFile dwcDpWithDataFolder(String name) throws IOException {
    Map<String, String> files = new LinkedHashMap<>();
    files.put(
        "datapackage.json",
        "{\"profile\":\"dwc-dp\",\"resources\":[{\"name\":\"occurrence\",\"path\":\"data/occurrence.csv\"}]}");
    files.put("data/occurrence.csv", "occurrenceID,scientificName\n1,Puma concolor\n");
    files.put("eml.xml", "<eml:eml xmlns:eml=\"https://eml.ecoinformatics.org/eml-2.2.0\"/>");
    return zip(name, files);
  }

  /** A DwC-A with its data file in a subfolder, which the store flattens (validator-ws behaviour). */
  public static MockMultipartFile dwcaWithDataFolder(String name) throws IOException {
    Map<String, String> files = new LinkedHashMap<>();
    files.put(
        "meta.xml",
        "<archive xmlns=\"http://rs.tdwg.org/dwc/text/\"><core rowType="
            + "\"http://rs.tdwg.org/dwc/terms/Occurrence\"><files><location>occurrence.txt"
            + "</location></files><id index=\"0\"/></core></archive>");
    files.put("data/occurrence.txt", "occurrenceID\tscientificName\n1\tPuma concolor\n");
    return zip(name, files);
  }

  public static MockMultipartFile dwca(String name) throws IOException {
    Map<String, String> files = new LinkedHashMap<>();
    files.put(
        "meta.xml",
        "<archive xmlns=\"http://rs.tdwg.org/dwc/text/\" metadata=\"eml.xml\"><core rowType="
            + "\"http://rs.tdwg.org/dwc/terms/Occurrence\"><files><location>occurrence.txt"
            + "</location></files><id index=\"0\"/></core></archive>");
    files.put("occurrence.txt", "occurrenceID\tscientificName\n1\tPuma concolor\n");
    files.put(
        "eml.xml",
        "<eml:eml xmlns:eml=\"eml://ecoinformatics.org/eml-2.1.1\" packageId=\"p\" system=\"s\">"
            + "<dataset><title>Cats of the example valley</title></dataset></eml:eml>");
    return zip(name, files);
  }

  /** A zip of the given entries (name → content), in order. */
  public static MockMultipartFile zip(String name, Map<String, String> files) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
      for (Map.Entry<String, String> file : files.entrySet()) {
        zip.putNextEntry(new ZipEntry(file.getKey()));
        zip.write(file.getValue().getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
      }
    }
    return new MockMultipartFile("file", name, "application/zip", bytes.toByteArray());
  }
}
