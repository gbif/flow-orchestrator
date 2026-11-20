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

import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.support.Archives;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** How uploads are unpacked into the shared store, which the workers read. */
class FileStoreManagerTest {

  private static final String DESCRIPTOR =
      "{\"profile\":\"dwc-dp\",\"resources\":[{\"name\":\"occurrence\",\"path\":\"data/occurrence.csv\"}]}";
  private static final String ROWS = "occurrenceID,scientificName\n1,Puma concolor\n";

  @TempDir Path tmp;

  private DataFile upload(MockMultipartFile file, String key) throws Exception {
    FileStoreManager store =
        new FileStoreManager(tmp.resolve("work").toString(), tmp.resolve("store").toString(), null);
    return store.uploadDataFile(file, key).getTask().get(30, TimeUnit.SECONDS);
  }

  private static MockMultipartFile zip(String... nameThenContent) throws Exception {
    Map<String, String> files = new LinkedHashMap<>();
    for (int i = 0; i < nameThenContent.length; i += 2) {
      files.put(nameThenContent[i], nameThenContent[i + 1]);
    }
    return Archives.zip("package.zip", files);
  }

  @Test
  void dataPackageKeepsItsFolders() throws Exception {
    DataFile file = upload(Archives.dwcDpWithDataFolder("pumas.zip"), "k1");

    Path dir = tmp.resolve("store/k1");
    assertEquals(FileFormat.DWCDP, file.getFileFormat());
    // The descriptor references data/occurrence.csv: the analyser and pipelines resolve it there.
    assertTrue(Files.isRegularFile(dir.resolve("datapackage.json")));
    assertTrue(Files.isRegularFile(dir.resolve("data/occurrence.csv")));
    assertFalse(Files.exists(dir.resolve("occurrence.csv")));
  }

  @Test
  void aTopFolderAroundThePackageIsDropped() throws Exception {
    DataFile file =
        upload(
            zip(
                "pumas/datapackage.json", DESCRIPTOR,
                "pumas/data/occurrence.csv", ROWS,
                "pumas/eml.xml", "<eml/>",
                "__MACOSX/pumas/._datapackage.json", "fork",
                "pumas/.DS_Store", "junk"),
            "k2");

    Path dir = tmp.resolve("store/k2");
    assertEquals(FileFormat.DWCDP, file.getFileFormat());
    assertTrue(Files.isRegularFile(dir.resolve("datapackage.json")));
    assertTrue(Files.isRegularFile(dir.resolve("data/occurrence.csv")));
    assertTrue(Files.isRegularFile(dir.resolve("eml.xml")));
    assertFalse(Files.exists(dir.resolve("pumas")));
    assertFalse(Files.exists(dir.resolve("__MACOSX")));
    assertFalse(Files.exists(dir.resolve(".DS_Store")));
  }

  @Test
  void theShallowestDescriptorIsThePackage() throws Exception {
    upload(
        zip(
            "pumas/examples/nested/datapackage.json", "{}",
            "pumas/datapackage.json", DESCRIPTOR,
            "pumas/data/occurrence.csv", ROWS,
            "README.txt", "outside the package"),
        "k3");

    Path dir = tmp.resolve("store/k3");
    assertTrue(Files.isRegularFile(dir.resolve("data/occurrence.csv")));
    assertTrue(Files.isRegularFile(dir.resolve("examples/nested/datapackage.json")));
    assertEquals(DESCRIPTOR, Files.readString(dir.resolve("datapackage.json")));
    // Outside the descriptor's folder: not part of the package.
    assertFalse(Files.exists(dir.resolve("README.txt")));
  }

  @Test
  void entryNamesStartingWithDotSlashWork() throws Exception {
    upload(zip("./datapackage.json", DESCRIPTOR, "./data/occurrence.csv", ROWS), "k4");

    Path dir = tmp.resolve("store/k4");
    assertTrue(Files.isRegularFile(dir.resolve("datapackage.json")));
    assertTrue(Files.isRegularFile(dir.resolve("data/occurrence.csv")));
  }

  @Test
  void anEntryEscapingThePackageFolderFailsTheUpload() throws Exception {
    MockMultipartFile evil =
        zip("datapackage.json", DESCRIPTOR, "data/../../escaped.txt", "nope");

    assertThrows(ExecutionException.class, () -> upload(evil, "k5"));
    assertFalse(Files.exists(tmp.resolve("store/escaped.txt")));
    assertFalse(Files.exists(tmp.resolve("escaped.txt")));
  }

  @Test
  void otherArchivesAreStillFlattened() throws Exception {
    DataFile file = upload(Archives.dwcaWithDataFolder("cats.zip"), "k6");

    Path dir = tmp.resolve("store/k6");
    assertEquals(FileFormat.DWCA, file.getFileFormat());
    assertTrue(Files.isRegularFile(dir.resolve("occurrence.txt")));
    assertFalse(Files.exists(dir.resolve("data")));
  }

  @Test
  void dataPackageRootIsTheDescriptorsFolder() throws Exception {
    Path wrapped = tmp.resolve("w.zip");
    Files.write(wrapped, zip("a/b/datapackage.json", "{}").getBytes());
    Path csv = Files.writeString(tmp.resolve("data.csv"), "a,b\n1,2\n");

    assertEquals(Optional.of("a/b/"), FileStoreManager.dataPackageRoot(wrapped));
    assertEquals(Optional.empty(), FileStoreManager.dataPackageRoot(csv));
  }
}
