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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.tika.Tika;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import static org.gbif.orchestrator.ws.file.SupportedMediaTypes.COMPRESS_CONTENT_TYPE;
import static org.gbif.orchestrator.ws.file.SupportedMediaTypes.SPREADSHEET_CONTENT_TYPES;
import static org.gbif.orchestrator.ws.file.SupportedMediaTypes.TABULAR_CONTENT_TYPES;

/**
 * Utility class to: - automatically detect the media types based on file or bytes. - decide
 * org.gbif.validation.api.vocabulary.FileFormat based on media type
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MediaTypeAndFormatDetector {

  private static final Tika TIKA = new Tika();
  private static final Pattern COLDP_REQUIRED_FILE =
      Pattern.compile("^name(?:[-_]*usage)?[-_]*\\.(csv|tsv|tab|txt)$", Pattern.CASE_INSENSITIVE);

  /**
   * @return detected media type
   * @see org.apache.tika.detect.Detector
   */
  public static String detectMediaType(Path filePath) throws IOException {
    return TIKA.detect(filePath);
  }

  /**
   * Detected media type from the an {@link InputStream} and a filename. "Firstly, magic based
   * detection is used on the start of the file ... if available, the filename is then used to
   * improve the detail of the detection" Source: https://tika.apache.org/1.1/detection.html
   *
   * @param filename filename including the extension. Optional but used to improve the detail of
   *     the detection.
   * @see org.apache.tika.detect.Detector
   */
  public static String detectMediaType(InputStream is, @Nullable String filename)
      throws IOException {
    return TIKA.detect(is, filename);
  }

  /**
   * Given a {@link Path} to a file (or folder) and a original contentType this function will check
   * to reevaluate the contentType and return the matching {@link FileFormat}. If a more specific
   * contentType can not be found the original one will be return with the matching {@link
   * FileFormat}.
   *
   * @param dataFilePath shall point to data file or folder (not a zip file)
   */
  public static Optional<MediaTypeAndFormat> evaluateMediaTypeAndFormat(
      Path dataFilePath, String detectedContentType) throws IOException {
    Objects.requireNonNull(dataFilePath, "dataFilePath must be provided");
    String contentType = detectedContentType;

    if (COMPRESS_CONTENT_TYPE.contains(detectedContentType)) {
      List<Path> content;
      try (Stream<Path> list = Files.list(dataFilePath)) {
        content = list.toList();
      }
      if (content.size() == 1) {
        contentType = MediaTypeAndFormatDetector.detectMediaType(content.get(0));
      } else {
        if (content.stream().anyMatch(c -> isNameColDPFile(c.getFileName().toString()))) {
          return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.COLDP));
        } else if (content.stream().anyMatch(MediaTypeAndFormatDetector::isDwCDatapackage)) {
          return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.DWCDP));
        } else {
          return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.DWCA));
        }
      }
    }

    if (TABULAR_CONTENT_TYPES.contains(contentType)) {
      return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.TABULAR));
    }

    if (SPREADSHEET_CONTENT_TYPES.contains(contentType)) {
      return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.SPREADSHEET));
    }
    return Optional.empty();
  }

  private static boolean isDwCDatapackage(Path c) {
    return c.getFileName().toString().toLowerCase(Locale.ROOT).endsWith("datapackage.json");
  }

  static boolean isNameColDPFile(String filName) {
    return COLDP_REQUIRED_FILE.matcher(filName).matches();
  }

  /** Simple holder for mediaType and fileFormat */
  @Getter
  @AllArgsConstructor(staticName = "create")
  public static class MediaTypeAndFormat {
    private final String mediaType;
    private final FileFormat fileFormat;
  }
}
