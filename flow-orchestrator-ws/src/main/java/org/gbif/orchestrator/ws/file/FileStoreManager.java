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

import org.gbif.utils.file.CompressionUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.springframework.web.multipart.MultipartFile;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.orchestrator.ws.file.MediaTypeAndFormatDetector.detectMediaType;
import static org.gbif.orchestrator.ws.file.MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat;
import static org.gbif.orchestrator.ws.file.SupportedMediaTypes.COMPRESS_CONTENT_TYPE;

/**
 * Class responsible to manage files uploaded for validation. This class will unzip the file is
 * required.
 */
@Slf4j
public class FileStoreManager {

  @Data
  @Builder
  public static class AsyncDownloadResult {
    private final DataFile dataFile;
    private final CompletableFuture<DataFile> downloadTask;
  }

  @Data
  @Builder
  public static class AsyncDataFileTask {
    private final DataFile start;
    private final CompletableFuture<DataFile> task;
  }

  /** The descriptor that makes a zip a data package (DwC-DP). */
  static final String DATA_PACKAGE_DESCRIPTOR = "datapackage.json";

  private final Path storePath;

  private final DownloadFileManager downloadFileManager;

  public FileStoreManager(
      String workingDirectory, String storeDirectory, DownloadFileManager downloadFileManager)
      throws IOException {
    Path workingDirectory1 = Paths.get(workingDirectory);
    this.storePath = Paths.get(storeDirectory);
    this.downloadFileManager = downloadFileManager;
    createIfNotExists(workingDirectory1);
    createIfNotExists(storePath);
  }

  public CompletableFuture<DataFile> extractAndGetFileInfoAsync(
      Path dataFilePath, Path destinationFolder, String fileName) {
    return CompletableFuture.supplyAsync(
        () -> extractAndGetFileInfo(dataFilePath, destinationFolder, fileName));
  }

  @SneakyThrows
  public DataFile extractAndGetFileInfo(
      Path dataFilePath, Path destinationFolder, String fileName) {
    try {

      // check if we have something to unzip
      String detectedMediaType = detectMediaType(dataFilePath);
      if (COMPRESS_CONTENT_TYPE.contains(detectedMediaType)) {
        Optional<String> dataPackageRoot = dataPackageRoot(dataFilePath);
        if (dataPackageRoot.isPresent()) {
          unpackDataPackage(dataFilePath, dataPackageRoot.get(), destinationFolder);
        } else {
          // Other formats: flattened, as validator-ws has always done.
          CompressionUtil.decompressFile(destinationFolder.toFile(), dataFilePath.toFile());
        }
      }

      // from here we can decide to change the content type (e.g. zipped excel file)
      return fromMediaTypeAndFormat(dataFilePath, fileName, detectedMediaType, destinationFolder);
    } catch (Exception ex) {
      log.error("Deleting temporary content of {} after IOException.", fileName, ex);
      FileUtils.deleteDirectory(destinationFolder.toFile());
      throw ex;
    }
  }

  private DataFile fromMediaTypeAndFormat(
      Path dataFilePath, String fileName, String detectedMediaType, Path finalPath)
      throws UnsupportedMediaTypeException, IOException {
    return evaluateMediaTypeAndFormat(finalPath, detectedMediaType)
        .map(
            mtf ->
                DataFile.create(
                    dataFilePath,
                    fileName,
                    mtf.getFileFormat(),
                    detectedMediaType,
                    mtf.getMediaType()))
        .orElseThrow(
            () -> new UnsupportedMediaTypeException("Unsupported file type: " + detectedMediaType));
  }

  @SneakyThrows
  public AsyncDataFileTask uploadDataFile(MultipartFile multipartFile, String targetDirectory) {
    String fileName = sanitizeFileName(multipartFile.getOriginalFilename());
    Path destinationFolder = getDestinationPath(targetDirectory);
    Path dataFilePath = destinationFolder.resolve(fileName);

    createIfNotExists(destinationFolder);

    // copy the file
    multipartFile.transferTo(dataFilePath.toFile());

    // check if we have something to unzip
    return AsyncDataFileTask.builder()
        .start(DataFile.builder().sourceFileName(fileName).filePath(dataFilePath).build())
        .task(extractAndGetFileInfoAsync(dataFilePath, destinationFolder, fileName))
        .build();
  }

  /**
   * Starts downloading and extracting a file. The returned task completes with the extracted file
   * info, or exceptionally if the download or the extraction fails.
   */
  @SneakyThrows
  public AsyncDownloadResult downloadDataFile(String url, String targetDirectory)
      throws IOException {

    // also false for URLs the validator doesn't fetch, see DownloadFileManager
    if (!downloadFileManager.isAvailable(url)) {
      throw new IllegalArgumentException(
          "Failed to download file from "
              + url
              + ". The resource is not reachable. Please check that the URL is correct");
    }

    // the file name is taken from the redirected URL, if there are redirects
    String finalUrl = downloadFileManager.getFinalUrl(url);

    // extract the file name from http headers if available
    String headerFileName = getFileNameFromContentDisposition(finalUrl);
    final String fileName = headerFileName != null ? headerFileName : getFileName(finalUrl);
    Path destinationFolder = getDestinationPath(targetDirectory);
    createIfNotExists(destinationFolder);
    Path dataFilePath = getDestinationPath(targetDirectory).resolve(fileName);

    return AsyncDownloadResult.builder()
        .dataFile(DataFile.builder().sourceFileName(fileName).filePath(dataFilePath).build())
        .downloadTask(
            downloadFileManager
                .downloadAsync(finalUrl, dataFilePath)
                .thenApply(
                    file -> extractAndGetFileInfo(dataFilePath, destinationFolder, fileName)))
        .build();
  }

  public String getFileNameFromContentDisposition(String url) throws IOException {
    return extractFileName(downloadFileManager.getHeader(url, "Content-Disposition"));
  }

  private String extractFileName(String contentDisposition) {
    if (contentDisposition == null) {
      return null;
    }

    Matcher matcher = Pattern.compile("filename=\"?([^\";]+)\"?").matcher(contentDisposition);

    return matcher.find() ? Paths.get(matcher.group(1)).getFileName().toString() : null;
  }

  @SneakyThrows
  public void deleteIfExist(String path) {
    File file = getDestinationPath(path).toFile();
    if (file.exists()) {
      FileUtils.deleteDirectory(file);
    }
  }

  /**
   * For a zip holding a data package (DwC-DP): the folder of its {@code datapackage.json}, as an
   * entry-name prefix ({@code ""} at the root, {@code "pumas/"} when the package was zipped with a
   * top folder). The shallowest descriptor wins. Empty for anything else, including a file that is
   * not a readable zip (a tarball, for example).
   */
  static Optional<String> dataPackageRoot(Path file) {
    try (ZipFile zip = new ZipFile(file.toFile())) {
      return zip.stream()
          .filter(e -> !e.isDirectory())
          .map(e -> entryName(e))
          .filter(name -> !isIgnoredEntry(name))
          .filter(name -> baseName(name).equalsIgnoreCase(DATA_PACKAGE_DESCRIPTOR))
          .min(Comparator.comparingInt((String name) -> name.split("/").length))
          .map(name -> name.substring(0, name.length() - baseName(name).length()));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  /**
   * Unpacks a data package with its folders, the descriptor's folder becoming {@code target}, as
   * the crawler does: the descriptor references its resources by relative path ({@code
   * data/event.parquet}), and the analyser and pipelines resolve them from there. A top folder the
   * package was zipped in is dropped; entries outside the descriptor's folder, {@code __MACOSX}
   * and hidden files are skipped. An entry that would land outside {@code target} fails the
   * upload.
   */
  static void unpackDataPackage(Path zipFile, String root, Path target) throws IOException {
    Path base = target.toAbsolutePath().normalize();
    int skipped = 0;
    try (ZipFile zip = new ZipFile(zipFile.toFile())) {
      for (ZipEntry entry : Collections.list(zip.entries())) {
        String name = entryName(entry);
        if (!name.startsWith(root) || isIgnoredEntry(name)) {
          skipped++;
          continue;
        }
        String relative = name.substring(root.length());
        if (relative.isEmpty()) {
          continue;
        }
        Path out = base.resolve(relative).normalize();
        if (!out.startsWith(base)) {
          throw new IOException("Zip entry outside the package folder: " + entry.getName());
        }
        if (out.equals(base)) {
          continue;
        }
        if (entry.isDirectory()) {
          Files.createDirectories(out);
          continue;
        }
        Files.createDirectories(out.getParent());
        try (InputStream in = zip.getInputStream(entry)) {
          Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
    log.info(
        "Unpacked data package {} from folder '{}' of the zip ({} entries skipped)",
        zipFile.getFileName(),
        root,
        skipped);
  }

  private static String entryName(ZipEntry entry) {
    return entry.getName().replace('\\', '/');
  }

  private static String baseName(String entryName) {
    return entryName.substring(entryName.lastIndexOf('/') + 1);
  }

  /** {@code __MACOSX} resource forks and hidden files or folders, as gbif-common ignores them. */
  private static boolean isIgnoredEntry(String entryName) {
    for (String segment : entryName.split("/")) {
      boolean hidden = segment.startsWith(".") && !segment.equals(".") && !segment.equals("..");
      if (segment.equals("__MACOSX") || hidden) {
        return true;
      }
    }
    return false;
  }

  /**
   * Strips any directory, '/' or '\' separated, from a client supplied file name, so the file can't
   * be written outside its target directory.
   */
  static String sanitizeFileName(String fileName) {
    String name = FilenameUtils.getName(fileName);
    if (name == null || name.isBlank() || name.equals(".") || name.equals("..")) {
      throw new IllegalArgumentException("Invalid file name: " + fileName);
    }
    return name;
  }

  @SneakyThrows
  private static String getFileName(String url) {
    return Paths.get(new URI(url).getPath()).getFileName().toString();
  }

  /** Creates the directory(path) if it doesn't exist. */
  private static void createIfNotExists(Path path) throws IOException {
    if (!path.toFile().exists()) {
      Files.createDirectories(path);
    }
  }

  /** Creates a new random path to be used when copying files. */
  private Path getDestinationPath(String destinationDirectory) {
    return storePath.resolve(destinationDirectory);
  }
}
