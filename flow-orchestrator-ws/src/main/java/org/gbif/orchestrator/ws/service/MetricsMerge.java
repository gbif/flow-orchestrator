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
package org.gbif.orchestrator.ws.service;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.orchestrator.ws.api.Metrics.FileInfo;
import org.gbif.orchestrator.ws.api.Metrics.IssueInfo;
import org.gbif.orchestrator.ws.api.Metrics.TermInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Builds a validation's {@code metrics.files} from the steps' reports as the legacy workers built
 * them in validator-ws, one merge after the other:
 *
 * <ol>
 *   <li>the archive check's and ChecklistBank's file infos, matched by file name ({@link
 *       #mergeByFileName}, pipelines {@code Validations.mergeFileInfo});
 *   <li>the generated interpretation metrics, matched by row type ({@link #mergeGenerated},
 *       pipelines {@code ValidationUtil.mergeMetrics}): indexed counts, terms (grouped and ordered
 *       by DwC term) and issues.
 * </ol>
 *
 * Ported as is, quirks included: a file whose terms the archive check didn't report gets no
 * generated terms either.
 */
final class MetricsMerge {

  private static final TermFactory TERMS = TermFactory.instance();

  private MetricsMerge() {}

  /** Adds {@code fileInfo} to {@code files}, merging it into the file of the same name. */
  static List<FileInfo> mergeByFileName(List<FileInfo> files, FileInfo fileInfo) {
    List<FileInfo> result = new ArrayList<>(files);
    if (fileInfo == null) {
      return result;
    }
    if (fileInfo.getFileName() == null) {
      result.add(fileInfo);
      return result;
    }
    Optional<FileInfo> match =
        files.stream().filter(f -> fileInfo.getFileName().equals(f.getFileName())).findAny();
    result.removeIf(f -> fileInfo.getFileName().equals(f.getFileName()));
    result.add(match.map(existing -> mergeNonNullFields(existing, fileInfo)).orElse(fileInfo));
    return result;
  }

  /** The existing file's values win; terms and issues are combined. */
  private static FileInfo mergeNonNullFields(FileInfo f1, FileInfo f2) {
    List<IssueInfo> issues = new ArrayList<>(f1.getIssues());
    issues.addAll(f2.getIssues());
    return FileInfo.builder()
        .fileName(Optional.ofNullable(f1.getFileName()).orElse(f2.getFileName()))
        .fileType(Optional.ofNullable(f1.getFileType()).orElse(f2.getFileType()))
        .count(Optional.ofNullable(f1.getCount()).orElse(f2.getCount()))
        .indexedCount(Optional.ofNullable(f1.getIndexedCount()).orElse(f2.getIndexedCount()))
        .rowType(Optional.ofNullable(f1.getRowType()).orElse(f2.getRowType()))
        .terms(mergeTermsInfo(f1.getTerms(), f2.getTerms()))
        .issues(issues)
        .build();
  }

  private static List<TermInfo> mergeTermsInfo(List<TermInfo> from, List<TermInfo> to) {
    Set<String> toSet = to.stream().map(TermInfo::getTerm).collect(Collectors.toSet());
    List<TermInfo> result = new ArrayList<>(to);
    from.stream().filter(t -> !toSet.contains(t.getTerm())).forEach(result::add);
    return result;
  }

  /**
   * Merges the generated interpretation metrics into {@code files}: a file of the same row type
   * gets the indexed count, the interpreted term values and new issues; a row type not found is
   * added as it is.
   */
  static List<FileInfo> mergeGenerated(List<FileInfo> files, List<FileInfo> generated) {
    List<FileInfo> result = new ArrayList<>(files);
    for (FileInfo generatedFile : generated) {
      Optional<FileInfo> existing =
          result.stream()
              .filter(f -> f.getRowType() != null && f.getRowType().equals(generatedFile.getRowType()))
              .findFirst();
      if (existing.isPresent()) {
        FileInfo file = existing.get();
        file.setIndexedCount(generatedFile.getIndexedCount());
        mergeTerms(file, generatedFile);
        mergeIssues(file, generatedFile);
      } else {
        result.add(generatedFile);
      }
    }
    return result;
  }

  private static void mergeTerms(FileInfo original, FileInfo generated) {
    List<TermInfo> originalTerms = original.getTerms();
    if (originalTerms == null || originalTerms.isEmpty()) {
      return;
    }
    List<TermInfo> merged = new ArrayList<>(originalTerms);
    Map<String, TermInfo> byName =
        originalTerms.stream()
            .collect(Collectors.toMap(TermInfo::getTerm, Function.identity(), (a, b) -> a));
    for (TermInfo generatedTerm : generated.getTerms()) {
      TermInfo existing = byName.get(generatedTerm.getTerm());
      if (existing != null) {
        existing.setUniqueRawValues(generatedTerm.getUniqueRawValues());
        existing.setInterpretedIndexed(generatedTerm.getInterpretedIndexed());
        existing.setUniqueInterpretedValues(generatedTerm.getUniqueInterpretedValues());
        existing.setSampleInterpretedValuesMap(generatedTerm.getSampleInterpretedValuesMap());
      } else {
        merged.add(generatedTerm);
      }
    }
    merged.forEach(MetricsMerge::setTermGroupAndOrdinal);
    original.setTerms(
        merged.stream()
            .sorted(
                Comparator.comparing(
                        TermInfo::getTermGroup, Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(
                        TermInfo::getTermIndex, Comparator.nullsLast(Comparator.naturalOrder())))
            .collect(Collectors.toList()));
  }

  private static void setTermGroupAndOrdinal(TermInfo term) {
    Term recognised = term.getTerm() == null ? null : TERMS.findTerm(term.getTerm());
    if (recognised == null) {
      term.setTermGroup("UNRECOGNISED");
      term.setTermIndex(-1);
    } else if (recognised instanceof DwcTerm dwcTerm) {
      term.setTermGroup(dwcTerm.prefix() + "_" + dwcTerm.getGroup());
      term.setTermIndex(dwcTerm.ordinal());
    } else if (recognised instanceof Enum<?> enumTerm) {
      term.setTermGroup(recognised.getClass().getSimpleName());
      term.setTermIndex(enumTerm.ordinal());
    } else {
      term.setTermGroup(recognised.getClass().getSimpleName());
      term.setTermIndex(-1);
    }
  }

  private static void mergeIssues(FileInfo file, FileInfo generated) {
    List<IssueInfo> merged = new ArrayList<>(file.getIssues());
    Set<String> known = new HashSet<>();
    merged.forEach(issue -> known.add(issue.getIssue()));
    for (IssueInfo issue : generated.getIssues()) {
      if (known.add(issue.getIssue())) {
        merged.add(issue);
      }
    }
    file.setIssues(merged);
  }
}
