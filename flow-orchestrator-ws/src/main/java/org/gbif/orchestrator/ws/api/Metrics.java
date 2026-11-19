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

import org.gbif.orchestrator.ws.api.Validation.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.*;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@Jacksonized
@NoArgsConstructor
@AllArgsConstructor
public class Metrics {

  private boolean indexeable;

  @Builder.Default private List<ValidationStep> stepTypes = new ArrayList<>();

  @JsonProperty("files")
  @Builder.Default
  private List<FileInfo> fileInfos = new ArrayList<>();

  private String error;

  @Data
  @Builder
  @Jacksonized
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ValidationStep implements Comparable<ValidationStep> {

    // Keep stepType as String to prevent a clash between validation-api and gbif-api StepType enums
    private String stepType;
    private Status status;
    private String message;
    private int executionOrder;

    @Override
    public int compareTo(ValidationStep other) {
      return Integer.compare(this.executionOrder, other.executionOrder);
    }
  }

  @Data
  @Builder
  @Jacksonized
  @NoArgsConstructor
  @AllArgsConstructor
  @ToString
  public static class FileInfo {
    private String fileName;
    private DwcFileType fileType;
    private Long count;
    private Long indexedCount;
    private String rowType;
    @Builder.Default private List<TermInfo> terms = Collections.emptyList();
    @Builder.Default private List<IssueInfo> issues = Collections.emptyList();
  }

  @Data
  @Builder
  @Jacksonized
  @NoArgsConstructor
  @AllArgsConstructor
  public static class TermInfo {
    private String term;
    private String termGroup;
    private Integer termIndex; // for ordering terms in the same group
    private Long rawIndexed;
    private Long uniqueRawValues;
    private Long interpretedIndexed;
    private Long uniqueInterpretedValues;
    private Map<String, Long> sampleInterpretedValuesMap;
  }

  @Data
  @Builder
  @Jacksonized
  @NoArgsConstructor
  @AllArgsConstructor
  public static class IssueInfo {
    private String issue;
    private EvaluationCategory issueCategory;
    private Long count;

    @Builder.Default private List<IssueSample> samples = Collections.emptyList();

    public static IssueInfo create(EvaluationType type) {
      return IssueInfo.builder().issue(type.name()).issueCategory(type.getCategory()).build();
    }

    public static IssueInfo create(EvaluationType type, String dataKey, String dataValue) {
      return IssueInfo.builder()
          .issue(type.name())
          .issueCategory(type.getCategory())
          .samples(
              Collections.singletonList(
                  IssueSample.builder()
                      .relatedData(Collections.singletonMap(dataKey, dataValue))
                      .build()))
          .build();
    }
  }

  @Data
  @Builder
  @Jacksonized
  @NoArgsConstructor
  @AllArgsConstructor
  public static class IssueSample {
    private String recordId;
    private Map<String, String> relatedData = Collections.emptyMap();
  }

  @Override
  public String toString() {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(this);
    } catch (IOException e) {
      // NOP
    }
    return "";
  }
}
