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
package org.gbif.orchestrator.ws.persistence;

import org.gbif.api.model.registry.Dataset;
import org.gbif.orchestrator.ws.api.FileFormat;
import org.gbif.orchestrator.ws.api.Metrics;
import org.gbif.orchestrator.ws.api.Validation;
import org.gbif.orchestrator.ws.api.Validation.Status;
import org.gbif.orchestrator.ws.api.ValidationSearchRequest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The {@code validation} table (formerly validator-ws's MyBatis {@code ValidationMapper}).
 *
 * <p>An update never clears a column with a null value (as before), apart from {@code status}, and
 * never undeletes a row. Rows are only soft-deleted.
 */
@Repository
public class ValidationRepository {

  /** Sortable fields of the search API and their columns. */
  private static final Map<String, String> SORT_COLUMNS =
      Map.of("created", "created", "installationKey", "installation_key", "key", "key");

  private static final String FIELDS =
      "key, source_id, installation_key, notification_emails, status, file_format, username,"
          + " file, file_size, clb_dataset_key, clb_validation_message, dataset, metrics,"
          + " created, modified, deleted";

  private static final List<String> EXECUTING =
      Validation.executingStatuses().stream().map(Enum::name).sorted().toList();

  private final JdbcClient jdbc;
  private final ObjectMapper json;

  public ValidationRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
    this.json =
        new ObjectMapper()
            .findAndRegisterModules()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public void create(Validation v) {
    jdbc.sql(
            """
            INSERT INTO validation (key, source_id, installation_key, notification_emails, status,
              file_format, username, file, file_size, dataset, metrics, created, modified)
            VALUES (:key, :sourceId, :installationKey, :emails, :status, :fileFormat, :username,
              :file, :fileSize, :dataset, :metrics, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """)
        .param("key", v.getKey())
        .param("sourceId", v.getSourceId())
        .param("installationKey", v.getInstallationKey())
        .param("emails", write(emptyToNull(v.getNotificationEmails())))
        .param("status", v.getStatus().name())
        .param("fileFormat", v.getFileFormat() == null ? null : v.getFileFormat().name())
        .param("username", v.getUsername())
        .param("file", v.getFile())
        .param("fileSize", v.getFileSize())
        .param("dataset", write(v.getDataset()))
        .param("metrics", write(v.getMetrics()))
        .update();
  }

  /** Any row with this key, including soft-deleted ones (as before). */
  public Optional<Validation> get(UUID key) {
    return jdbc.sql("SELECT " + FIELDS + " FROM validation WHERE key = :key")
        .param("key", key)
        .query(this::map)
        .optional();
  }

  /** Updates the row; returns the number of rows changed (0 or 1). */
  public int update(Validation v) {
    return update(v, "");
  }

  /** Updates the row only if it is still executing and not deleted, so a cancel isn't undone. */
  public int updateIfExecuting(Validation v) {
    return update(v, " AND deleted IS NULL AND status IN (:executing)");
  }

  /**
   * Moves an executing validation to a final status with its metrics; used for orchestrated
   * validations when the workflow ends.
   *
   * @return true if this call made the transition (false: already ended, cancelled or deleted)
   */
  public boolean finish(UUID key, Status status, Metrics metrics) {
    return jdbc.sql(
                """
                UPDATE validation SET status = :status, metrics = :metrics,
                  modified = CURRENT_TIMESTAMP
                WHERE key = :key AND deleted IS NULL AND status IN (:executing)
                """)
            .param("status", status.name())
            .param("metrics", write(metrics))
            .param("key", key)
            .param("executing", EXECUTING)
            .update()
        == 1;
  }

  private int update(Validation v, String condition) {
    List<String> sets = new ArrayList<>();
    Map<String, Object> params = new LinkedHashMap<>();
    sets.add("status = :status");
    params.put("status", v.getStatus().name());
    if (v.getFileFormat() != null) {
      sets.add("file_format = :fileFormat");
      params.put("fileFormat", v.getFileFormat().name());
    }
    if (v.getFile() != null) {
      sets.add("file = :file");
      params.put("file", v.getFile());
    }
    if (v.getFileSize() != null && v.getFileSize() > 0) {
      sets.add("file_size = :fileSize");
      params.put("fileSize", v.getFileSize());
    }
    if (v.getDataset() != null) {
      sets.add("dataset = :dataset");
      params.put("dataset", write(v.getDataset()));
    }
    if (v.getMetrics() != null) {
      sets.add("metrics = :metrics");
      params.put("metrics", write(v.getMetrics()));
    }
    if (v.getClbDatasetKey() != null) {
      sets.add("clb_dataset_key = :clbDatasetKey");
      params.put("clbDatasetKey", v.getClbDatasetKey());
    }
    if (v.getClbValidationMessage() != null) {
      sets.add("clb_validation_message = :clbValidationMessage");
      params.put("clbValidationMessage", v.getClbValidationMessage());
    }
    sets.add("modified = CURRENT_TIMESTAMP");
    params.put("key", v.getKey());
    if (!condition.isEmpty()) {
      params.put("executing", EXECUTING);
    }
    return jdbc.sql(
            "UPDATE validation SET "
                + String.join(", ", sets)
                + " WHERE key = :key"
                + condition)
        .params(params)
        .update();
  }

  /** Soft delete; a deleted row is left as it is. */
  public void delete(UUID key) {
    jdbc.sql(
            "UPDATE validation SET deleted = CURRENT_TIMESTAMP WHERE key = :key AND deleted IS NULL")
        .param("key", key)
        .update();
  }

  /**
   * Non-deleted validations matching the request, newest first unless the request sorts.
   *
   * @param username only this user's validations; null for all users (jobs)
   */
  public List<Validation> list(String username, ValidationSearchRequest request) {
    Filter filter = filter(username, request);
    StringBuilder sql =
        new StringBuilder("SELECT " + FIELDS + " FROM validation WHERE ").append(filter.where);
    sql.append(" ORDER BY ").append(orderBy(request));
    if (request.getLimit() != null) {
      sql.append(" LIMIT :limit OFFSET :offset");
      filter.params.put("limit", request.getLimit());
      filter.params.put("offset", request.getOffset() == null ? 0L : request.getOffset());
    }
    return jdbc.sql(sql.toString()).params(filter.params).query(this::map).list();
  }

  public long count(String username, ValidationSearchRequest request) {
    Filter filter = filter(username, request);
    return jdbc.sql("SELECT count(*) FROM validation WHERE " + filter.where)
        .params(filter.params)
        .query(Long.class)
        .single();
  }

  /** Keys of executing, non-deleted validations not modified since {@code before}. */
  public List<UUID> getRunningValidations(Date before) {
    return jdbc.sql(
            """
            SELECT key FROM validation
            WHERE deleted IS NULL AND status IN (:executing) AND modified < :before
            ORDER BY modified
            """)
        .param("executing", EXECUTING)
        .param("before", new Timestamp(before.getTime()))
        .query(UUID.class)
        .list();
  }

  private record Filter(String where, Map<String, Object> params) {}

  private static Filter filter(String username, ValidationSearchRequest request) {
    List<String> conditions = new ArrayList<>(List.of("deleted IS NULL"));
    Map<String, Object> params = new LinkedHashMap<>();
    if (username != null) {
      conditions.add("username = :username");
      params.put("username", username);
    }
    if (request.getInstallationKey() != null) {
      conditions.add("installation_key = :installationKey");
      params.put("installationKey", request.getInstallationKey());
    }
    if (request.getSourceId() != null) {
      conditions.add("source_id = :sourceId");
      params.put("sourceId", request.getSourceId());
    }
    if (request.getClbDatasetKey() != null) {
      conditions.add("clb_dataset_key = :clbDatasetKey");
      params.put("clbDatasetKey", request.getClbDatasetKey());
    }
    if (request.getStatus() != null && !request.getStatus().isEmpty()) {
      conditions.add("status IN (:status)");
      params.put("status", request.getStatus().stream().map(Enum::name).sorted().toList());
    }
    if (request.getFromDate() != null) {
      conditions.add("created >= :fromDate");
      params.put("fromDate", new Timestamp(request.getFromDate().getTime()));
    }
    if (request.getToDate() != null) {
      conditions.add("created < :toDate");
      params.put("toDate", new Timestamp(request.getToDate().getTime()));
    }
    return new Filter(String.join(" AND ", conditions), params);
  }

  /** The request's sort on known fields only, then key so paging is stable. */
  private static String orderBy(ValidationSearchRequest request) {
    List<String> order = new ArrayList<>();
    if (request.getSortBy() != null) {
      for (ValidationSearchRequest.SortBy sort : request.getSortBy()) {
        String column = SORT_COLUMNS.get(sort.getField());
        if (column != null) {
          order.add(
              column
                  + (sort.getOrder() == ValidationSearchRequest.SortOrder.ASC ? " ASC" : " DESC"));
        }
      }
    }
    if (order.isEmpty()) {
      order.add("created DESC");
    }
    order.add("key");
    return String.join(", ", order);
  }

  private Validation map(ResultSet rs, int row) throws SQLException {
    return Validation.builder()
        .key(rs.getObject("key", UUID.class))
        .sourceId(rs.getString("source_id"))
        .installationKey(rs.getObject("installation_key", UUID.class))
        .notificationEmails(readEmails(rs.getString("notification_emails")))
        .status(Status.valueOf(rs.getString("status")))
        .fileFormat(
            rs.getString("file_format") == null
                ? null
                : FileFormat.valueOf(rs.getString("file_format")))
        .username(rs.getString("username"))
        .file(rs.getString("file"))
        .fileSize(rs.getObject("file_size", Long.class))
        .clbDatasetKey(rs.getObject("clb_dataset_key", Integer.class))
        .clbValidationMessage(rs.getString("clb_validation_message"))
        .dataset(read(rs.getString("dataset"), Dataset.class))
        .metrics(
            Optional.ofNullable(read(rs.getString("metrics"), Metrics.class))
                .orElseGet(() -> Metrics.builder().build()))
        .created(date(rs.getTimestamp("created")))
        .modified(date(rs.getTimestamp("modified")))
        .deleted(date(rs.getTimestamp("deleted")))
        .build();
  }

  private static Date date(Timestamp timestamp) {
    return timestamp == null ? null : new Date(timestamp.getTime());
  }

  private static <T> Collection<T> emptyToNull(Collection<T> values) {
    return values == null || values.isEmpty() ? null : values;
  }

  private Set<String> readEmails(String value) {
    if (value == null) {
      return null;
    }
    List<String> emails = read(value, new TypeReference<List<String>>() {});
    return emails == null ? null : new LinkedHashSet<>(emails);
  }

  private String write(Object value) {
    if (value == null) {
      return null;
    }
    try {
      return json.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot write " + value.getClass().getSimpleName(), e);
    }
  }

  private <T> T read(String value, Class<T> type) {
    if (value == null) {
      return null;
    }
    try {
      return json.readValue(value, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unreadable " + type.getSimpleName() + " in validation", e);
    }
  }

  private <T> T read(String value, TypeReference<T> type) {
    try {
      return json.readValue(value, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unreadable JSON in validation", e);
    }
  }
}
