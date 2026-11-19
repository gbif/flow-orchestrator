# One-off: validator-ws data into the orchestrator schema

Round A replaces validator-ws with `flow-orchestrator-ws`. validator-ws has its own database
(`validation.datasource.*`); its rows are copied once per environment into the registry
database's `orchestrator` schema. Nothing here runs automatically.

**Before:** `db/001`–`004` are applied in the registry database (`004` with the real role names).

## Order

1. **Stop validator-ws.** Running validations are not lost. Their workers keep calling
   `/validation` through the gateway, and those calls fail until step 4, so do steps 1–4 quickly
   or when nothing is running:

   ```sql
   -- validator database
   SELECT count(*) FROM validation
   WHERE deleted IS NULL
     AND status IN ('DOWNLOADING','SUBMITTED','RUNNING','QUEUED','WAITING_FOR_CHECKLISTBANK');
   ```

2. **Copy `validation`.** Enums become text, `notification_emails` (`text[]`) becomes a JSON
   array, and the json columns become text.

   ```sql
   -- psql, connected to the validator database
   \copy (SELECT key, source_id, installation_key, to_json(notification_emails)::text,
                 status::text, file_format::text, username, file, file_size,
                 clb_dataset_key, clb_validation_message, dataset::text, metrics::text,
                 created, modified, deleted
          FROM validation) TO 'validation.csv' WITH (FORMAT csv)

   -- psql, connected to the registry database, as the orchestrator role
   \copy orchestrator.validation (key, source_id, installation_key, notification_emails,
                                  status, file_format, username, file, file_size,
                                  clb_dataset_key, clb_validation_message, dataset, metrics,
                                  created, modified, deleted)
         FROM 'validation.csv' WITH (FORMAT csv)
   ```

3. **Copy `validation_report`, if it has rows.** It had no production writer, so it is probably
   empty (`SELECT count(*) FROM validation_report;`). Its rows become `workflow_report` rows: the
   validation key is the dataset key, and the report text becomes jsonb.

   ```sql
   -- validator database
   \copy (SELECT validation_key, attempt, report_type, report, created, modified
          FROM validation_report) TO 'validation_report.csv' WITH (FORMAT csv)

   -- registry database, as the orchestrator role
   CREATE TEMP TABLE validation_report_in (
     validation_key uuid, attempt integer, report_type varchar(64), report text,
     created timestamptz, modified timestamptz);
   \copy validation_report_in FROM 'validation_report.csv' WITH (FORMAT csv)
   INSERT INTO orchestrator.workflow_report
     (dataset_key, attempt, report_type, report, comment, created, modified)
   SELECT validation_key, attempt, report_type, report::jsonb, 'Copied from validator-ws',
          created, modified
   FROM validation_report_in;
   ```

4. **Start `flow-orchestrator-ws` and point the gateway's `/v1/validation*` route at it.** The
   paths are unchanged, so the data-validator UI and the legacy workers need no change.

5. **Check:** the row counts match in both databases, and `GET /v1/validation/{key}` returns an
   old validation unchanged.

Keep the validator database until Round B has run in that environment. Then it can be dropped.
