# DwC-DP analysis report: `DWCDP_ANALYSIS`

The validation web service (flow-orchestrator-ws) returns the DwC-DP analyser's report of a DwC-DP
validation through the report endpoint. This page describes the request, the response body, and
example responses that can be used as mocks as they are.

The report is the dwc-dp-analyser's `AnalysisExecution`, stored unchanged by the orchestrator. The
examples were serialized from the analyser's own Java records (dwc-dp-analyser 0.0.17-SNAPSHOT)
with the settings the analyser service uses for its messages.

## Request

```
GET /v1/validation/{key}/validationreport/{attempt}/DWCDP_ANALYSIS
```

| Part | Value |
|---|---|
| `key` | The validation key (UUID), as returned by `POST /v1/validation`. |
| `attempt` | `1`. Validations have a single attempt. |
| Authentication | As for the other validation endpoints (GBIF user; roles `USER`, `APP`, `IPT` or `REGISTRY_ADMIN`). |

Responses:

| Status | Meaning |
|---|---|
| `200` | The report (JSON object below). Parse the body as JSON: the endpoint does not declare a content type, so it may arrive as `text/plain`. |
| `404` | No such report: the analysis has not finished yet, or the validation is not a DwC-DP. |
| `401` | Not authenticated. |

When to ask for it: the report exists once `GET /v1/validation/{key}` lists the step
`VALIDATOR_VALIDATE_DWCDP` with status `FINISHED`. Its overall verdict is `metadata.isValid`: an
invalid package ends the validation `FINISHED` with `metrics.indexeable: false` and the reason in
`metrics.error`.

## Response body

```
AnalysisExecution
├─ result
│  ├─ descriptorValidation        checks of datapackage.json
│  │  ├─ issues: [ValidationIssue]
│  │  ├─ isValid: boolean          no ERROR issue
│  │  └─ hasDataAnalysis: boolean  false: an ERROR stopped the data checks (resourceAnalysisResults is empty)
│  ├─ emlValidation               checks of eml.xml (optional file)
│  │  ├─ isPresent: boolean        false: no eml.xml; then issues is empty and isValid is true
│  │  ├─ issues: [ValidationIssue]
│  │  └─ isValid: boolean          no ERROR issue (does not affect metadata.isValid)
│  └─ resourceAnalysisResults: [   one per data resource (table)
│        name: string              resource name, e.g. "occurrence"
│        totalRows: number
│        columnStatistics: [ { name, populatedValues, uniqueValues } ]   non-null and distinct values per column
│        primaryKeyViolation: null | { resource, fields: [string], violationCount, sampleRows: [object] }
│        foreignKeyViolations: [ { resource, fields: [string], referenceResource, referenceFields: [string], violationCount, sampleRows: [object] } ]
│        dataTypeViolations: [ { resource, field, declaredType, violationCount, sampleValues: [string] } ]
│     ]
├─ metadata
│  ├─ started, finished: string    ISO local date-time in UTC, no offset, microseconds (e.g. "2026-11-20T13:34:02.861137")
│  ├─ features: [string]           checks that ran (enumeration below)
│  └─ isValid: boolean             the verdict
└─ version: string                 report format version, "1.0"
```

`metadata.isValid` is true when `descriptorValidation.isValid` is true and no resource has a
primary-key, foreign-key or data-type violation. EML issues do not affect it.

### ValidationIssue

| Field | Type | Notes |
|---|---|---|
| `severity` | string | `ERROR`, `WARNING` or `INFO`. An `ERROR` makes its section invalid. |
| `violationType` | string | See the enumeration below. |
| `message` | string | Human-readable. |
| `location` | string or null | Where in the document, e.g. a JSON pointer such as `/resources/1/path`. |
| `detail` | string or null | Extra data as a JSON object **serialized into a string**, e.g. `{"line":"14","column":"16"}`. Parse it if needed. |

### Enumerations

`violationType`:
- datapackage.json: `DESCRIPTOR_NOT_FOUND`, `INVALID_JSON`, `MISSING_RESOURCES`, `PATH_NOT_FOUND`,
  `MISSING_NAME`, `RESOURCE_MISSING_NAME`, `FK_UNKNOWN_REFERENCE_RESOURCE`, `UNKNOWN_FIELD_TYPE`,
  `UNRECOGNIZED_PROFILE_VERSION`, `JSON_SCHEMA_VIOLATION`, `JSON_SCHEMA_UNAVAILABLE`,
  `REQUIRED_FIELD_MISSING`, `FIELD_TYPE_MISMATCH`, `FIELD_DEFINITION_MISMATCH`, `FIELD_DUPLICATE`,
  `FIELD_DEFINITION_MISSING`, `FOREIGN_KEY_MISSING`, `FOREIGN_KEY_FIELD_NOT_DECLARED`,
  `FOREIGN_KEY_TARGET_FIELD_NOT_DECLARED`, `FOREIGN_KEY_NOT_IN_CANONICAL_SCHEMA`, `UNKNOWN_FIELD`,
  `TABLE_SCHEMA_UNAVAILABLE`
- eml.xml: `INVALID_XML`, `EML_MISSING_TITLE`, `EML_MISSING_CREATOR`, `EML_XSD_VIOLATION`,
  `EML_XSD_UNAVAILABLE`

`metadata.features`: `COUNT`, `COUNT_DISTINCT`, `FOREIGN_KEY_CONSTRAINT`, `PRIMARY_KEY_UNIQUE`,
`DATA_TYPE_CONSTRAINT`, `DESCRIPTOR_VALIDATION`, `EML_VALIDATION`.

### Notes for mocking and rendering

- Absent objects are written as `null` (`primaryKeyViolation`, `location`, `detail`). Lists are
  never null but may be empty.
- `sampleRows` are rows of the table, as objects of column name to value; values are strings,
  numbers or null. The key order is not significant.
- Counts are whole numbers; very large tables can exceed 2³¹.
- Clients should ignore fields they don't know: the format may gain fields.

## Example responses

### A valid data package

Two resources, no data problems; the EML has a schema warning, which does not make the package
invalid.

```json
{
  "result": {
    "descriptorValidation": {
      "issues": [],
      "isValid": true,
      "hasDataAnalysis": true
    },
    "emlValidation": {
      "isPresent": true,
      "issues": [
        {
          "severity": "WARNING",
          "violationType": "EML_XSD_VIOLATION",
          "message": "XSD error at line 14: cvc-complex-type.2.4.a: Invalid content was found starting with element 'creator'.",
          "location": null,
          "detail": "{\"message\":\"cvc-complex-type.2.4.a: Invalid content was found starting with element 'creator'.\",\"line\":\"14\",\"column\":\"16\",\"fatal\":\"false\"}"
        }
      ],
      "isValid": true
    },
    "resourceAnalysisResults": [
      {
        "name": "event",
        "foreignKeyViolations": [],
        "primaryKeyViolation": null,
        "dataTypeViolations": [],
        "columnStatistics": [
          {
            "name": "eventID",
            "populatedValues": 120,
            "uniqueValues": 120
          },
          {
            "name": "eventDate",
            "populatedValues": 118,
            "uniqueValues": 64
          }
        ],
        "totalRows": 120
      },
      {
        "name": "occurrence",
        "foreignKeyViolations": [],
        "primaryKeyViolation": null,
        "dataTypeViolations": [],
        "columnStatistics": [
          {
            "name": "occurrenceID",
            "populatedValues": 1200,
            "uniqueValues": 1200
          },
          {
            "name": "eventID",
            "populatedValues": 1200,
            "uniqueValues": 120
          },
          {
            "name": "scientificName",
            "populatedValues": 1196,
            "uniqueValues": 87
          }
        ],
        "totalRows": 1200
      }
    ]
  },
  "metadata": {
    "started": "2026-11-20T13:34:02.861137",
    "finished": "2026-11-20T13:34:04.112406",
    "features": [
      "COUNT",
      "COUNT_DISTINCT",
      "FOREIGN_KEY_CONSTRAINT",
      "PRIMARY_KEY_UNIQUE",
      "DATA_TYPE_CONSTRAINT",
      "DESCRIPTOR_VALIDATION",
      "EML_VALIDATION"
    ],
    "isValid": true
  },
  "version": "1.0"
}
```

### Data problems (invalid)

The descriptor is fine, but the occurrence table has duplicate primary keys, occurrences pointing
to events that do not exist, and values that are not integers.

```json
{
  "result": {
    "descriptorValidation": {
      "issues": [],
      "isValid": true,
      "hasDataAnalysis": true
    },
    "emlValidation": {
      "isPresent": true,
      "issues": [
        {
          "severity": "WARNING",
          "violationType": "EML_XSD_VIOLATION",
          "message": "XSD error at line 14: cvc-complex-type.2.4.a: Invalid content was found starting with element 'creator'.",
          "location": null,
          "detail": "{\"message\":\"cvc-complex-type.2.4.a: Invalid content was found starting with element 'creator'.\",\"line\":\"14\",\"column\":\"16\",\"fatal\":\"false\"}"
        }
      ],
      "isValid": true
    },
    "resourceAnalysisResults": [
      {
        "name": "event",
        "foreignKeyViolations": [],
        "primaryKeyViolation": null,
        "dataTypeViolations": [],
        "columnStatistics": [
          {
            "name": "eventID",
            "populatedValues": 120,
            "uniqueValues": 120
          },
          {
            "name": "eventDate",
            "populatedValues": 118,
            "uniqueValues": 64
          }
        ],
        "totalRows": 120
      },
      {
        "name": "occurrence",
        "foreignKeyViolations": [
          {
            "resource": "occurrence",
            "fields": [
              "eventID"
            ],
            "referenceResource": "event",
            "referenceFields": [
              "eventID"
            ],
            "violationCount": 5,
            "sampleRows": [
              {
                "eventID": "ev-missing",
                "occurrenceID": "occ-0009"
              }
            ]
          }
        ],
        "primaryKeyViolation": {
          "resource": "occurrence",
          "fields": [
            "occurrenceID"
          ],
          "violationCount": 3,
          "sampleRows": [
            {
              "eventID": "ev-002",
              "occurrenceID": "occ-0017"
            }
          ]
        },
        "dataTypeViolations": [
          {
            "resource": "occurrence",
            "field": "individualCount",
            "declaredType": "integer",
            "violationCount": 2,
            "sampleValues": [
              "two",
              "3.5"
            ]
          }
        ],
        "columnStatistics": [
          {
            "name": "occurrenceID",
            "populatedValues": 1200,
            "uniqueValues": 1200
          },
          {
            "name": "eventID",
            "populatedValues": 1200,
            "uniqueValues": 120
          },
          {
            "name": "scientificName",
            "populatedValues": 1196,
            "uniqueValues": 87
          },
          {
            "name": "individualCount",
            "populatedValues": 1180,
            "uniqueValues": 23
          }
        ],
        "totalRows": 1200
      }
    ]
  },
  "metadata": {
    "started": "2026-11-20T13:34:02.861137",
    "finished": "2026-11-20T13:34:04.112406",
    "features": [
      "COUNT",
      "COUNT_DISTINCT",
      "FOREIGN_KEY_CONSTRAINT",
      "PRIMARY_KEY_UNIQUE",
      "DATA_TYPE_CONSTRAINT",
      "DESCRIPTOR_VALIDATION",
      "EML_VALIDATION"
    ],
    "isValid": false
  },
  "version": "1.0"
}
```

### Rejected at the descriptor (invalid)

`datapackage.json` names a file that is not in the package; the data checks did not run, and there
is no eml.xml.

```json
{
  "result": {
    "descriptorValidation": {
      "issues": [
        {
          "severity": "ERROR",
          "violationType": "PATH_NOT_FOUND",
          "message": "Resource 'occurrence' path 'data/occurrence.csv' does not exist in the data package.",
          "location": "/resources/1/path",
          "detail": null
        }
      ],
      "isValid": false,
      "hasDataAnalysis": false
    },
    "emlValidation": {
      "isPresent": false,
      "issues": [],
      "isValid": true
    },
    "resourceAnalysisResults": []
  },
  "metadata": {
    "started": "2026-11-20T14:02:11.004211",
    "finished": "2026-11-20T14:02:11.093877",
    "features": [
      "COUNT",
      "COUNT_DISTINCT",
      "FOREIGN_KEY_CONSTRAINT",
      "PRIMARY_KEY_UNIQUE",
      "DATA_TYPE_CONSTRAINT",
      "DESCRIPTOR_VALIDATION",
      "EML_VALIDATION"
    ],
    "isValid": false
  },
  "version": "1.0"
}
```
