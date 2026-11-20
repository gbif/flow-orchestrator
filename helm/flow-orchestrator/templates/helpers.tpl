{{/*
Expand the name of the chart.
*/}}
{{- define "flow-orchestrator.name" -}}
{{- .Chart.Name }}
{{- end }}

{{/*
Fully qualified app name: the release name.
*/}}
{{- define "flow-orchestrator.fullname" -}}
{{- .Release.Name }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "flow-orchestrator.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{ include "flow-orchestrator.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "flow-orchestrator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flow-orchestrator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Name of the Secret holding the env vars listed in values "secrets".
*/}}
{{- define "flow-orchestrator.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- include "flow-orchestrator.fullname" . }}-secrets
{{- end }}
{{- end }}
