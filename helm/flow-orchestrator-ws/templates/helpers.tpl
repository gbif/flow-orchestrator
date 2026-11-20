{{/*
Expand the name of the chart.
*/}}
{{- define "flow-orchestrator-ws.name" -}}
{{- .Chart.Name }}
{{- end }}

{{/*
Fully qualified app name: the release name.
*/}}
{{- define "flow-orchestrator-ws.fullname" -}}
{{- .Release.Name }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "flow-orchestrator-ws.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{ include "flow-orchestrator-ws.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "flow-orchestrator-ws.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flow-orchestrator-ws.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Name of the Secret holding the env vars listed in values "secrets".
*/}}
{{- define "flow-orchestrator-ws.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- include "flow-orchestrator-ws.fullname" . }}-secrets
{{- end }}
{{- end }}
