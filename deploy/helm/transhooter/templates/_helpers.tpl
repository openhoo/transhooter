{{- define "transhooter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "transhooter.fullname" -}}
{{- if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{ else }}{{ printf "%s-%s" .Release.Name (include "transhooter.name" .) | trunc 63 | trimSuffix "-" }}{{ end -}}
{{- end -}}
{{- define "transhooter.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "transhooter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "transhooter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "transhooter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "transhooter.image" -}}
{{- printf "%s:%s" .repository .tag -}}
{{- end -}}
