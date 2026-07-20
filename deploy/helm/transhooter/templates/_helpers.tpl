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

{{- define "transhooter.validateTrustedClientIp" -}}
{{- $trust := .Values.ingress.trustedClientIp -}}
{{- $header := default "" .Values.config.trustedClientIpHeader -}}
{{- if $trust.enabled -}}
  {{- if not .Values.ingress.enabled -}}
    {{- fail "ingress.enabled must be true when ingress.trustedClientIp.enabled is true" -}}
  {{- end -}}
  {{- if not .Values.ingress.className -}}
    {{- fail "ingress.className must identify the ingress-nginx class when trusted client IP is enabled" -}}
  {{- end -}}
  {{- if ne $trust.controller "nginx" -}}
    {{- fail "ingress.trustedClientIp.controller must be nginx" -}}
  {{- end -}}
  {{- if not $trust.configurationSnippetEnabled -}}
    {{- fail "ingress.trustedClientIp.configurationSnippetEnabled must be true and ingress-nginx must allow snippet annotations" -}}
  {{- end -}}
  {{- if not $header -}}
    {{- fail "config.trustedClientIpHeader is required when ingress.trustedClientIp.enabled is true" -}}
  {{- end -}}
  {{- if eq (len $trust.namespaceSelector) 0 -}}
    {{- fail "ingress.trustedClientIp.namespaceSelector must select the trusted ingress namespace" -}}
  {{- end -}}
  {{- if eq (len $trust.podSelector) 0 -}}
    {{- fail "ingress.trustedClientIp.podSelector must select the trusted ingress controller pods" -}}
  {{- end -}}
{{- else if $header -}}
  {{- fail "config.trustedClientIpHeader must be empty unless ingress.trustedClientIp.enabled is true" -}}
{{- end -}}
{{- end -}}
