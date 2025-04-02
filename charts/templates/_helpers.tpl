{{- define "dashboard-service.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "dashboard-service.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "dashboard-service.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}
