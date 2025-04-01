{{- define "dashboard_service.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "dashboard_service.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "dashboard_service.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}
