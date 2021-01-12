{{- define "platform-monitoring.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "platform-monitoring.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "platform-monitoring.labels.standard" -}}
app: {{ include "platform-monitoring.name" . }}
chart: {{ include "platform-monitoring.chart" . }}
heritage: {{ .Release.Service | quote }}
release: {{ .Release.Name | quote }}
{{- end -}}

{{- define "platform-monitoring.minio.probes" -}}
livenessProbe:
  httpGet:
    path: /minio/health/live
    port: {{ .Values.minio.port }}
{{- /* if dict is empty 'with' block is not executed, creating default dummy dict */ -}}
{{- with (.Values.minio.livenessProbe | default (dict "dummy" "dummy")) }}
  initialDelaySeconds: {{ .initialDelaySeconds | default 5 }}
  periodSeconds: {{ .periodSeconds | default 10 }}
  timeoutSeconds: {{ .timeoutSeconds | default 1 }}
  successThreshold: {{ .successThreshold | default 1 }}
  failureThreshold: {{ .failureThreshold | default 3 }}
{{- end }}
readinessProbe:
  httpGet:
    path: /minio/health/ready
    port: {{ .Values.minio.port }}
{{- /* if dict is empty 'with' block is not executed, creating default dummy dict */ -}}
{{- with (.Values.minio.readinessProbe | default (dict "dummy" "dummy")) }}
  initialDelaySeconds: {{ .initialDelaySeconds | default 5 }}
  periodSeconds: {{ .periodSeconds | default 10 }}
  timeoutSeconds: {{ .timeoutSeconds | default 1 }}
  successThreshold: {{ .successThreshold | default 1 }}
  failureThreshold: {{ .failureThreshold | default 3 }}
{{- end }}
{{- end -}}

{{- define "platform-monitoring.minio.resources" -}}
{{- if .Values.minio.resources }}
resources:
{{ toYaml .Values.minio.resources | indent 2 }}
{{- end }}
{{- end -}}

{{- define "platform-monitoring.logs.storage.s3.keyPrefixFormat" -}}
kube.var.log.containers.{pod_name}_{namespace_name}_{container_name}
{{- end -}}

{{- define "platform-monitoring.logs.storage.key" -}}
platform-monitoring-logs-storage-key
{{- end -}}
