{{- define "platformMonitoring.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "platformMonitoring.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "platformMonitoring.fluentbit.fullname" -}}
{{- if .Values.fluentbit.fullnameOverride -}}
{{- .Values.fluentbit.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "fluent-bit" .Values.fluentbit.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "platformMonitoring.fluentd.fullname" -}}
{{- if .Values.fluentd.fullnameOverride -}}
{{- .Values.fluentd.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "fluent-bit" .Values.fluentd.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "platformMonitoring.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "platformMonitoring.labels.standard" -}}
app: {{ include "platformMonitoring.name" . }}
chart: {{ include "platformMonitoring.chart" . }}
heritage: {{ .Release.Service | quote }}
release: {{ .Release.Name | quote }}
{{- end -}}

{{- define "platformMonitoring.minio.probes" -}}
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

{{- define "platformMonitoring.minio.resources" -}}
{{- if .Values.minio.resources }}
resources:
{{ toYaml .Values.minio.resources | indent 2 }}
{{- end }}
{{- end -}}

{{- define "platformMonitoring.logs.storage.s3.keyPrefixFormat" -}}
kube.var.log.containers.{pod_name}_{namespace_name}_{container_name}
{{- end -}}

{{- define "platformMonitoring.logs.storage.keySecret" -}}
{{- if .Values.logs.persistence.keySecret -}}
{{ .Values.logs.persistence.keySecret }}
{{- else -}}
{{ include "platformMonitoring.fullname" . }}-logs-storage-key
{{- end -}}
{{- end -}}
