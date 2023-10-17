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

{{- define "platformMonitoring.minioGateway.fullname" -}}
{{- if .Values.minioGateway.fullnameOverride -}}
{{- .Values.minioGateway.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "minio-gateway" .Values.minioGateway.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "platformMonitoring.minioGateway.endpoint" -}}
{{- $serviceName := include "platformMonitoring.minioGateway.fullname" . -}}
{{- printf "http://%s:%s" $serviceName (toString .Values.minioGateway.port) -}}
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

{{- define "platformMonitoring.kubeAuthMountRoot" -}}
{{- printf "/var/run/secrets/kubernetes.io/serviceaccount" -}}
{{- end -}}
