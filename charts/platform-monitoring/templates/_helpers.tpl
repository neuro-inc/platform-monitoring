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

{{- define "platformMonitoring.env.common" -}}
- name: NP_MONITORING_CLUSTER_NAME
  value: {{ .Values.platform.clusterName }}
- name: NP_MONITORING_CONTAINER_RUNTIME_PORT
  value: {{ .Values.containerRuntime.port | quote }}
- name: NP_MONITORING_PLATFORM_API_TOKEN
{{- if .Values.platform.token }}
{{ toYaml .Values.platform.token | indent 2 }}
{{- end }}
- name: NP_MONITORING_PLATFORM_AUTH_TOKEN
{{- if .Values.platform.token }}
{{ toYaml .Values.platform.token | indent 2 }}
{{- end }}
- name: NP_MONITORING_PLATFORM_CONFIG_TOKEN
{{- if .Values.platform.token }}
{{ toYaml .Values.platform.token | indent 2 }}
{{- end }}
- name: NP_MONITORING_K8S_API_URL
  value: https://kubernetes.default:443
- name: NP_MONITORING_K8S_AUTH_TYPE
  value: token
- name: NP_MONITORING_K8S_CA_PATH
  value:  {{ include "platformMonitoring.kubeAuthMountRoot" . }}/ca.crt
- name: NP_MONITORING_K8S_TOKEN_PATH
  value:  {{ include "platformMonitoring.kubeAuthMountRoot" . }}/token
- name: NP_MONITORING_PLATFORM_API_URL
  value: {{ .Values.platform.apiUrl | quote }}
- name: NP_MONITORING_PLATFORM_AUTH_URL
  value: {{ .Values.platform.authUrl | quote }}
- name: NP_MONITORING_PLATFORM_CONFIG_URL
  value: {{ .Values.platform.configUrl | quote }}
- name: NP_MONITORING_K8S_NS
  value: {{ .Values.jobsNamespace }}
- name: NP_MONITORING_REGISTRY_URL
  value: {{ .Values.platform.registryUrl | quote }}
- name: NP_MONITORING_K8S_KUBELET_PORT
  value: {{ .Values.kubeletPort | quote }}
{{- if .Values.nvidiaDCGMPort }}
- name: NP_MONITORING_K8S_NVIDIA_DCGM_PORT
  value: {{ .Values.nvidiaDCGMPort | quote }}
{{- end }}
{{- if .Values.sentry }}
- name: SENTRY_DSN
  value: {{ .Values.sentry.dsn }}
- name: SENTRY_CLUSTER_NAME
  value: {{ .Values.sentry.clusterName }}
- name: SENTRY_APP_NAME
  value: {{ .Values.sentry.appName }}
- name: SENTRY_SAMPLE_RATE
  value: {{ .Values.sentry.sampleRate | default 0 | quote }}
{{- end }}
{{- if .Values.nodeLabels.job }}
- name: NP_MONITORING_NODE_LABEL_JOB
  value: {{ .Values.nodeLabels.job }}
{{- end }}
{{- if .Values.nodeLabels.nodePool }}
- name: NP_MONITORING_NODE_LABEL_NODE_POOL
  value: {{ .Values.nodeLabels.nodePool }}
{{- end }}
- name: NP_MONITORING_LOGS_CLEANUP_INTERVAL_SEC
  value: {{ .Values.logs.cleanup_interval_sec | quote }}
{{- end -}}

{{- define "platformMonitoring.env.s3" -}}
{{- $logsPersistence := .Values.logs.persistence -}}
{{- if eq $logsPersistence.type "aws" }}
- name: NP_MONITORING_LOGS_STORAGE_TYPE
  value: s3
- name: NP_MONITORING_S3_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "platformMonitoring.logs.storage.keySecret" . }}
      key: access_key_id
- name: NP_MONITORING_S3_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "platformMonitoring.logs.storage.keySecret" . }}
      key: secret_access_key
{{- if $logsPersistence.aws.region }}
- name: NP_MONITORING_S3_REGION
  value: {{ $logsPersistence.aws.region | quote }}
{{- end }}
{{- if $logsPersistence.aws.endpoint }}
- name: NP_MONITORING_S3_ENDPOINT_URL
  value: {{ $logsPersistence.aws.endpoint | quote }}
{{- end }}
- name: NP_MONITORING_S3_JOB_LOGS_BUCKET_NAME
  value: {{ $logsPersistence.aws.bucket | quote }}
{{- else if eq $logsPersistence.type "gcp" }}
- name: NP_MONITORING_LOGS_STORAGE_TYPE
  value: s3
- name: NP_MONITORING_S3_REGION
  value: {{ $logsPersistence.gcp.location | quote }}
- name: NP_MONITORING_S3_ACCESS_KEY_ID
  value: minio_access_key
- name: NP_MONITORING_S3_SECRET_ACCESS_KEY
  value: minio_secret_key
- name: NP_MONITORING_S3_ENDPOINT_URL
  value: {{ include "platformMonitoring.minioGateway.endpoint" . }}
- name: NP_MONITORING_S3_JOB_LOGS_BUCKET_NAME
  value: {{ $logsPersistence.gcp.bucket | quote }}
{{- else if eq $logsPersistence.type "azure" }}
- name: NP_MONITORING_LOGS_STORAGE_TYPE
  value: s3
- name: NP_MONITORING_S3_REGION
  value: minio
- name: NP_MONITORING_S3_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "platformMonitoring.logs.storage.keySecret" . }}
      key: account_name
- name: NP_MONITORING_S3_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "platformMonitoring.logs.storage.keySecret" . }}
      key: account_key
- name: NP_MONITORING_S3_ENDPOINT_URL
  value: {{ include "platformMonitoring.minioGateway.endpoint" . }}
- name: NP_MONITORING_S3_JOB_LOGS_BUCKET_NAME
  value: {{ $logsPersistence.azure.bucket | quote }}
{{- else if eq $logsPersistence.type "minio" }}
- name: NP_MONITORING_LOGS_STORAGE_TYPE
  value: s3
- name: NP_MONITORING_S3_ENDPOINT_URL
  value: {{ $logsPersistence.minio.url | quote }}
- name: NP_MONITORING_S3_REGION
  value: {{ $logsPersistence.minio.region | quote }}
- name: NP_MONITORING_S3_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "platformMonitoring.logs.storage.keySecret" . }}
      key: access_key
- name: NP_MONITORING_S3_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "platformMonitoring.logs.storage.keySecret" . }}
      key: secret_key
- name: NP_MONITORING_S3_JOB_LOGS_BUCKET_NAME
  value: {{ $logsPersistence.minio.bucket | quote }}
{{- end }}
{{- end -}}

{{- define "platformMonitoring.volumes.common" -}}
- name: kube-api-data
  projected:
    sources:
    - serviceAccountToken:
        expirationSeconds: 3600
        path: token
    - configMap:
        name: kube-root-ca.crt
        items:
        - key: ca.crt
          path: ca.crt
{{- end -}}

{{- define "platformMonitoring.volumeMounts.common" -}}
- mountPath: {{ include "platformMonitoring.kubeAuthMountRoot" . }}
  name: kube-api-data
  readOnly: true
{{- end -}}

{{- define "platformMonitoring.logs.compact.selectorLabels" -}}
app: {{ include "platformMonitoring.name" . }}
release: {{ .Release.Name | quote }}
service: platform-monitoring-logs-compact
{{- end -}}
