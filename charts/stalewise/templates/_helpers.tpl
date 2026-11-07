{{- define "stalewise.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stalewise.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "stalewise.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stalewise.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "stalewise.labels" -}}
{{ include "stalewise.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "stalewise.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "stalewise.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "stalewise.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.digest -}}
{{- printf "%s:%s@%s" .Values.image.repository $tag .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}

{{/* Peer pod IPs join the ACL at runtime; a sidecar hides them behind 127.0.0.6, trusted only under the AuthorizationPolicy. */}}
{{- define "stalewise.purgeAcl" -}}
{{- $acl := list "127.0.0.1/32" "::1/128" -}}
{{- $sidecar := and .Values.mesh.sidecar .Values.signaller.enabled -}}
{{- if and $sidecar (not .Values.mesh.authorizationPolicy) -}}
{{- fail "mesh.authorizationPolicy must be true when mesh.sidecar and signaller.enabled are set; otherwise any mesh client could PURGE or BAN" -}}
{{- end -}}
{{- if and .Values.mesh.sidecar .Values.mesh.authorizationPolicy -}}
{{- $acl = append $acl "127.0.0.6/32" -}}
{{- end -}}
{{- $acl = concat $acl .Values.purgeAcl -}}
{{- join "," (uniq $acl) -}}
{{- end -}}

{{- define "stalewise.templatePath" -}}
{{- if or .Values.template.inline .Values.template.existingConfigMap -}}
{{- printf "/etc/stalewise/template/%s" .Values.template.key -}}
{{- end -}}
{{- end -}}
