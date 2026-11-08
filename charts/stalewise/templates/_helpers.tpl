{{- define "stalewise.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stalewise.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
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
{{- $sidecar := and .Values.mesh.sidecar (or .Values.signaller.enabled .Values.shard.enabled) -}}
{{- if and $sidecar (not .Values.mesh.authorizationPolicy) -}}
{{- fail "mesh.authorizationPolicy must be true when mesh.sidecar is set with signaller.enabled or shard.enabled; otherwise any mesh client could PURGE, BAN or forge shard forwards" -}}
{{- end -}}
{{- if and .Values.mesh.sidecar .Values.mesh.authorizationPolicy -}}
{{- $acl = append $acl "127.0.0.6/32" -}}
{{- end -}}
{{- $acl = concat $acl .Values.purgeAcl -}}
{{- join "," (uniq $acl) -}}
{{- end -}}

{{- define "stalewise.peerAuthority" -}}
{{- .Values.signaller.peerAuthority | default (printf "%s-peers.%s.svc.cluster.local:%v" (include "stalewise.fullname" .) .Release.Namespace .Values.ports.http) -}}
{{- end -}}

{{- define "stalewise.templatePath" -}}
{{- if or .Values.template.inline .Values.template.existingConfigMap -}}
{{- printf "/etc/stalewise/template/%s" .Values.template.key -}}
{{- end -}}
{{- end -}}


{{/* Comma-separated selector labels; the controller matches them against VirtualService sourceLabels. */}}
{{- define "stalewise.sourceLabels" -}}
{{- $labels := list -}}
{{- range $k, $v := (include "stalewise.selectorLabels" . | fromYaml) -}}
{{- $labels = append $labels (printf "%s=%s" $k $v) -}}
{{- end -}}
{{- join "," $labels -}}
{{- end -}}

{{/* Bytes of a "[name=]malloc,<size>" stevedore; 0 for other stevedores. Varnish sizes are binary. */}}
{{- define "stalewise.stevedoreBytes" -}}
{{- $pattern := "^(?:[^=,]+=)?(?:malloc|default),([0-9]+)([kKmMgGtT]?)[bB]?$" -}}
{{- if regexMatch $pattern . -}}
{{- $units := dict "" 1 "k" 1024 "m" 1048576 "g" 1073741824 "t" 1099511627776 -}}
{{- $unit := regexReplaceAll $pattern . "${2}" | lower -}}
{{- mul (regexReplaceAll $pattern . "${1}" | int64) (get $units $unit) -}}
{{- else -}}
0
{{- end -}}
{{- end -}}

{{/* Bytes of an integer Kubernetes quantity such as 512Mi or 1G. */}}
{{- define "stalewise.quantityBytes" -}}
{{- $pattern := "^([0-9]+)(Ki|Mi|Gi|Ti|k|M|G|T)?$" -}}
{{- $q := toString . -}}
{{- if not (regexMatch $pattern $q) -}}
{{- fail (printf "resources.limits.memory %q must be an integer quantity such as 512Mi" $q) -}}
{{- end -}}
{{- $units := dict "" 1 "Ki" 1024 "Mi" 1048576 "Gi" 1073741824 "Ti" 1099511627776 "k" 1000 "M" 1000000 "G" 1000000000 "T" 1000000000000 -}}
{{- mul (regexReplaceAll $pattern $q "${1}" | int64) (get $units (regexReplaceAll $pattern $q "${2}")) -}}
{{- end -}}

{{/*
Smallest memory limit, in MiB, that holds the stores, the base process and capacity.maxObjects.
Measured on Varnish 9.1: about 102 MiB idle base, 0.6 to 1.2 KiB per object outside storage.
src/capacity.rs holds the same constants for the startup check.
*/}}
{{- define "stalewise.minMemoryMi" -}}
{{- $base := 134217728 -}}
{{- $perObject := 1024 -}}
{{- $stores := add (include "stalewise.stevedoreBytes" .Values.storage | int64) (include "stalewise.stevedoreBytes" .Values.transientStorage | int64) -}}
{{- $bytes := add $stores $base (mul (int64 .Values.capacity.maxObjects) $perObject) -}}
{{- div (add $bytes 1048575) 1048576 -}}
{{- end -}}

{{/* Container resources; derives the memory limit and request when unset, fails when the limit is too small. */}}
{{- define "stalewise.resources" -}}
{{- $resources := deepCopy (.Values.resources | default dict) -}}
{{- $limits := $resources.limits | default dict -}}
{{- $requests := $resources.requests | default dict -}}
{{- $min := include "stalewise.minMemoryMi" . | int64 -}}
{{- if hasKey $limits "memory" -}}
{{- $limit := include "stalewise.quantityBytes" $limits.memory | int64 -}}
{{- if lt $limit (mul $min 1048576) -}}
{{- fail (printf "resources.limits.memory %v is below the %dMi that storage, transientStorage, capacity.maxObjects (%v) and the 128Mi base need; raise it or lower the others" $limits.memory $min .Values.capacity.maxObjects) -}}
{{- end -}}
{{- else -}}
{{- $_ := set $limits "memory" (printf "%dMi" $min) -}}
{{- end -}}
{{- if not (hasKey $requests "memory") -}}
{{- $_ := set $requests "memory" $limits.memory -}}
{{- end -}}
{{- $_ := set $resources "limits" $limits -}}
{{- $_ := set $resources "requests" $requests -}}
{{- toYaml $resources -}}
{{- end -}}

{{/* CPU quantity in millicores: "4", "1.5" or "500m". */}}
{{- define "stalewise.millicores" -}}
{{- $q := toString . -}}
{{- if regexMatch "^[0-9]+m$" $q -}}
{{- trimSuffix "m" $q | int64 -}}
{{- else if regexMatch "^[0-9]+(\\.[0-9]+)?$" $q -}}
{{- mulf $q 1000 | floor | int64 -}}
{{- else -}}
{{- fail (printf "sidecar CPU limit %q must be cores or millicores" $q) -}}
{{- end -}}
{{- end -}}

{{/*
Pod annotations: podAnnotations plus the mesh.proxy sidecar settings. Explicit podAnnotations win;
mesh.proxy.concurrency merges into the proxy.istio.io/config JSON next to any drain settings.
*/}}
{{- define "stalewise.podAnnotations" -}}
{{- $annotations := deepCopy (.Values.podAnnotations | default dict) -}}
{{- $proxy := .Values.mesh.proxy -}}
{{- if $proxy.concurrency -}}
{{- $config := fromYaml (get $annotations "proxy.istio.io/config" | default "{}") -}}
{{- if hasKey $config "Error" -}}
{{- fail (printf "podAnnotations proxy.istio.io/config is not valid JSON or YAML: %s" $config.Error) -}}
{{- end -}}
{{- $_ := set $config "concurrency" ($config.concurrency | default $proxy.concurrency) -}}
{{- $_ := set $annotations "proxy.istio.io/config" (toJson $config) -}}
{{- end -}}
{{- with $proxy.cpuRequest -}}
{{- $_ := set $annotations "sidecar.istio.io/proxyCPU" (get $annotations "sidecar.istio.io/proxyCPU" | default (toString .)) -}}
{{- end -}}
{{- with $proxy.cpuLimit -}}
{{- $_ := set $annotations "sidecar.istio.io/proxyCPULimit" (get $annotations "sidecar.istio.io/proxyCPULimit" | default (toString .)) -}}
{{- end -}}
{{- include "stalewise.checkProxyCpu" (dict "annotations" $annotations "proxy" $proxy) -}}
{{- with $annotations -}}
{{- toYaml . -}}
{{- end -}}
{{- end -}}

{{/* Fails when Envoy would run more workers than its CPU limit has cores. */}}
{{- define "stalewise.checkProxyCpu" -}}
{{- $limit := get .annotations "sidecar.istio.io/proxyCPULimit" -}}
{{- if and .proxy.concurrency (not $limit) -}}
{{- fail "mesh.proxy.concurrency needs mesh.proxy.cpuLimit: Envoy workers beyond the sidecar CPU limit are throttled and raise tail latency" -}}
{{- end -}}
{{- $concurrency := (fromYaml (get .annotations "proxy.istio.io/config" | default "{}")).concurrency | default 0 | int64 -}}
{{- if and $limit (gt (mul $concurrency 1000) (include "stalewise.millicores" $limit | int64)) -}}
{{- fail (printf "mesh.proxy.concurrency %d exceeds the sidecar CPU limit of %v cores; throttled Envoy workers raise tail latency" $concurrency $limit) -}}
{{- end -}}
{{- end -}}
