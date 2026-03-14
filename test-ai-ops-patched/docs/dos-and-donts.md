# Kubernetes Operations — Do's and Don'ts

## DO's

### Scaling
- **DO** scale gradually — increase replicas by 2x at a time, not 10x
- **DO** check HPA (HorizontalPodAutoscaler) settings before manual scaling
- **DO** verify resource requests/limits are set before scaling up

### Deployments
- **DO** always use `kubectl rollout undo` to revert a bad deployment
- **DO** set `maxSurge` and `maxUnavailable` in deployment strategy
- **DO** use `--dry-run=client` before applying any manifest changes
- **DO** label all resources consistently for easier selection

### Logging & Debugging
- **DO** use `kubectl logs --previous` to see logs from crashed containers
- **DO** use `kubectl describe pod` as first step for any pod issue
- **DO** check events with `kubectl get events --sort-by=.lastTimestamp`

### Resource Management
- **DO** always set both `requests` and `limits` for CPU and memory
- **DO** use Vertical Pod Autoscaler (VPA) recommendations for sizing
- **DO** monitor node allocatable vs requested resources weekly

---

## DON'Ts

### Dangerous Operations
- **DON'T** use `kubectl delete pod` in production without understanding why it crashed first
- **DON'T** force-delete pods (`--force --grace-period=0`) unless the node is confirmed dead
- **DON'T** edit running pods directly — always update the Deployment spec
- **DON'T** `kubectl exec` into production pods to make runtime changes

### Scaling Anti-patterns
- **DON'T** scale horizontally to fix a memory leak — it only masks the problem (see KI-001)
- **DON'T** set CPU limits lower than CPU requests
- **DON'T** remove resource limits to "fix" OOMKilled — find the actual leak

### ConfigMap / Secret Management
- **DON'T** store secrets in ConfigMaps — use Kubernetes Secrets or a vault
- **DON'T** update a ConfigMap without validating it first (`kubectl diff`)
- **DON'T** assume pods auto-reload after ConfigMap changes — they don't (see KI-002)

### Networking
- **DON'T** bypass the readiness probe by setting `initialDelaySeconds=0`
- **DON'T** expose services as NodePort in production — use Ingress
- **DON'T** delete and recreate a Service — this causes DNS TTL issues

### Maintenance
- **DON'T** drain a node without cordoning it first: `kubectl cordon <node>`
- **DON'T** run `kubectl delete namespace` unless you are certain — it deletes everything
- **DON'T** modify etcd directly under any circumstances
