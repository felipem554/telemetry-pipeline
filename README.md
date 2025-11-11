# telemetry-pipeline
Telemetry / Event Ingestion Pipeline Stack: Go (ingest API &amp; consumers), NGINX (edge), Kafka (ingest buffer), Flink or simple consumer groups (stream processing), Redis (hot state / lookup / recent joined state), Prometheus + Grafana (metrics + dashboards), Jaeger (tracing), Docker, Kubernetes (k8s), Helm, GitHub Actions (CI/CD).
