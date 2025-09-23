# Clickhouse PoC-PoC

I want to try Clickhouse out for OpenTelemetry logging and tracing.

Before deploying a PoC, I want to make sure the components can work together.

This is a PoC of that.  A PoC-PoC.

This has a docker-compose.yaml which launches:

 - Grafana: http://localhost:3000
 - ClickHouse: http://localhost:8123
 - OpenTelemetry Collector:
   - OTLP gRPC: localhost:4317
   - OTLP HTTP: localhost:4318
   - Syslog UDP: localhost:5514
