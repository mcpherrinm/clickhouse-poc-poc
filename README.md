# Clickhouse PoC-PoC

I want to try Clickhouse out for OpenTelemetry logging and tracing.

Before deploying a PoC, I want to make sure the components can work together.

This is a PoC of that.  A PoC-PoC.

There will be a compose.yaml which launches the following containers:

* Clickhouse
* OpenTelemetry Collector
  * set to export logs/traces/metrics to Clickhouse.
  * TODO: Some setup for getting logs in
* Grafana, configured with the Clickhouse plugin and Datasource

