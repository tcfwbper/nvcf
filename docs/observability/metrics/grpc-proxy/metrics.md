# gRPC Proxy Metrics

The NVCF gRPC proxy emits Prometheus metrics on Service `grpc` (and the headless variant `grpc-headless`) in namespace `nvcf`, port `metrics` (10083), path `/metrics`. The exposition format is Prometheus text (via `prometheus/client_golang`).

The metric families below cover client connection health, the NATS pipe to workers, and gRPC worker session attachment.

| Metric name                                              | Metric type | Source           | Description                                                                                            | Unit (where applicable) | Interesting Labels             | Required Filters (where applicable)                  |
| -------------------------------------------------------- | ----------- | ---------------- | ------------------------------------------------------------------------------------------------------ | ----------------------- | ------------------------------ | ---------------------------------------------------- |
| nvcf_grpc_proxy_service_active_connections_total         | Gauge       | grpc:10083/metrics | Active client TCP connections to the gRPC proxy                                                        |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_active_http_requests_total       | Gauge       | grpc:10083/metrics | Active client HTTP requests in-flight                                                                  |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_in_bytes                    | Gauge       | grpc:10083/metrics | Bytes received over the pod's NATS connections, sampled every 5 s                                     | bytes                   |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_in_msgs                     | Gauge       | grpc:10083/metrics | Messages received over the pod's NATS connections, sampled every 5 s                                  |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_out_bytes                   | Gauge       | grpc:10083/metrics | Bytes sent over the pod's NATS connections, sampled every 5 s                                          | bytes                   |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_out_msgs                    | Gauge       | grpc:10083/metrics | Messages sent over the pod's NATS connections, sampled every 5 s                                       |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_error_total                 | Counter     | grpc:10083/metrics | Errors observed on the NATS connection                                                                 |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_failure_total               | Counter     | grpc:10083/metrics | NATS failures classified by cause                                                                      |                         | reason                         | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_disconnect_total            | Counter     | grpc:10083/metrics | NATS disconnect events                                                                                 |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_reconnect_total             | Counter     | grpc:10083/metrics | NATS reconnect attempts                                                                                |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_reconnects                  | Gauge       | grpc:10083/metrics | Network connections reopened while reconnecting, before the handshake completes; refused dials are not counted. Sampled every 5 s |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_lame_duck_total             | Counter     | grpc:10083/metrics | NATS lame-duck messages observed                                                                       |                         |                                | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_connected                   | Gauge       | grpc:10083/metrics | 1 while a NATS connection is connected, 0 while it is reconnecting or closed                          |                         | connection                     | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_send_failures_total         | Counter     | grpc:10083/metrics | Dispatch-connection sends that gave up waiting for the connection, and send attempts that failed because it was reconnecting or closed |                         | send, stage, reason            | namespace="nvcf"                                     |
| nvcf_grpc_proxy_service_nats_send_duration_seconds_bucket | Histogram | grpc:10083/metrics | Dispatch-connection send time from start to final result, including the wait for the connection and retries | seconds | send, result, le | namespace="nvcf" |
| nvcf_grpc_proxy_service_session_init_seconds_bucket      | Histogram   | grpc:10083/metrics | Time spent initializing a gRPC worker session. Fires only on gRPC worker attach to port 10086.         | seconds                 | is_reconnect, le               | namespace="nvcf"                                     |
| http_server_request_duration_seconds_bucket              | Histogram   | grpc:10083/metrics | RED metric: HTTP server request duration on the gRPC proxy.                                            | seconds                 | http_request_method, http_response_status_code, http_route, le | namespace="nvcf"                                     |
| http_server_request_body_size_bytes_bucket               | Histogram   | grpc:10083/metrics | HTTP server request body size                                                                          | bytes                   | http_request_method, http_route, le | namespace="nvcf"                                     |
| http_server_response_body_size_bytes_bucket              | Histogram   | grpc:10083/metrics | HTTP server response body size                                                                         | bytes                   | http_request_method, http_route, le | namespace="nvcf"                                     |
| rpc_client_duration_milliseconds_bucket                  | Histogram   | grpc:10083/metrics | OpenTelemetry gRPC client RPC duration (per-RPC outcome)                                               | milliseconds            | rpc_service, rpc_method, rpc_grpc_status_code, le | namespace="nvcf"                                     |

## Notes

- `nvcf_grpc_proxy_service_session_init_seconds_bucket` is the SLI for gRPC inference function health. HTTP inference functions invoked through the regular HTTP invocation gateway bypass the gRPC proxy entirely and do not register session-init samples here.
- The `nvcf_grpc_proxy_service_nats_*` family is a useful proxy signal for "is the gRPC proxy -> NATS pipe healthy" (in/out bytes and message deltas) and "are NATS upstreams stable" (reconnect and error counters).
- Per-RPC outcomes (success vs. error per call) are covered by the OpenTelemetry `rpc_client_*` family; aggregate proxy-side errors are covered by `nvcf_grpc_proxy_service_nats_error_total`.
- The proxy keeps two NATS connections, labelled `connection` on `nats_connected`. `shared` carries lookups, purges and subscriptions and buffers sends while it reconnects. `dispatch` carries LLS dispatches, work items and session joins and buffers nothing, so a send made while it reconnects fails instead of going out after the outage.
- `nats_in_bytes`, `nats_in_msgs`, `nats_out_bytes`, `nats_out_msgs` and `nats_reconnects` sum both connections and are sampled every 5 s in the background. A sample that waits on a reconnecting connection keeps the previous values until it returns, so these five can be older than 5 s. The `nats_*` counters, `nats_connected` and `nats_send_failures_total` update when the event happens.
- `nats_send_failures_total` labels: `send` is `work_item`, `dispatch` or `session_join`. `stage` is `wait` (gave up waiting for the connection before sending) or `publish` (an attempt failed on the connection's state). `reason` is `canceled`, `timeout`, `closed` or `reconnecting`. At `publish`, `reconnecting` means the NATS client rejected the attempt before sending it, and each send retries it within its deadline, so this counts attempts, including ones a later retry sends. `closed` means the connection closed during the attempt, which NATS may have received. `nats_send_duration_seconds` observes each send once by `result` (`success` or `error`), so its count gives the send rate and error rate.
- The NATS failure `reason` values are `certificate_expired`, `tls_verification`, `tls`, `authentication`, `timeout`, `connection`, and `other`.
- Standard Go runtime metrics (`go_*`) and process metrics (`process_*`) are also exposed on the same endpoint and follow upstream conventions.

## Alert queries

Alert on disconnect churn independently from successful reconnects:

```promql
sum(increase(nvcf_grpc_proxy_service_nats_disconnect_total[5m])) > 0
```

Group NATS errors by their bounded reason label:

```promql
sum by (reason) (increase(nvcf_grpc_proxy_service_nats_failure_total[5m])) > 0
```

Use `reason="certificate_expired"` for certificate expiry alerts. Use
`reason=~"tls_verification|tls"` for other TLS failures and
`reason="authentication"` for NATS credential failures.

## Reproducing locally

```bash
kubectl port-forward -n nvcf svc/grpc 10083:10083
curl http://127.0.0.1:10083/metrics | grep -E '^nvcf_grpc_proxy_'
```
