# request-trace-uploader

`request-trace-uploader` is the NVCF sidecar for Dynamo request tracing.
Dynamo calls the captured objects `RequestTraceRecord` values. The records that
contain input and output payloads have event type `request_payload`.

The minimum supported Dynamo version is v1.4.0. That version writes every
record type to one rolling `.jsonl.gz` segment family, so records are
classified by their `event_type` field rather than by filename. Dynamo v1.3.x
emitted a separate `AuditRecord` type to its own file family and is not
supported.

This service discovers only closed segments. The highest indexed segment
remains owned by the Dynamo writer.

## How it works

`service.Uploader` runs the pipeline against a journal kept on the spool
volume, so every step survives a restart.

1. Startup: health answers at once and reports not ready. The uploader
   takes the spool lock, waiting up to the drain timeout plus 10 seconds for
   an earlier uploader. It loads the journal, repairs what a crash left, and
   reports ready. Files another uploader left on the spool stay where they
   are. A journal entry whose source it cannot identify keeps the process
   live and not ready until an operator acts.
2. Intake: each closed segment is fingerprinted (SHA-256) and recorded
   before it is read. The highest indexed segment is never read or deleted.
   Up to `REQUEST_TRACE_UPLOADER_PREPARE_WORKERS` segments are prepared at
   once (default one). Results are recorded in spool order whatever order
   they finish in, and a crash mid-prepare leaves each unfinished segment to
   be prepared again from its source. Lines longer than 16 MiB share
   `REQUEST_TRACE_UPLOADER_PREPARE_MEMORY_BYTES` at five times their size:
   a prepare whose line does not fit waits, counted in
   `prepare_memory_waits`, and the line that started first never waits, so
   one line larger than the budget is still read. Each prepare also uses a
   fixed amount for buffers and shorter lines, which depends on the format
   writer.
3. Record handling: suppression, then redaction, then the backend's format
   writer. A backend chooses its transport format by implementing
   `format.Factory` on its client; a backend that takes gzip JSON lines on
   one destination gets the default writer. An unsent transport prepared
   under other record settings, or under an earlier redaction version, is
   prepared again from its source.
4. Batching: prepared segments are grouped per destination within the
   backend's file and byte limits, and lanes are flushed every pass. While
   intake is preparing a backlog, each batch that fills up is submitted at
   once, beside intake, so uploads overlap preparing; a batch that is not
   full waits for the end of the pass. A batch is recorded before the
   backend is called, and a returned job id is saved before the error is
   looked at.
5. Polling: one loop polls every open job on the status interval. A job is
   pending, succeeded, failed or not found. A job with a saved id is polled,
   never submitted again under that id. A job still pending, or whose status
   cannot be read or is not one the core knows, past the max job age is kept
   as stranded and polled less often; a job the backend has no record of is
   kept as stranded at once. A job that failed in a way that may still have
   landed (class `uncertain`, such as an upload the destination could not
   confirm) has its work sent again once as a new job, counted in
   `submits_ambiguous` as a possible duplicate; a second such failure is
   kept as stranded. A status the core does not know is counted in
   `job_status_unknown` and polled again until the max job age.
6. Cleanup: after success, transports are deleted, the source is re-hashed
   if its size or modification time changed, and only then deleted. A source
   name that no longer holds a regular file is kept. A backend that does not
   export, such as `debug` or a dry run, never deletes a source; after a
   restart under a backend that exports, that source is prepared and sent
   again. A cleanup round reads the spool's highest index once per segment
   family, not once per deletion.
7. Shutdown: on SIGTERM the uploader lets work in progress finish, scans
   the spool one last time so a segment that closed after the last scan is
   not left behind, submits what is prepared, polls once more, and releases
   the spool, all within the drain timeout. What is left stays in
   the journal for the next start.
8. Spool bounds: every 10 seconds, apart from scan passes so a long prepare
   or upload never delays it, the uploader measures the spool, updates the
   spool gauges, and deletes quarantine entries older than 7 days, then the
   oldest entries until quarantine is under 20% of the spool. A reject
   artifact still being written is left alone, but counts toward that share.
   If the spool is still above the high watermark (85% by default), segments
   are not deleted then: intake deletes them at its next segment boundary or
   when it ends, once every running prepare is recorded, or at the start of
   the next pass, so no segment is deleted while one is being prepared. The
   eviction policy deletes data until the spool is down to the low watermark
   (75% by default): by default quarantine first, then the oldest closed
   segments that are in no batch. Between the two watermarks nothing is
   deleted, so deletion is a backstop for a nearly full spool. A segment in
   a batch that may have been sent, and the active segment, are never
   deleted. Preparing pauses while the spool is above the high watermark,
   while free space is under 512 MiB or twice the next segment's size, or
   while 16 prepared segments, or the batch segment setting if larger, wait
   for a batch; prepared work is still sent. Segments being prepared count
   toward free space. While full batches are sent during a backlog, intake
   waits at that bound for the next batch instead of stopping. Every
   deletion is counted in `segments_evicted` or `quarantine_evicted`.
   On a small spool the free-space floor is lower: at most half of what
   the high watermark leaves free, 7.5% of the spool at the default 85%, so
   a spool that relief brings back down can prepare again. Below about
   3.3 GiB, 15% is under 512 MiB (154 MiB on a 1 GiB spool), and at the
   high watermark it is all the room left for Dynamo's active segment and
   the transport being written: if they outgrow it, an emptyDir goes over
   its size limit and the kubelet evicts the pod. Fullness is measured on
   the whole filesystem that holds the spool. On a disk shared with other
   data, such as an emptyDir on the node's disk, that data counts too and
   the emptyDir's size limit is not seen, so the pod can be evicted for
   going over the limit before the spool looks full. Set
   `REQUEST_TRACE_UPLOADER_SPOOL_BUDGET_BYTES` to the spool's emptyDir
   `sizeLimit` or its PVC size: the space the files and directories under
   the source, state and quarantine directories take on disk, counted in
   blocks as the kubelet counts an emptyDir, is then measured against that
   budget as well.
   Deletion starts when either the budget or the filesystem is above its
   high watermark and frees each of those down to its own low watermark;
   one only between its watermarks sets nothing. Free space is the least
   of the two, and quarantine's share and the floor are of the smaller.
   The spool and quarantine settings below change these bounds.

Faults follow `internal/retry`: auth failures and outages hold every submit
without moving a segment; a segment fault quarantines that segment after a
batch is split to find it. After `REQUEST_TRACE_UPLOADER_QUARANTINE_TRIP`
segments are quarantined in a row, a breaker stops intake and counts
`quarantine_tripped`. Segments already prepared are still submitted,
polled and cleaned up. After
`REQUEST_TRACE_UPLOADER_QUARANTINE_RESUME` intake reads new
segments again, counts `quarantine_resumed`, and starts a fresh run, so a
fault that is still there trips the breaker again. A success ends a run
but does not close a tripped breaker. A backend that records its own metrics implements
`telemetry.Receiver` and gets the shared recorder. This binary exports
metrics, logs and traces over OTLP, each when its endpoint is set (see
Metrics, Logs and Traces below); a distribution can pass its own recorder
in `service.Options.Metrics` and its own tracer in `service.Options.Tracer`.

## Metrics

Metrics are pushed over OTLP to the workload's collector. There is no scrape
endpoint. Two settings configure the export, and no other
`OTEL_EXPORTER_OTLP_*` variable changes it:

- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`: the collector URL, `http` or
  `https`, used as given. For `http/protobuf` include the path, usually
  `/v1/metrics`. Unset turns the export off.
- `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`: `grpc`, `http`, or `http/protobuf`.
  Required when the endpoint is set.

`OTEL_EXPORTER_OTLP_ENDPOINT` is never used as a fallback.

The exported resource reads the standard `OTEL_RESOURCE_ATTRIBUTES` and
`OTEL_SERVICE_NAME`. The chart sets the function and pod identity through
them, so the collector can tell producers apart; `service.name` defaults to
`request-trace-uploader`.

Every instrument is
named `dynamo.request_trace.<name>`, carries a `backend` attribute, and uses
only bounded attributes; counters start at zero.

Rate, errors and duration of the export path:

- `uploads` (`status`): jobs that finished, `success` or `failure`.
- `upload.duration` (`status`), seconds: one backend submit call, `success`
  when it returned a job id and no error.
- `prepare_duration`, seconds: preparing one segment, from opening the
  format writer through reading, suppression, redaction and writing the
  transport.
- `confirm_latency`, seconds: from a job's submit to its confirmation.
- `export_latency`, seconds: from a segment's discovery to the confirmation
  of its export, once per segment.
- `records_received` (`record_type`): every line read from a segment, by
  Dynamo v1.4.0 event type, or `unknown` for an unparseable line, an older
  release's record, or an event type this build does not know.

- `backend_calls` (`operation`, `status`) and `backend_call.duration`
  (`operation`, `status`), seconds: each HTTP call a backend makes through
  the transport the core hands it (see Traces), by the operation the
  backend names and the status class (`2xx` to `5xx`, or `error` when no
  response came back).

`prepare_duration` and `backend_call.duration` buckets run from 5 ms to 1
hour; the other durations run from 50 ms to 24 hours. Metrics are pushed
every 60 seconds and once more after the shutdown drain. A failed export
never blocks an upload or changes readiness.

## Logs

Every log line is one JSON object on stderr. When they are known, each line
carries the workload's identity: `function_id` and `function_version_id`
from `NVCF_FUNCTION_ID` and `NVCF_FUNCTION_VERSION_ID`, or from
`nvcf.function.id` and `nvcf.function.version.id` in
`OTEL_RESOURCE_ATTRIBUTES`, and `pod` and `cluster` from `k8s.pod.name` and
`k8s.cluster.name` there. Lines name segments and batches by their local
number or short id. They never carry credentials, record payloads, NCA ids,
request ids, remote job ids or local file paths; a backend fault is logged by its
class, scope and reason.

The same records also go to the collector over OTLP when its endpoint is
set, with the shared resource instead of the identity fields. The resource
carries the same identity: `nvcf.function.id` and
`nvcf.function.version.id` from the NVCF function variables when they are
set, as on the local lines, and the rest of `OTEL_RESOURCE_ATTRIBUTES`:

- `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` and `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL`:
  as for metrics. Unset turns OTLP logs off; the JSON lines stay.

## Traces

Spans go to the collector over OTLP when its endpoint is set:

- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and
  `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`: as for metrics. Unset turns traces
  off.

The pipeline makes one span per step: `request-trace-uploader.prepare` for
one segment, `request-trace-uploader.submit` for one backend submit, and
`request-trace-uploader.poll` for one job status read. With more than one
prepare worker, a prepare span ends when its result is recorded, in spool
order, so it can include time spent waiting for an earlier segment. Spans
carry only `backend`, an outcome `status`, and a fault's `class`, `scope`
and `reason`; a failure sets the error status with the reason as its
description and never records error text. Every span is sampled.

A backend that calls its destination over HTTP implements
`telemetry.TransportReceiver` and wraps each HTTP client's transport with
the function it receives, naming the operation, such as `submit` or
`status`. Each call is then a `request-trace-uploader.backend_call` client
span, a child of the step that made it, carries the W3C trace context, and
is counted in `backend_calls` and `backend_call.duration`. The `objectstore`
backend routes every store call through it, as operation `upload`, keeping
its redirect check and TLS settings. Batch and queue sizes are fixed, so an
`OTEL_BSP_*` or `OTEL_BLRP_*` variable cannot change them.

## Shutdown

After the shutdown drain, spans, metrics and logs are exported once more,
together within 5 seconds, so the pod's grace period must cover the drain
timeout plus 5 seconds. Each OTLP signal is configured on its own: an
endpoint without its protocol stops startup, and each signal's export runs
in the background, so a collector that is down costs telemetry, never an
upload or readiness.

The `backend` package defines the export destination contract and a registry.
Submit and status are separate operations so a slow confirmation cannot block
other segments. Backends register themselves from an init function, so a build
links only the backends it imports. This binary links `debug` and
`objectstore`. A distribution that needs another backend imports it in its own
`main`, starts through `runner`, and reuses the `config`, `backend`, `format`,
`telemetry`, `telemetry/otlp`, and `service` packages from this module.

## Configuration

Required:

- `REQUEST_TRACE_UPLOADER_SOURCE_DIR`: absolute directory containing segments
- `REQUEST_TRACE_UPLOADER_BACKEND`: a backend linked into the build, such as
  `objectstore` or `debug` in this binary

Optional, with defaults:

- `REQUEST_TRACE_UPLOADER_SEGMENT_PREFIX`: default `request-trace`
- `REQUEST_TRACE_UPLOADER_SECRETS_FILE`: default `/var/secrets/secrets.json`
- `REQUEST_TRACE_UPLOADER_STATE_DIR`: default under the source directory
- `REQUEST_TRACE_UPLOADER_QUARANTINE_DIR`: default under the source directory
- `HEALTH_ADDR`: default `:8011`
- `REQUEST_TRACE_UPLOADER_SCAN_INTERVAL`: default `30s`
- `REQUEST_TRACE_UPLOADER_ATTEMPT_TIMEOUT`: default `30s`; bounds each backend
  job status call
- `REQUEST_TRACE_UPLOADER_OPERATION_TIMEOUT`: default `90s`; validated, not
  applied: a submit has no core deadline, and an upload deadline belongs to
  the backend. The objectstore backend bounds the wait for the response
  headers once the object is sent, not the upload itself; debug reads locally
- `REQUEST_TRACE_UPLOADER_MAX_RETRIES`: default `2`
- `REQUEST_TRACE_UPLOADER_RETRY_INITIAL_BACKOFF`: default `100ms`
- `REQUEST_TRACE_UPLOADER_RETRY_MAX_BACKOFF`: default `15s`
- `REQUEST_TRACE_UPLOADER_RETRY_MULTIPLIER`: default `2.0`
- `REQUEST_TRACE_UPLOADER_AUDIT_PREFIX`: a second segment family on the same
  spool; default none
- `REQUEST_TRACE_UPLOADER_REDACT_MULTIMODAL`: default `true`
- `REQUEST_TRACE_UPLOADER_DRAIN_TIMEOUT`: shutdown drain after SIGTERM;
  default `60s`, `0s` for none, at most `1h`. Set it to fit the pod's grace
  period.
- `REQUEST_TRACE_UPLOADER_MAX_JOB_AGE`: how long a job that has an id may
  stay unresolved, still pending or with a status that cannot be read or is
  unknown, before it is reported stranded; default `6h`, from `1m` to `168h`. A
  stranded job is kept and polled less often.
- `REQUEST_TRACE_UPLOADER_QUARANTINE_TRIP`: how many segments quarantined
  in a row stop intake; default `5`, from `1` to `1000`
- `REQUEST_TRACE_UPLOADER_QUARANTINE_RESUME`: how long intake stays stopped
  after a trip before it reads new segments again; default `1h`, from `1m`
  to `24h`
- `REQUEST_TRACE_UPLOADER_QUARANTINE_MAX_AGE`: how long a quarantined entry
  is kept; default `168h` (7 days), from `1m` to `720h` (30 days)
- `REQUEST_TRACE_UPLOADER_QUARANTINE_MAX_FRACTION`: the largest share of the
  spool quarantine may hold; default `0.2`, above `0` and at most `1`
- `REQUEST_TRACE_UPLOADER_SPOOL_CHECK_INTERVAL`: how often the spool is
  measured and quarantine bounded, apart from scans; default `10s`, from `1s`
  to `1h`
- `REQUEST_TRACE_UPLOADER_SPOOL_HIGH_WATERMARK`: the used share of the spool
  above which preparing pauses and the eviction policy deletes data; default
  `0.85`, above `0` and below `1`
- `REQUEST_TRACE_UPLOADER_SPOOL_LOW_WATERMARK`: the used share the eviction
  policy frees the spool down to; default `0.75`, above `0` and below the
  high watermark. Set it too when lowering the high watermark to `0.75` or
  below.
- `REQUEST_TRACE_UPLOADER_SPOOL_EVICTION_POLICY`: what is deleted above the
  high watermark: `quarantine-then-oldest` (default), `oldest` (closed
  segments only, quarantine kept), or `none` (nothing deleted; preparing
  pauses until space frees up)
- `REQUEST_TRACE_UPLOADER_SPOOL_MIN_FREE_BYTES`: free space preparing leaves
  for the writer; default `536870912` (512 MiB), from `0` to
  `1099511627776` (1 TiB)
- `REQUEST_TRACE_UPLOADER_STATUS_INTERVAL`: how often every open job is
  polled, whatever the backend; default `30s`, from `1s` to `1h`
- `REQUEST_TRACE_UPLOADER_BATCH_MAX_SEGMENTS`: the most segments in one
  upload batch; default `16` or the backend's file limit per job if lower,
  from `1` to `500`. A value above the backend's limit stops startup.
  `objectstore` and `debug` take one file per job, so they accept only `1`,
  which changes nothing; leave it unset for them.
- `REQUEST_TRACE_UPLOADER_BATCH_MAX_BYTES`: the most transport bytes in one
  upload batch, as whole bytes with no unit; default `1073741824` (1 GiB)
  or the backend's batch limit if lower, from `1048576` (1 MiB) to
  `17179869184` (16 GiB). A value above the backend's batch limit stops
  startup. A segment whose transport alone is larger is quarantined. On a
  backend that sends one segment per job, such as `objectstore` and
  `debug`, this is only the largest transport accepted; `objectstore` also
  takes no object over 5 GiB.
- `REQUEST_TRACE_UPLOADER_CREATES_PER_MINUTE`: how many backend jobs one
  pod may create per minute; default `30`, from `1` to `600`
- `REQUEST_TRACE_UPLOADER_CREATE_BURST`: how many creates may start back to
  back after the budget was idle; default `1`, from `1` to `100`
- `REQUEST_TRACE_UPLOADER_SEGMENT_SCOPE_FAILURES`: how many failures of one
  segment, while other segments succeed, quarantine it; default `5`, from
  `2` to `100`. At `1` a fault of the whole destination would quarantine
  healthy segments instead of holding.
- `REQUEST_TRACE_UPLOADER_UNCONFIRMED_RESENDS`: on a backend that cannot
  take a resend safely, how many times a submit that got no job id and that
  the destination could not confirm is sent again before the work is
  quarantined, a batch being split first; default `2`, from `0` to `10`.
  Each resend may land a duplicate and is counted in `submits_ambiguous`.
  The count is kept in memory, so a restart or a split starts it again.
  Other uncertain submits, such as timeouts during an outage, are retried
  with no limit. `objectstore` and `debug` take a resend safely, so it has
  no effect on them.
- `REQUEST_TRACE_UPLOADER_AUTH_MAX_BACKOFF`: the longest hold after the
  destination refuses the credentials; holds double from `1m` up to it;
  default `30m`, from `1m` to `24h`
- `REQUEST_TRACE_UPLOADER_RETRY_AFTER_MAX`: the longest a destination's
  Retry-After may hold uploads; default `30m`, from `1m` to `24h`
- `REQUEST_TRACE_UPLOADER_SPOOL_BUDGET_BYTES`: the spool's own size, as
  whole bytes with no unit, from `1073741824` (1 GiB) to
  `1125899906842624` (1 PiB); default none, and `0` is none too. Set it
  from the spool's emptyDir `sizeLimit` or its PVC size. The spool bounds
  then count the space the spool takes on disk, in blocks as the kubelet
  counts an emptyDir, against it as well as the filesystem; see step 8 of
  How it works. Without it only the filesystem counts.
- `REQUEST_TRACE_UPLOADER_RECORD_MAX_BYTES`: the most bytes one source
  record (one line) may have, as whole bytes with no unit; default
  `268435456` (256 MiB), from `1048576` (1 MiB) to `536870912` (512 MiB). A
  longer record is not parsed: its raw bytes go to the local reject file and
  it is counted as `records_unreadable` with reason `line_too_long`.
- `REQUEST_TRACE_UPLOADER_PREPARE_WORKERS`: how many segments are prepared
  at once; default `1`, from `1` to `16`. Each worker can use one core.
- `REQUEST_TRACE_UPLOADER_PREPARE_MEMORY_BYTES`: memory that lines longer
  than 16 MiB share across workers, in bytes; default `1073741824`
  (1 GiB), from `67108864` (64 MiB) to `68719476736` (64 GiB)

Time settings are Go durations with a unit, such as `30s`, `5m` or `1h`. A
bare number, a negative value, zero where zero is not allowed, or a value
out of range stops startup.

Used only by one backend:

- `REQUEST_TRACE_UPLOADER_OBJECTSTORE_BUCKET` and
  `REQUEST_TRACE_UPLOADER_OBJECTSTORE_REGION`: required by `objectstore`
- `REQUEST_TRACE_UPLOADER_OBJECTSTORE_ENDPOINT`: an `https://` URL for an
  S3-compatible store other than AWS; default the SDK endpoint for the region
- `REQUEST_TRACE_UPLOADER_OBJECTSTORE_KEY_PREFIX`: prefix for object keys;
  default the bucket root
- `REQUEST_TRACE_UPLOADER_OBJECTSTORE_PATH_STYLE`: path-style bucket
  addressing, which most stores other than AWS need; default `false`
- `REQUEST_TRACE_UPLOADER_OBJECTSTORE_DRY_RUN`: log what would be uploaded,
  send nothing and need no credentials; default `false`
- `REQUEST_TRACE_UPLOADER_OBJECTSTORE_RESPONSE_TIMEOUT`: how long to wait
  for the store's answer once a request, body included, is sent; default
  `5m`, from `10s` to `1h`. It applies to each request, never cuts an upload
  on a slow link, and ends a wait on a store that took the object and never
  answers.
- `REQUEST_TRACE_UPLOADER_DEBUG_VERBOSITY`: `basic` (default) or `detailed`
  logging for `debug`

Object-store submission is synchronous, so its first status poll reports the
final outcome.

The binary loads configuration strictly. An invalid or incompatible value,
or an unknown name that starts with `REQUEST_TRACE_UPLOADER_`, stops startup
with a non-zero exit. Errors name the setting, never its value.
`config.Load` on its own still falls back to defaults with a warning, for
callers that want that.

The uploader reads only the names listed in this README. It has no aliases,
so a deployment sets these names directly.

Missing paths and unreadable secret files prevent readiness.

## Record suppression

Suppression runs before redaction on every record of a closed segment. A
rule has an operator-assigned name, a header group, match values, and an
action. The strictest matching rule wins: `dropRecord` beats
`stripPayload`, and among rules with the same action the first one wins.
Rules are checked on `request_payload` records and on any record that has a
request header field (`headers` or `payload.http_request_headers`). In
Dynamo v1.4.0 only `request_payload` carries headers; the field check means
a newer record type that carries them is checked too.

- `REQUEST_TRACE_UPLOADER_SUPPRESS_NCA_IDS`: comma-separated NCA ids. Each id
  is trimmed, and an `nca-<id>-nca` form is reduced to `<id>`. The list
  builds one `stripPayload` rule named `nca_id` on the headers in
  `REQUEST_TRACE_UPLOADER_SUPPRESS_NCA_HEADERS`. Unset or blank turns it off.
  An id that is empty once reduced, such as `nca--nca`, stops startup.
- `REQUEST_TRACE_UPLOADER_SUPPRESS_NCA_HEADERS`: comma-separated header names
  that carry the NCA id, read in the order given; the first one with a value
  decides. Names are trimmed and lower-cased. The uploader has no built-in
  list, so this is required while `REQUEST_TRACE_UPLOADER_SUPPRESS_NCA_IDS`
  has an id; the deployment supplies the default, for example
  `nvcf-ncaid,nvcf-nca-id`. A blank, repeated or invalid header name stops
  startup, even while no NCA id is set.
- `REQUEST_TRACE_UPLOADER_SUPPRESSION_RULES`: further rules as a JSON list,
  evaluated after the NCA id rule, for example
  `[{"name":"tenants","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`.
  A name is a short lowercase token. The action is `stripPayload` or
  `dropRecord`.
- `REQUEST_TRACE_UPLOADER_SUPPRESS_HEADERLESS`: `export` (default) or `drop`
  for records that rules are not checked on, and for records of a type
  other than `request_payload` on which no rule's header resolved. It
  applies only while a rule exists.
- `REQUEST_TRACE_UPLOADER_SUPPRESSION_STRICT`: a boolean, default `false`.
  When true, a checked record on which some rule's header has no value,
  because the header was not captured or the headers are not an object, is
  replaced by a stub with the reason `header_unresolved` and no NCA id or
  header values; so is a record of another type that has a `payload` field
  of any kind but no request header field. A body outside a `payload`
  field, or under a wrapper the uploader does not read as one, is not
  covered: use the `drop` headerless policy to keep such records out. A
  matching `dropRecord` rule, or the `drop` headerless policy, still drops
  it. When false, such a record is exported unless another rule matches. A
  value that is not a boolean stops startup.

`stripPayload` replaces the record with a stub that has the same structure
and values as the one the earlier shell exporter wrote; its JSON formatting
can differ. `dropRecord` removes the record.

An unparseable rule list, an unknown action or policy, a missing or repeated
name, a rule with no value, or NCA ids without NCA id headers stops startup
with a non-zero exit.

Counters: `records_suppressed` (`rule`, `action`: `strip_payload`,
`drop_record`, or `strip_unresolved` for a record strict suppression
stripped because that rule's header had no value; a record that misses
several rules' headers counts once for each, so do not sum
`strip_unresolved` across rules to count records), `records_evaluated`
(`record_type`, every checked record by its type),
`suppression_header_present` (`rule`), and `records_incomplete`
(`record_type`) for payload records whose `payload_complete` is false.
Header names and values never become attributes. When suppression is on
and the first 100 payload records carry no rule header, the uploader logs
one warning. With strict suppression on, it also logs one warning per rule
the first time it strips a record for that rule. Without strict
suppression, it never holds or drops records for a missing header.

When a restart brings new suppression or redaction settings, a segment not
yet sent everywhere is prepared again from its source under the new
settings. With two destinations, one that already confirmed the segment gets
it again, which can duplicate its rows and is counted in
`submits_ambiguous`. A submit that may have landed is retried as it was, so
its destination can still get the segment under the old settings. The
segment's other destinations wait until that submit is done and are then
prepared again. A segment whose source is gone is quarantined instead of
sent under the old settings.
