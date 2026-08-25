# Read-only hardware benchmark

`python -m luxpower.benchmark` is a development probe for measuring a local
LuxPower dongle. It is separate from Home Assistant and production polling.

## Safety boundary

The benchmark can only:

- open and close TCP connections;
- passively receive unsolicited bytes;
- send Modbus input-register reads using function code 4.

The benchmark client has no write method. Before every send, it checks the encoded
packet and rejects any function code other than 4. Live execution also requires the
explicit `--confirm-read-only` acknowledgement. It does not import or call the
existing write API. The final pre-send guard validates the complete 38-byte read
envelope: protocol and frame lengths, translated-data function, both configured
targets, input-read function, register range, and CRC.

The probe never emits raw packets, register values, hosts, or serial numbers in its
structured output. The target is represented by a short SHA-256 fingerprint so
runs against the same configuration can be compared. Error reporting records only
exception class names.

## Inspect the plan without hardware

```bash
python -m luxpower.benchmark --plan
```

This requires no connection details and performs no network access. The two
experimental shapes are:

- `full`: the current six 125-register input reads covering 0-749;
- `operational`: two reads covering 0-107 and 114-232.

The operational shape is the minimum two-request cover of every register currently
classified `OPERATIONAL`. It deliberately includes documented and undocumented
non-operational registers between those addresses. It is benchmark metadata only;
production polling does not use it.

## Live execution

Keep connection details out of shell arguments. Supply them through the process
environment or another local secret mechanism that populates these variables:

```text
LUXPOWER_HOST
LUXPOWER_PORT                 optional; defaults to 8000
LUXPOWER_DONGLE_SERIAL
LUXPOWER_INVERTER_SERIAL
```

Run from an environment where the `luxpower` package and its dependencies are
available:

```bash
python -m luxpower.benchmark \
  --confirm-read-only \
  --cycles 10 \
  --cadences 10,5,3,2 \
  --unsolicited-probes 3 \
  --unsolicited-window 2 \
  --output /tmp/luxpower-benchmark.json
```

The default bounded run performs ten cycles at each cadence for each read shape.
It starts at 10 seconds and proceeds toward 2 seconds. If a shape has any partial
or failed cycle at a cadence, faster tests for that shape are skipped
conservatively. Full and selective shapes are paired at each cadence, their order
alternates between cadences to reduce ordering bias, and they never compete for
the dongle concurrently.

Human-readable progress is written to standard error. Versioned, sanitized JSON is
written to standard output and optionally to `--output`. Store live result files
outside the repository; `/tmp` is recommended.

## Measurements and timing meaning

Durations use `time.monotonic()`. Event timestamps use timezone-aware UTC.

Each reconnect-per-cycle result records:

- TCP connect and close duration;
- the production-equivalent initial `read(300)` with its one-second timeout;
- initial bytes, arrival delay, frame classifications, and combined/trailing data;
- request range, count, send time, response read latency, complete latency, byte
  count, send/drain/response failure phase, parsed count, and recovery counters;
- total cycle duration and interval utilization;
- per-register observation freshness changes.

`first_read_ms` measures when the first socket `read()` completes. It is an upper
bound on first-byte latency, not packet-capture-level timing.

`bytes_sent` means bytes accepted by `StreamWriter.write()` for queuing. The
application cannot prove wire delivery; `drain_completed` records whether the
corresponding drain completed. Effective observation intervals use run-local,
per-register monotonic acceptance times; each register's first observation in a
shape/cadence run is excluded. UTC `observed_at` remains separately available as
the Stage 2-compatible absolute observation timestamp. The reported nearest-rank
p95 is descriptive only; with the default ten cycles it is effectively a small-
sample upper-tail indicator, not a population estimate.

Before cadence testing, passive probes connect and listen for a bounded window
without sending any bytes. Lux frame summaries contain protocol/function/register
metadata only. Where an initial translated-data frame contains registers for the
configured inverter, the benchmark compares its values with the immediate explicit
read and reports only match/difference counts.

Function-193 frames are reported as structurally accepted but with integrity
`unknown`, because the upstream parser documents no known CRC rule for them. They
must not be treated as validated push telemetry. Initial values are compared with
explicit reads only when they came from an integrity-validated function-code-4
input-register frame for the configured inverter.

For safety, a failed experimental request stops the remaining ranges in that
cycle. That is deliberately more conservative than production after some invalid
non-timeout responses, so failure-cycle request counts are not claimed to reproduce
production load exactly. Any partial/failed cycle, freshness invariant breach, or
packet-recovery attempt stops faster tests for that read shape.

## Production invariants

The benchmark does not change:

- the Home Assistant or standalone polling paths;
- the normal 0-749 scan or 125/40-register options;
- holding-register or battery cadence;
- cache, retry, recovery, or freshness semantics;
- connection lifecycle or initial-data discard;
- request/response packet formats;
- write acknowledgement, readback, or entity behavior.

Measured results should guide a later implementation PR. This probe does not enable
selective reads, faster polling, persistent connections, or unsolicited-data use in
production.
