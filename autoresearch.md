# Autoresearch: Streaming Aggregation Throughput

## Objective
Maximize throughput (events/sec) of `StreamingAggregator.java` — a single-threaded Java streaming window aggregation engine. Processes 10M timestamped CSV sensor events through tumbling windows (1min: count/sum/min/max/avg per sensor) and sliding windows (5min/1min slide: p50/p99 per sensor). 5% late data with watermark-based emission.

## Metrics
- **Primary**: throughput (events/sec, higher is better)
- **Secondary**: elapsed_ms

## How to Run
`./autoresearch.sh` outputs `METRIC throughput=<n>` lines.
Log results with `/home/claude/.claude/plugins/cache/sderosiaux-claude-plugins/claudecode-autoresearch/0.1.0/scripts/log-experiment.sh`.

## Files in Scope
- `src/StreamingAggregator.java` — the aggregation engine (only file to modify)

## Off Limits
- `src/DataGenerator.java` — generates test data
- `src/BatchValidator.java` — correctness oracle
- `autoresearch.sh` — benchmark harness
- `autoresearch.checks.sh` — correctness checks

## Constraints
- `./autoresearch.checks.sh` must pass (output matches BatchValidator)
- Java 21 (can use modern APIs: records, pattern matching, VarHandles, etc.)
- No external dependencies (stdlib only)
- Output format must match exactly (checked by diff vs BatchValidator)

## Profiling Notes
_To be filled after baseline._

## What's Been Tried
_Nothing yet — starting from baseline._

## Landscape Model
_To be built after ~10 experiments._
