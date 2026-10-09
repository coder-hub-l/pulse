# Architecture Decision Records (ADRs)

## ADR 001: Partitioning by `metric_key`
- **Context:** Streaming events require per-key stateful processing (EWMA baselines, tumbling windows, drift statistics).
- **Decision:** Partition Redpanda/Kafka topics by `metric_key` using deterministic hash keys (`StickyKeyPartitioner` or Murmur2).
- **Consequences:** Ensures that all events for a given metric key always land on the same worker partition. Eliminates cross-worker coordination and distributed locks during baseline updates.

## ADR 002: Allowed-Lateness Watermarks over Micro-Batching
- **Context:** Real-world IoT/client events arrive out of order or with clock skew.
- **Decision:** Implement event-time monotonic watermarks with configurable allowed lateness (e.g. 15s - 10m).
- **Consequences:** Late events within allowed lateness update the active window rollups. Events exceeding allowed lateness are immutably routed to the Dead Letter Queue (`pulse-events-dlq`) for audit and offline backfill without corrupting live time-series rollups.

## ADR 003: Statistical Streaming Baselines vs Heavy Deep Learning Models
- **Context:** Need to detect anomalies on 100k+ to 1M metric keys with sub-10ms processing latency and low memory footprint.
- **Decision:** Use Welford's online algorithm for 168 hour-of-week slots combined with EWMA smoothing, z-scores, and Page-Hinkley drift detection.
- **Consequences:** Requires only ~1.4 KB heap memory per tracked metric key. Delivers immediate O(1) detection per event without GPU infrastructure or complex PyTorch/TensorFlow serving pipelines.

## ADR 004: Deduplication with Redis `SETNX` and Idempotency Guard
- **Context:** Kafka consumer crashes and replays can cause double-counting of rollups and alert storms.
- **Decision:** Use Redis atomic `SETNX` with a 24-hour TTL for event idempotency keys, plus an `IdempotencyGuard` on ClickHouse window boundary commits.
- **Consequences:** Provides strict exactly-once effect semantics under consumer replays and network retries.
