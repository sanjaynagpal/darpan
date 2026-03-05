# Specification: Stateful Mirroring & Transformation Service (SMTS)

## 1. Overview
The SMTS provides a reliable, in-memory mirror of a remote data source (Type `T`). It allows multiple consumers to subscribe to filtered subsets of data, ensuring a gapless transition from historical snapshots to real-time updates.

## 2. Core Requirements
- **Scale:** ~3,000,000 records in memory ($O(1)$ access).
- **Latency:** Sub-millisecond lookup from the Warehouse.
- **Consistency:** Soft-delete strategy ensures late-joining consumers can synchronize deletions in downstream stores.
- **Reliability:** Weekly hard resets to clear state and re-sync with the Source of Truth.

## 3. Architecture Components

### 3.1 The Warehouse (`DataWarehouse<T, K>`)
- **Storage:** `ConcurrentHashMap<K, MirrorEntry<T>>`.
- **Soft Delete:** Incoming `Delete` events mark the `isDeleted` flag and preserve the record until the weekly reset.
- **Gapless Handover:** 1. Start buffering live events.
    2. Stream snapshot of existing records (including tombstones).
    3. Drain live buffer to the consumer.

### 3.2 The Persistence Pipeline (`T -> R`)
- **Transformation:** Pure function mapping $T$ to a persistent schema $R$.
- **Batching:** Flushes to database when count reaches **500** OR time exceeds **60s**.
- **Retries:** 3 attempts with exponential backoff before moving to a Dead Letter Queue (DLQ).

### 3.3 The Action Pipeline
- Provides a direct `Flow<MirrorEntry<T>>` for consumers who perform side effects (logging, alerting) without transformation.

## 4. Error Handling
- **Network Flaps:** The `SourceIngestor` is responsible for reconnection logic.
- **Poison Pills:** Transformation errors are logged; the pipeline continues to prevent stream blockage.
- **Persistence Failure:** Handled via retry-loop and DLQ logging.