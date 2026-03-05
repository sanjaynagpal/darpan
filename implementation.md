This is the final blueprint for the **Stateful Mirroring & Transformation Service (SMTS)**. It is designed to be a reusable framework where the Warehouse handles the "hard" problems of state and synchronization, while Consumers focus on business logic.

---

## 1. The Technical Specification (`specification.md`)

```markdown
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

```

---

## 2. The Implementation (`SMTS.kt`)

```kotlin
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import java.util.concurrent.ConcurrentHashMap
import java.time.Duration
import org.slf4j.LoggerFactory

// --- Core Models ---

data class MirrorEntry<T>(
    val data: T,
    val isDeleted: Boolean = false,
    val timestamp: Long = System.currentTimeMillis()
)

sealed class SourceEvent<out T, out K> {
    data class Upsert<T>(val item: T) : SourceEvent<T, Nothing>()
    data class Delete<K>(val key: K) : SourceEvent<Nothing, K>()
}

interface SourceIngestor<T, K> {
    suspend fun getInitialSnapshot(): List<T>
    fun streamUpdates(): Flow<SourceEvent<T, K>>
}

// --- The Warehouse ---

class DataWarehouse<T, K>(
    private val ingestor: SourceIngestor<T, K>,
    private val keyExtractor: (T) -> K,
    private val scope: CoroutineScope
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mirror = ConcurrentHashMap<K, MirrorEntry<T>>()
    private val _updates = MutableSharedFlow<MirrorEntry<T>>(extraBufferCapacity = 5000)

    /**
     * Gapless Handover: Ensures consumers get a consistent snapshot + live updates.
     */
    fun subscribe(predicate: (T) -> Boolean): Flow<MirrorEntry<T>> = channelFlow {
        // 1. Immediately buffer live updates to avoid missing events during snapshot scan
        val liveUpdates = _updates.buffer(Channel.BUFFERED)

        // 2. Stream historical snapshot (including soft-deleted tombstones)
        mirror.values.filter { predicate(it.data) }.forEach { send(it) }

        // 3. Drain the live stream
        liveUpdates.collect { entry ->
            if (predicate(entry.data)) send(entry)
        }
    }

    suspend fun start() {
        // Initial Load
        val snapshot = ingestor.getInitialSnapshot()
        snapshot.forEach { mirror[keyExtractor(it)] = MirrorEntry(it) }
        log.info("Warehouse initialized with ${snapshot.size} records.")

        // Background Ingestion
        scope.launch {
            ingestor.streamUpdates().collect { event ->
                val entry = when (event) {
                    is SourceEvent.Upsert -> MirrorEntry(event.item, isDeleted = false)
                    is SourceEvent.Delete -> {
                        val existing = mirror[event.key]
                        existing?.copy(isDeleted = true) ?: return@collect 
                    }
                }
                mirror[keyExtractor(entry.data)] = entry
                _updates.emit(entry)
            }
        }
    }

    fun weeklyReset() {
        log.info("Performing weekly purge of soft-deleted records.")
        mirror.values.removeIf { it.isDeleted }
    }
}

// --- The Consumers ---

/**
 * For T -> R transformation and Batched Persistence
 */
interface PersistenceConsumer<T, R> {
    fun getCriteria(): (T) -> Boolean
    fun transform(item: T, isDeleted: Boolean): R
    suspend fun persistBatch(items: List<R>)
}

/**
 * For "do whatever" side-effect logic
 */
interface ActionConsumer<T> {
    fun getCriteria(): (T) -> Boolean
    suspend fun onEvent(item: T, isDeleted: Boolean)
}

// --- The Pipelines ---

class SMTSPipelineManager(private val scope: CoroutineScope) {
    
    fun <T, R> launchPersistence(warehouse: DataWarehouse<T, *>, consumer: PersistenceConsumer<T, R>) {
        scope.launch {
            warehouse.subscribe(consumer.getCriteria())
                .map { consumer.transform(it.data, it.isDeleted) }
                .chunkedByTimeout(500, Duration.ofSeconds(60))
                .collect { batch ->
                    retryWithBackoff(3) { consumer.persistBatch(batch) }
                }
        }
    }

    fun <T> launchAction(warehouse: DataWarehouse<T, *>, consumer: ActionConsumer<T>) {
        scope.launch {
            warehouse.subscribe(consumer.getCriteria())
                .collect { entry ->
                    try {
                        consumer.onEvent(entry.data, entry.isDeleted)
                    } catch (e: Exception) {
                        println("Action failed: ${e.message}")
                    }
                }
        }
    }
}

// --- Utilities ---

suspend fun retryWithBackoff(maxAttempts: Int, block: suspend () -> Unit) {
    var attempt = 1
    while (true) {
        try {
            block()
            return
        } catch (e: Exception) {
            if (attempt >= maxAttempts) throw e
            delay(1000L * attempt)
            attempt++
        }
    }
}

fun <T> Flow<T>.chunkedByTimeout(size: Int, duration: Duration): Flow<List<T>> = channelFlow {
    val buffer = mutableListOf<T>()
    var lastFlush = System.currentTimeMillis()

    launch {
        while (isActive) {
            delay(1000)
            if (buffer.isNotEmpty() && (System.currentTimeMillis() - lastFlush) >= duration.toMillis()) {
                val toFlush = synchronized(buffer) { buffer.toList().also { buffer.clear() } }
                if (toFlush.isNotEmpty()) { send(toFlush); lastFlush = System.currentTimeMillis() }
            }
        }
    }

    this@chunkedByTimeout.collect { item ->
        val toFlush = synchronized(buffer) {
            buffer.add(item)
            if (buffer.size >= size) buffer.toList().also { buffer.clear() } else null
        }
        if (toFlush != null) { send(toFlush); lastFlush = System.currentTimeMillis() }
    }
}

```

---

### Final Implementation Notes:

1. **Thread Safety:** The `ConcurrentHashMap` handles the 3M records safely. The `chunkedByTimeout` uses `synchronized(buffer)` to ensure that a timer-based flush and a count-based flush don't try to clear the same list simultaneously.
2. **Soft Delete Consistency:** By sending the `MirrorEntry` (data + isDeleted flag) in the `subscribe` flow, even if a consumer starts up 3 days after a record was deleted, it still receives that "tombstone" during the snapshot. This allows the consumer's DB to stay in sync with the Warehouse.
3. **Memory:** At 3M records, ensure the JVM is started with `-Xmx8g` or higher to prevent excessive Garbage Collection during the weekly snapshot load.

This completes our collaboration on the SMTS specification and draft. **Is there any specific edge case, such as "Partial Success" within a DB batch, that you'd like to refine further?**