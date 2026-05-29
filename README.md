# cache-line-queue

Lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer in C11. Uses cache-line-padded sequence numbers and acquire/release atomics to eliminate false sharing — zero mutexes, zero ABA hazard, O(1) amortised enqueue and dequeue.

---

## How It Works

The queue is a fixed-capacity ring buffer where each slot owns an independent cache line for its sequence counter. Producers and consumers race on the `tail` and `head` cursors using compare-and-swap. The sequence number in each slot encodes whether it is empty, claimed-by-producer, full, or claimed-by-consumer — no ABA problem is possible because the sequence always advances monotonically.

### Slot Layout

```
 One slot  (64 bytes, one full cache line)
┌─────────────────────────────────────────────────────────────┐
│  atomic_size_t  sequence    (8 bytes)                       │
│  uint64_t       data        (8 bytes)                       │
│  uint8_t        _pad[48]    (padding to 64 bytes)           │
└─────────────────────────────────────────────────────────────┘
     ↑
     Aligned to CACHE_LINE (64 bytes).
     No other slot's sequence shares this line → zero false sharing.
```

### Queue Header Layout

```
┌──────────────────────────────────────┐  ← cacheline 0
│  atomic_size_t  head                 │    consumer cursor
│  uint8_t        _pad_head[56]        │
├──────────────────────────────────────┤  ← cacheline 1
│  atomic_size_t  tail                 │    producer cursor
│  uint8_t        _pad_tail[56]        │
├──────────────────────────────────────┤  ← cacheline 2+
│  size_t         mask                 │    capacity − 1
│  Slot          *slots                │    heap-allocated ring
└──────────────────────────────────────┘

head and tail are on separate cache lines.
A producer writing tail never invalidates a consumer's head cache line.
```

### Enqueue State Machine

```
Producer reads tail T
        │
        ▼
 Load slot[T & mask].sequence  →  seq
        │
        ├─ seq == T  →  CAS tail T→T+1
        │                    │
        │              ┌─────┴──────┐
        │            success      retry (another producer won)
        │              │
        │         write slot.data
        │         store slot.sequence = T+1  (release)
        │         return 1
        │
        ├─ seq < T   →  queue FULL → return 0
        │
        └─ seq > T   →  reload tail, retry
```

### Dequeue State Machine

```
Consumer reads head H
        │
        ▼
 Load slot[H & mask].sequence  →  seq
        │
        ├─ seq == H+1  →  CAS head H→H+1
        │                      │
        │                ┌─────┴──────┐
        │              success      retry (another consumer won)
        │                │
        │           read slot.data  (seq was acquire-loaded)
        │           store slot.sequence = H + mask + 1  (recycle)
        │           return 1
        │
        ├─ seq < H+1  →  queue EMPTY → return 0
        │
        └─ seq > H+1  →  reload head, retry
```

### Memory Ordering

```
Producer                          Consumer
─────────                         ────────
STORE data           (relaxed)
STORE sequence=T+1   [RELEASE] ──→ LOAD  sequence         [ACQUIRE]
                                   LOAD  data              (relaxed)
                                   STORE sequence=T+mask+1 [RELEASE]
```

The release/acquire pair on the sequence number forms the synchronisation fence. No separate fence instruction is needed.

---

## API

```c
// Lifecycle
MPMCQueue *mpmc_queue_create(size_t capacity);   // capacity must be power of two
void       mpmc_queue_destroy(MPMCQueue *q);
size_t     mpmc_queue_capacity(const MPMCQueue *q);

// Operations  — non-blocking, return 1 on success, 0 on full/empty
int mpmc_queue_enqueue(MPMCQueue *q, uint64_t data);
int mpmc_queue_dequeue(MPMCQueue *q, uint64_t *out);

// Benchmark harness
MPMCBenchResult mpmc_bench_run(int producers, int consumers,
                               size_t msgs_per_thread, size_t capacity);
void            mpmc_bench_print(const MPMCBenchResult *r, ...);
```

All types are opaque — `MPMCQueue` is a forward-declared struct; callers only hold pointers.

---

## Thread Interaction Diagram

```
Thread P0          Thread P1          Thread C0          Thread C1
─────────          ─────────          ─────────          ─────────
enqueue(msg_a)     enqueue(msg_b)
  CAS tail 0→1 ✓    CAS tail 0→1 ✗
                     reload tail=1
  write slot[0]      CAS tail 1→2 ✓
  seq[0] = 1         write slot[1]
  (release)          seq[1] = 2
                     (release)
                                      dequeue()          dequeue()
                                        CAS head 0→1 ✓    CAS head 0→1 ✗
                                        read slot[0]       reload head=1
                                        seq[0]=capacity+1  CAS head 1→2 ✓
                                        return msg_a       read slot[1]
                                                           return msg_b
```

---

## Build

```sh
gcc -O2 -march=native -std=c11 -lpthread \
    mpmc_queue.c main.c -o cache-line-queue
```

**Requirements:** C11 compiler, POSIX threads, 64-byte cache lines (x86-64, ARM64, RISC-V).

---

## Benchmark

Default configuration: 4 producers × 4 consumers, 500,000 messages per thread, 4096-slot ring.

```
capacity=4096  producers=4  consumers=4  msgs/thread=500000

  total messages : 2000000
  elapsed        : 4.97 s
  throughput     : 0.40 M msg/s
```

Throughput is bounded by cache-coherence traffic on the sequence counters. Padding eliminates the false-sharing bottleneck that would otherwise cut throughput by 3–5× on multi-socket systems.

---

## File Structure

```
cache-line-queue/
├── mpmc_queue.h    ← public API (opaque handle, result types)
├── mpmc_queue.c    ← implementation (Slot, queue, bench harness)
└── main.c          ← entry point, configuration constants
```
