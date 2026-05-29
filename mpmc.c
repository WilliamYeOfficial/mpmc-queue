#define _GNU_SOURCE
#include "mpmc_queue.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <assert.h>

#define CACHE_LINE 64

typedef struct {
    _Alignas(CACHE_LINE) atomic_size_t sequence;
    uint64_t data;
    char _pad[CACHE_LINE - sizeof(atomic_size_t) - sizeof(uint64_t)];
} Slot;

struct MPMCQueue {
    _Alignas(CACHE_LINE) atomic_size_t head;
    char _pad_head[CACHE_LINE - sizeof(atomic_size_t)];
    _Alignas(CACHE_LINE) atomic_size_t tail;
    char _pad_tail[CACHE_LINE - sizeof(atomic_size_t)];
    size_t mask;
    Slot  *slots;
};

MPMCQueue *mpmc_queue_create(size_t capacity)
{
    assert((capacity & (capacity - 1)) == 0);
    MPMCQueue *q = aligned_alloc(CACHE_LINE, sizeof(MPMCQueue));
    if (!q) return NULL;
    q->mask  = capacity - 1;
    q->slots = aligned_alloc(CACHE_LINE, sizeof(Slot) * capacity);
    if (!q->slots) { free(q); return NULL; }
    for (size_t i = 0; i < capacity; ++i)
        atomic_store_explicit(&q->slots[i].sequence, i, memory_order_relaxed);
    atomic_store_explicit(&q->head, 0, memory_order_relaxed);
    atomic_store_explicit(&q->tail, 0, memory_order_relaxed);
    return q;
}

void mpmc_queue_destroy(MPMCQueue *q)
{
    if (q) { free(q->slots); free(q); }
}

size_t mpmc_queue_capacity(const MPMCQueue *q)
{
    return q->mask + 1;
}

int mpmc_queue_enqueue(MPMCQueue *q, uint64_t data)
{
    size_t   pos  = atomic_load_explicit(&q->tail, memory_order_relaxed);
    Slot    *slot;
    intptr_t diff;
    for (;;) {
        slot        = &q->slots[pos & q->mask];
        size_t seq  = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        diff        = (intptr_t)seq - (intptr_t)pos;
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &q->tail, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
                break;
        } else if (diff < 0) {
            return 0;
        } else {
            pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
        }
    }
    slot->data = data;
    atomic_store_explicit(&slot->sequence, pos + 1, memory_order_release);
    return 1;
}

int mpmc_queue_dequeue(MPMCQueue *q, uint64_t *out)
{
    size_t   pos  = atomic_load_explicit(&q->head, memory_order_relaxed);
    Slot    *slot;
    intptr_t diff;
    for (;;) {
        slot        = &q->slots[pos & q->mask];
        size_t seq  = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        diff        = (intptr_t)seq - (intptr_t)(pos + 1);
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &q->head, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
                break;
        } else if (diff < 0) {
            return 0;
        } else {
            pos = atomic_load_explicit(&q->head, memory_order_relaxed);
        }
    }
    *out = slot->data;
    atomic_store_explicit(&slot->sequence, pos + q->mask + 1, memory_order_release);
    return 1;
}

typedef struct {
    MPMCQueue     *q;
    int            id;
    size_t         msgs;
    atomic_size_t *done;
    atomic_size_t *consumed;
    size_t         total_expected;
} ThreadArg;

static void *producer_thread(void *arg)
{
    ThreadArg *a = arg;
    for (size_t i = 0; i < a->msgs; ++i) {
        uint64_t msg = ((uint64_t)a->id << 32) | (uint32_t)i;
        while (!mpmc_queue_enqueue(a->q, msg))
            ;
    }
    atomic_fetch_add(a->done, 1);
    return NULL;
}

static void *consumer_thread(void *arg)
{
    ThreadArg *a = arg;
    uint64_t val;
    while (atomic_load_explicit(a->consumed, memory_order_relaxed) < a->total_expected) {
        if (mpmc_queue_dequeue(a->q, &val))
            atomic_fetch_add_explicit(a->consumed, 1, memory_order_relaxed);
    }
    return NULL;
}

static double timespec_diff(struct timespec a, struct timespec b)
{
    return (b.tv_sec - a.tv_sec) + (b.tv_nsec - a.tv_nsec) * 1e-9;
}

MPMCBenchResult mpmc_bench_run(int num_producers, int num_consumers,
                               size_t msgs_per_thread, size_t queue_capacity)
{
    MPMCQueue *q      = mpmc_queue_create(queue_capacity);
    pthread_t *prod   = malloc(sizeof(pthread_t) * (size_t)num_producers);
    pthread_t *cons   = malloc(sizeof(pthread_t) * (size_t)num_consumers);
    ThreadArg *pargs  = malloc(sizeof(ThreadArg) * (size_t)num_producers);
    ThreadArg *cargs  = malloc(sizeof(ThreadArg) * (size_t)num_consumers);

    atomic_size_t done     = 0;
    atomic_size_t consumed = 0;
    size_t total = (size_t)num_producers * msgs_per_thread;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < num_consumers; ++i) {
        cargs[i] = (ThreadArg){ q, i, msgs_per_thread, NULL, &consumed, total };
        pthread_create(&cons[i], NULL, consumer_thread, &cargs[i]);
    }
    for (int i = 0; i < num_producers; ++i) {
        pargs[i] = (ThreadArg){ q, i, msgs_per_thread, &done, &consumed, total };
        pthread_create(&prod[i], NULL, producer_thread, &pargs[i]);
    }
    for (int i = 0; i < num_producers; ++i) pthread_join(prod[i], NULL);
    for (int i = 0; i < num_consumers; ++i) pthread_join(cons[i], NULL);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = timespec_diff(t0, t1);

    free(prod); free(cons); free(pargs); free(cargs);
    mpmc_queue_destroy(q);

    return (MPMCBenchResult){
        .total_messages  = total,
        .elapsed_seconds = elapsed,
        .throughput_mps  = (double)total / elapsed / 1e6,
    };
}

void mpmc_bench_print(const MPMCBenchResult *r,
                      int producers, int consumers,
                      size_t msgs_per_thread, size_t capacity)
{
    printf("cache-line-queue: MPMC lock-free ring buffer\n");
    printf("  capacity=%zu  producers=%d  consumers=%d  msgs/thread=%zu\n\n",
           capacity, producers, consumers, msgs_per_thread);
    printf("  total messages : %zu\n",         r->total_messages);
    printf("  elapsed        : %.3f s\n",      r->elapsed_seconds);
    printf("  throughput     : %.2f M msg/s\n", r->throughput_mps);
}
