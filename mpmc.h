#ifndef MPMC_QUEUE_H
#define MPMC_QUEUE_H

#include <stddef.h>
#include <stdint.h>

typedef struct MPMCQueue MPMCQueue;

typedef struct {
    size_t total_messages;
    double elapsed_seconds;
    double throughput_mps;
} MPMCBenchResult;

MPMCQueue       *mpmc_queue_create(size_t capacity);
void             mpmc_queue_destroy(MPMCQueue *q);
int              mpmc_queue_enqueue(MPMCQueue *q, uint64_t data);
int              mpmc_queue_dequeue(MPMCQueue *q, uint64_t *out);
size_t           mpmc_queue_capacity(const MPMCQueue *q);

MPMCBenchResult  mpmc_bench_run(int num_producers, int num_consumers,
                                size_t msgs_per_thread, size_t queue_capacity);
void             mpmc_bench_print(const MPMCBenchResult *r,
                                  int producers, int consumers,
                                  size_t msgs_per_thread, size_t capacity);

#endif
