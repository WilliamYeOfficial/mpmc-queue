#include "mpmc_queue.h"

#define QUEUE_CAPACITY  (1u << 12)
#define NUM_PRODUCERS   4
#define NUM_CONSUMERS   4
#define MSGS_PER_THREAD 500000

int main(void)
{
    MPMCBenchResult r = mpmc_bench_run(
        NUM_PRODUCERS, NUM_CONSUMERS,
        MSGS_PER_THREAD, QUEUE_CAPACITY
    );
    mpmc_bench_print(&r, NUM_PRODUCERS, NUM_CONSUMERS,
                     MSGS_PER_THREAD, QUEUE_CAPACITY);
    return 0;
}
