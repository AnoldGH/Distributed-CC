#pragma once
#include <mpi.h>

enum class MessageType: int {
    // Worker to LB
    WORK_REQUEST = 0,   // requesting a cluster to be processed
    WORK_DONE = 1,      // the processing of the assigned cluster is completed successfully
    WORK_ABORTED = 2,   // the processing of the assigned cluster is aborted
    AGGREGATE_DONE = 3, // aggregation of results completed

    // LB to Worker
    DISTRIBUTE_WORK = 4,    // distribute a cluster to be processed

    // Worker to LB (piggybacked on WORK_REQUEST)
    WORKER_REPORT = 5,      // worker status report, sent immediately after WORK_REQUEST
};

constexpr int to_int(MessageType messageType) {
    return static_cast<int>(messageType);
}

// Special cluster ID value to signal no more jobs available
constexpr int NO_MORE_JOBS = -1;

// Cumulative status report sent from worker to load balancer.
// Piggybacked on every WORK_REQUEST (sent as a follow-up message).
// These are convenience stats only — delivery is best-effort.
struct WorkerReport {
    int oom_count;          // clusters killed by signal (likely OOM) since start
    int timeout_count;      // clusters that timed out since start
    int peak_memory_mb;     // max peak RSS (MB) across all clusters processed
};