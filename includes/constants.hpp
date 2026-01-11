#pragma once
#include <mpi.h>

enum class MessageType: int {
    WORK_REQUEST = 0,   // requesting a cluster to be processed
    WORK_DONE = 1,      // the processing of the assigned cluster is completed successfully
    WORK_ABORTED = 2,   // the processing of the assigned cluster is aborted
};

constexpr int to_int(MessageType messageType) {
    return static_cast<int>(messageType);
}