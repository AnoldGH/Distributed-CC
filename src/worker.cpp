#include <worker.hpp>

// Constructor
Worker::Worker(Logger& logger): logger(logger) {}

// Main run function
void Worker::run() {
    // TODO: implement worker run
    logger.info("Worker runtime phase started");

    // Worker main loop
    // while (true) {
    //     // TODO: stops the worker when it requests a job from the load balancer but receives a flag "NO_MORE_JOB"
    // }

    logger.info("Worker runtime phase ended");
}