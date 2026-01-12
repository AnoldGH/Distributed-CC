#pragma once
#include <logger.hpp>
#include <string>
#include <vector>

class Worker {
private:
    Logger& logger;

public:
    Worker(Logger& logger);
    void run();
};