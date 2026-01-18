#pragma once
#include <logger.hpp>
#include <string>
#include <vector>

class Worker {
private:
    Logger& logger;
    std::string work_dir;
    std::string algorithm;
    double clustering_parameter;
    int log_level;
    std::string connectedness_criterion;
    std::string mincut_type;
    bool prune;
    int time_limit_per_cluster;  // -1 = no limit

    /**
     * Process a single cluster
     * Returns true if successful, false if aborted
     */
    bool process_cluster(int cluster_id);

public:
    Worker(Logger& logger, const std::string& work_dir,
           const std::string& algorithm, double clustering_parameter,
           int log_level, const std::string& connectedness_criterion,
           const std::string& mincut_type, bool prune,
           int time_limit_per_cluster = -1);
    void run();
};