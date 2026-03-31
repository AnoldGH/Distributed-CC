#pragma once
#include <logger.hpp>
#include <constants.hpp>
#include <string>
#include <vector>

class Worker {
private:
    std::string method;  // "CM" or "WCC"
    Logger& logger;
    std::string work_dir;
    std::string clusters_dir;
    std::string algorithm;
    double clustering_parameter;
    int log_level;
    std::string connectedness_criterion;
    std::string mincut_type;
    bool prune;
    int time_limit_per_cluster;  // -1 = no limit
    int report_interval;         // send report every N requests, -1 = disabled
    int num_processors;          // number of processors per worker for CM/MincutOnly
    int yield_node_threshold;    // min node count for yielding sub-clusters (0 = disabled)
    int yield_id_counter = 0;    // auto-incrementing global ID for yielded sub-clusters

    WorkerReport report = {0, 0, 0};  // cumulative stats sent to LB

    /**
     * Process a single cluster
     * Returns {success, yield_count} where yield_count is the number of
     * sub-clusters directly yielded by this cluster's child process.
     */
    std::pair<bool, int> process_cluster(int cluster_id, bool is_yielded);

public:
    Worker(const std::string& method, Logger& logger, const std::string& work_dir,
           const std::string& clusters_dir,
           const std::string& algorithm, double clustering_parameter,
           int log_level, const std::string& connectedness_criterion,
           const std::string& mincut_type, bool prune,
           int time_limit_per_cluster = -1,
           int report_interval = 10,
           int num_processors = 1,
           int yield_node_threshold = 0);
    void run();
};