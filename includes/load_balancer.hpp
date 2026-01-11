#pragma once
#include <logger.hpp>
#include <string>
#include <vector>
#include <queue>

class LoadBalancer {
private:
    Logger logger;
    std::string work_dir;
    std::queue<int> job_queue;  // Queue of cluster IDs

    /**
     * Partition clustering into separate cluster files
     * Returns vector of created cluster IDs
     */
    std::vector<int> partition_clustering(const std::string& edgelist,
                                          const std::string& cluster_file,
                                          const std::string& output_dir);

    /**
     * Initialize job queue from created clusters
     */
    void initialize_job_queue(const std::vector<int>& created_clusters);

public:
    /**
     * Constructor: Initialize load balancer
     * - Partitions the clustering into separate cluster files
     * - Initializes the job queue
     * This runs synchronously on rank 0 before any workers start
     */
    LoadBalancer(const std::string& edgelist,
                const std::string& cluster_file,
                const std::string& work_dir,
                int log_level);

    /**
     * Runtime phase: Distribute jobs to workers
     * This runs in a separate thread on rank 0
     */
    void run();

    /**
     * Estimate the cost of processing a cluster
     */
    float getCost(int node_count, int edge_count);
};
