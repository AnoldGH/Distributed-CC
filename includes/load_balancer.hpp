#pragma once
#include <logger.hpp>
#include <constants.hpp>
#include <string>
#include <vector>
#include <queue>
#include <set>
#include <unordered_map>

// Records information of clusters to be assigned. Used to estimate cost and determine priority, etc.
struct ClusterInfo {
    int cluster_id;
    int node_count;     // number of nodes
    int edge_count;     // number of edges
};

class LoadBalancer {
private:
    std::string method;  // "CM" or "WCC"
    Logger logger;
    std::string work_dir;
    std::string output_file;
    bool use_rank_0_worker;
    float min_batch_cost;
    int drop_cluster_under;
    bool auto_accept_clique;
    std::vector<ClusterInfo> unprocessed_clusters;              // Vector of unprocessed clusters
    std::unordered_map<int, ClusterInfo> in_flight_clusters;    // Clusters that are assigned but not yet completed - map for quicker lookup
    std::unordered_map<int, WorkerReport> worker_reports;       // Latest cumulative report per worker rank

    /**
     * Partition clustering into separate cluster files
     * Returns vector of created cluster IDs
     */
    std::vector<ClusterInfo> partition_clustering(const std::string& edgelist,
                                          const std::string& cluster_file,
                                          const std::string& output_dir);

    /**
     * Load cluster info from pre-partitioned directory
     * Returns vector of ClusterInfo loaded from existing files
     * Note: this method assumes the directory contains a completed partioning and doesn't check if all clusters in the given clustering is included
     */
    std::vector<ClusterInfo> load_partitioned_clusters(const std::string& partitioned_dir);

    /**
     * Initialize job queue from created clusters
     */
    void initialize_job_queue(const std::vector<ClusterInfo>& created_clusters);

    /**
     * Bypass a cluster - write it directly to output without processing
     * Used for clusters that don't need CM processing (e.g., cliques)
     */
    void bypass_cluster(const ClusterInfo& cluster_info, const std::set<int>& nodes);

public:
    void save_checkpoint(); // save checkpoint - usually due to SIGTERM
    bool load_checkpoint(); // attempt to load checkpoint, return true if successful, or false if no checkpoint file exists

    /**
     * Constructor: Initialize load balancer
     * - Partitions the clustering into separate cluster files (or loads from pre-partitioned dir)
     * - Initializes the job queue
     * This runs synchronously on rank 0 before any workers start
     */
    LoadBalancer(const std::string& method,
                const std::string& edgelist,
                const std::string& cluster_file,
                const std::string& work_dir,
                const std::string& output_file,
                int log_level,
                bool use_rank_0_worker,
                const std::string& partitioned_clusters_dir = "",
                bool partition_only = false,
                float min_batch_cost = 200,
                int drop_cluster_under = -1,
                bool auto_accept_clique = false);

    /**
     * Runtime phase: Distribute jobs to workers
     * This runs in a separate thread on rank 0
     */
    void run();

    /**
     * Estimate the cost of processing a cluster
     */
    float get_cost(int node_count, int edge_count);

    /**
     * Estimate the cost of processing a cluster given cluster_info
     */
    float get_cost(ClusterInfo& cluster_info);
};