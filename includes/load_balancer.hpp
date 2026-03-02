#pragma once
#include <logger.hpp>
#include <constants.hpp>
#include <string>
#include <vector>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>

// Records information of clusters to be assigned. Used to estimate cost and determine priority, etc.
struct ClusterInfo {
    int cluster_id;
    int node_count;     // number of nodes
    int edge_count;     // number of edges
};

// Tree node for tracking hierarchical yield dependencies.
// Each node represents a cluster that may yield sub-clusters during processing.
// A parent is considered resolved when:
//   1. Its own work is done (work_done == true)
//   2. All children have resolved (resolved_children == expected_yields)
// expected_yields is set from yield_count in the WORK_DONE/WORK_ABORTED message.
// Before WORK_DONE, YIELD_REPORTs simply register children; resolution can't happen
// since work_done is false. After WORK_DONE, late YIELD_REPORTs register children
// without affecting resolved_children (they were pre-counted in expected_yields).
struct YieldNode {
    int cluster_id;
    int parent_id;              // -1 for original clusters (from the input file)
    bool work_done = false;     // WORK_DONE or WORK_ABORTED received for this node
    bool aborted = false;       // WORK_ABORTED specifically (triggers descendant sweep)
    int expected_yields = 0;    // yield_count from WORK_DONE/WORK_ABORTED message
    int resolved_children = 0;  // children that have completed, been swept, or been discarded
    std::vector<int> children;  // direct child cluster_ids
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

    // Comparator for job_queue: highest estimated cost on top (max-heap).
    struct CostCompare {
        LoadBalancer* lb;
        bool operator()(const ClusterInfo& a, const ClusterInfo& b) const {
            return lb->get_cost(a) < lb->get_cost(b);
        }
    };

    // Job queue: max-heap by estimated cost. Clusters are popped highest-cost-first.
    std::priority_queue<ClusterInfo, std::vector<ClusterInfo>, CostCompare> job_queue;
    int job_queue_active = 0;                                   // non-dropped clusters in job_queue
    std::unordered_set<int> dropped_clusters;                   // lazy deletion set for aborted descendants

    std::unordered_map<int, ClusterInfo> in_flight_clusters;    // Clusters that are assigned but not yet completed - map for quicker lookup
    std::unordered_map<int, WorkerReport> worker_reports;       // Latest cumulative report per worker rank

    // Tree-based yield ancestry tracking.
    // Each entry represents a cluster involved in yielding (as parent or child).
    // Roots (parent_id == -1) stay in in_flight_clusters until fully resolved.
    // Children are tracked only in yield_tree (removed from in_flight on creation).
    std::unordered_map<int, YieldNode> yield_tree;

    /**
     * Shared logic for WORK_DONE and WORK_ABORTED: handles yield tree tracking,
     * removes from in_flight when appropriate, and checks deferred termination.
     */
    bool handle_cluster_completion(int cluster_id, std::vector<int>& pending_work_requests, int yield_count, bool aborted);

    /**
     * Check if a yield node is resolved (work_done, all yields received, all children resolved).
     * If resolved, erase the subtree and cascade resolution to the parent.
     */
    void try_resolve(int cluster_id, std::vector<int>& pending_work_requests);

    /**
     * Recursively remove a node and all its descendants from yield_tree.
     */
    void erase_subtree(int cluster_id);

    /**
     * Walk the parent chain to check if any ancestor has been aborted.
     */
    bool is_ancestor_aborted(int cluster_id);

    /**
     * For an aborted node: mark unprocessed children as dropped (lazy deletion)
     * so they are skipped when popped from job_queue.
     */
    void sweep_aborted_descendants(int cluster_id);

    /**
     * Pop clusters from job_queue (skipping dropped entries), batch up to
     * min_batch_cost, assign to worker_rank via MPI. Returns true if work
     * was assigned, false if queue was effectively empty.
     */
    bool assign_batch(int worker_rank);

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
    float get_cost(const ClusterInfo& cluster_info);
};