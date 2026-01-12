#include <load_balancer.hpp>
#include <utils.hpp>
#include <constants.hpp>
#include <unordered_map>
#include <algorithm>
#include <set>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

// Constructor
LoadBalancer::LoadBalancer(const std::string& edgelist,
                          const std::string& cluster_file,
                          const std::string& work_dir,
                          int log_level)
    : logger(work_dir + "/logs/load_balancer.log", log_level),
      work_dir(work_dir) {

    const std::string clusters_dir = work_dir + "/" + "clusters";

    logger.info("LoadBalancer initialization starting");

    // Phase 1: Partition clustering into individual cluster files
    std::vector<ClusterInfo> created_clusters = partition_clustering(edgelist, cluster_file, clusters_dir);

    // Phase 2: Initialize job queue from created cluster files
    initialize_job_queue(created_clusters);

    logger.info("LoadBalancer initialization complete");
}

// Partition clustering into separate cluster files
std::vector<ClusterInfo> LoadBalancer::partition_clustering(const std::string& edgelist,
                                                     const std::string& cluster_file,
                                                     const std::string& output_dir) {
    logger.debug("Start partitioning initial clustering");
    logger.debug(">> Edgelist: " + edgelist);
    logger.debug(">> Clustering: " + cluster_file);
    logger.debug(">> Output directory: " + output_dir);

    std::vector<ClusterInfo> created_clusters;  // Track which clusters were created

    // Get delimiters
    char edgelist_delimiter = get_delimiter(edgelist);
    char cluster_delimiter = get_delimiter(cluster_file);

    logger.debug(std::string("Delimiters detected - edgelist: '") + edgelist_delimiter +
                "', cluster: '" + cluster_delimiter + "'");

    // Read clustering file: node_id -> cluster_id
    logger.debug("Reading clustering file...");
    std::unordered_map<int, int> node_to_cluster;
    // std::set<int> cluster_ids;
    std::unordered_map<int, ClusterInfo> clusters;

    std::ifstream clustering_stream(cluster_file);
    if (!clustering_stream.is_open()) {
        logger.error("Failed to open clustering file: " + cluster_file);
        throw std::runtime_error("Failed to open clustering file: " + cluster_file);
    }

    std::string line;
    std::getline(clustering_stream, line); // Skip header
    logger.debug("Clustering file header: " + line);

    int clustering_lines = 0;
    while (std::getline(clustering_stream, line)) {
        std::stringstream ss(line);
        std::string node_str, cluster_str;

        std::getline(ss, node_str, cluster_delimiter);
        std::getline(ss, cluster_str, cluster_delimiter);

        int node_id = std::stoi(node_str);
        int cluster_id = std::stoi(cluster_str);

        node_to_cluster[node_id] = cluster_id;

        if (clusters.count(cluster_id)) {
            ++clusters[cluster_id].node_count;
        } else {
            ClusterInfo info;
            info.cluster_id = cluster_id;
            info.node_count = 1;
            info.edge_count = 0;
            clusters.insert({cluster_id, info});
        }

        clustering_lines++;
    }
    clustering_stream.close();
    logger.debug("Read " + std::to_string(clustering_lines) + " nodes in " +
                std::to_string(clusters.size()) + " clusters");

    // Create storage for edges per cluster
    std::unordered_map<int, std::vector<std::pair<int, int>>> cluster_edges;

    // Read edgelist and partition edges
    logger.debug("Reading edgelist file...");
    std::ifstream edgelist_stream(edgelist);
    if (!edgelist_stream.is_open()) {
        logger.error("Failed to open edgelist file: " + edgelist);
        throw std::runtime_error("Failed to open edgelist file: " + edgelist);
    }

    std::getline(edgelist_stream, line); // Skip header
    logger.debug("Edgelist file header: " + line);

    int total_edges = 0;
    int intra_cluster_edges = 0;
    while (std::getline(edgelist_stream, line)) {
        std::stringstream ss(line);
        std::string source_str, target_str;

        std::getline(ss, source_str, edgelist_delimiter);
        std::getline(ss, target_str, edgelist_delimiter);

        int source = std::stoi(source_str);
        int target = std::stoi(target_str);
        total_edges++;

        // Check if both nodes are in the clustering
        if (node_to_cluster.find(source) != node_to_cluster.end() &&
            node_to_cluster.find(target) != node_to_cluster.end()) {

            int source_cluster = node_to_cluster[source];
            int target_cluster = node_to_cluster[target];

            // If both nodes in same cluster, add edge to that cluster
            if (source_cluster == target_cluster) {
                cluster_edges[source_cluster].emplace_back(source, target);
                intra_cluster_edges++;
            }
        }
    }
    edgelist_stream.close();
    logger.debug("Read " + std::to_string(total_edges) + " edges, " +
                std::to_string(intra_cluster_edges) + " intra-cluster edges");

    // Write out cluster files to output_dir
    logger.info("Writing cluster files to " + output_dir);
    int files_written = 0;
    for (auto& [cluster_id, cluster_info] : clusters) {
        int edge_count = cluster_edges[cluster_id].size();
        if (edge_count == 0) {
            continue;  // cluster is completely disconnected, pass
        }
        cluster_info.edge_count = edge_count;

        std::string filename = output_dir + "/" + std::to_string(cluster_id) + ".edgelist";
        std::ofstream out(filename);

        if (!out.is_open()) {
            logger.error("Failed to create output file: " + filename);
            throw std::runtime_error("Failed to create output file: " + filename);
        }

        out << "source,target\n";

        for (const auto& edge : cluster_edges[cluster_id]) {
            out << edge.first << "," << edge.second << "\n";
        }

        out.close();
        files_written++;
        created_clusters.emplace_back(cluster_info);  // Track this cluster

        if (files_written <= 5 || files_written == static_cast<int>(clusters.size())) {
            logger.debug("Wrote cluster " + std::to_string(cluster_id) + " with " +
                        std::to_string(edge_count) + " edges to " + filename);
        }
    }
    logger.info("partition_clustering completed successfully. Wrote " +
               std::to_string(files_written) + " cluster files");

    return created_clusters;
}

// Initialize job queue from created clusters
void LoadBalancer::initialize_job_queue(const std::vector<ClusterInfo>& created_clusters) {
    logger.info("Initializing job queue with " + std::to_string(created_clusters.size()) + " clusters");

    // Copy clusters to job_queue
    unprocessed_clusters = created_clusters;

    // Sort by cost in descending order. This is a trick to make sorting and popping cheaper.
    // Since we pop from the back, we want most costly at the end.
    std::sort(unprocessed_clusters.begin(), unprocessed_clusters.end(),
        [this](const ClusterInfo& a, const ClusterInfo& b) {
            float cost_a = getCost(a.node_count, a.edge_count);
            float cost_b = getCost(b.node_count, b.edge_count);
            return cost_a < cost_b;  // Ascending order, so most costly is at the back
        });

    logger.info("Job queue initialized with " + std::to_string(unprocessed_clusters.size()) + " unprocessed clusters.");
}

// Runtime phase: Distribute jobs to workers
void LoadBalancer::run() {
    logger.info("LoadBalancer runtime phase started");

    // TODO: Implement job distribution loop
    // - Listen for job requests from workers (MPI_Recv with TAG_JOB_REQUEST)
    // - Send cluster IDs from job_queue (MPI_Send with TAG_JOB_RESPONSE)
    // - Send termination signals when queue is empty

    int active_workers;
    MPI_Comm_size(MPI_COMM_WORLD, &active_workers);

    while (active_workers > 0) {
        // Listen to incoming messages from workers
        int message;
        MPI_Status status;
        MPI_Recv(&message, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        // Check message type
        int worker_rank = status.MPI_SOURCE;
        MessageType message_type = static_cast<MessageType>(status.MPI_TAG);

        if (message_type == MessageType::WORK_REQUEST) {
            // Worker requests a job
            int assign_cluster;

            if (!unprocessed_clusters.empty()) {
                ClusterInfo cluster_info = unprocessed_clusters.back();
                unprocessed_clusters.pop_back();

                assign_cluster = cluster_info.cluster_id;
                float cost = getCost(cluster_info.node_count, cluster_info.edge_count);

                logger.info("Assigning cluster " + std::to_string(assign_cluster) +
                    " (nodes: " + std::to_string(cluster_info.node_count) +
                    ", edges: " + std::to_string(cluster_info.edge_count) +
                    ", estimated cost: " + std::to_string(cost) + ")" +
                    " to worker " + std::to_string(worker_rank) +
                    " (" + std::to_string(unprocessed_clusters.size()) + " jobs remaining)");

                std::ofstream pending_out(work_dir + "/" + "pending" + "/" + std::to_string(assign_cluster));
            } else {
                assign_cluster = NO_MORE_JOBS;
                logger.info("Sending termination signal to worker " + std::to_string(worker_rank));
                --active_workers;
            }

            MPI_Send(&assign_cluster, 1, MPI_INT, worker_rank, to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD);
        } else if (message_type == MessageType::WORK_DONE) {
            logger.info("Worker " + std::to_string(worker_rank) + " completed cluster " + std::to_string(message));

            try {
                fs::remove(work_dir + "/" + "pending" + "/" + std::to_string(message));
            } catch(const std::exception e) {
                logger.error("No pending file found for cluster " + std::to_string(message));
            }
        } else if (message_type == MessageType::WORK_ABORTED) {
            logger.info("Worker " + std::to_string(worker_rank) + " aborted cluster " + std::to_string(message));
        }
    }

    // TODO: termination

    logger.info("LoadBalancer runtime phase ended");
}

// Estimate the cost of a cluster
float LoadBalancer::getCost(int node_count, int edge_count) {
    float density = (2.0f * edge_count) / (node_count * (node_count - 1));
    return node_count + (1.0f / density);
}