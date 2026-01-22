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
                          const std::string& output_file,
                          int log_level,
                          bool use_rank_0_worker,
                          const std::string& partitioned_clusters_dir,
                          bool partition_only)
    : logger(work_dir + "/logs/load_balancer.log", log_level),
      work_dir(work_dir),
      output_file(output_file),
      use_rank_0_worker(use_rank_0_worker) {

    const std::string clusters_dir = work_dir + "/" + "clusters";
    std::string summary_filename = partitioned_clusters_dir + "/summary.csv";

    logger.info("LoadBalancer initialization starting");

    std::vector<ClusterInfo> created_clusters;

    // Phase 1: Load or partition clusters
    if (fs::exists(summary_filename)) {
        logger.info("Loading pre-partitioned clusters from: " + partitioned_clusters_dir);
        created_clusters = load_partitioned_clusters(partitioned_clusters_dir);
    } else {
        logger.info("Partitioning clustering into individual cluster files");
        created_clusters = partition_clustering(edgelist, cluster_file, clusters_dir);
    }

    if (partition_only) {
        logger.info("Partition-only mode: skipping job queue initialization");
        return;
    }

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
    std::unordered_map<int, std::set<int>> cluster_to_node;
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
        cluster_to_node[cluster_id].insert(node_id);

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
        std::string cluster_filename = output_dir + "/" + std::to_string(cluster_id) + ".cluster";
        std::ofstream out(filename);
        std::ofstream cluster_out(cluster_filename);

        if (!out.is_open()) {
            logger.error("Failed to create output file: " + filename);
            throw std::runtime_error("Failed to create output file: " + filename);
        }

        out << "source,target\n";
        cluster_out << "node_id,cluster_id\n";

        // Write edgelist
        for (const auto& edge : cluster_edges[cluster_id]) {
            out << edge.first << "," << edge.second << "\n";
        }

        // Write cluster file
        for (const auto& node : cluster_to_node[cluster_id]) {
            cluster_out << node << "," << cluster_id << "\n";
        }

        out.close();
        cluster_out.close();
        files_written++;
        created_clusters.emplace_back(cluster_info);  // Track this cluster
    }

    // Write summary file for quicker load
    std::string summary_filename = output_dir + "/summary.csv";
    std::ofstream out_summary(summary_filename);
    out_summary << "cluster_id,node_count,edge_count\n";
    for (const auto& cluster : created_clusters)
        out_summary << cluster.cluster_id << "," << cluster.node_count << "," << cluster.edge_count << "\n";

    logger.info("partition_clustering completed successfully. Wrote " +
               std::to_string(files_written) + " cluster files");

    return created_clusters;
}

// Load cluster info from pre-partitioned directory
std::vector<ClusterInfo> LoadBalancer::load_partitioned_clusters(const std::string& partitioned_dir) {
    logger.debug("Loading clusters from: " + partitioned_dir);

    std::vector<ClusterInfo> clusters;

    // Load summary file
    std::string summary_filename = partitioned_dir + "/summary.csv";
    std::ifstream summary(summary_filename);
    std::string line;
    std::getline(summary, line);  // skip header

    while (std::getline(summary, line)) {
        std::istringstream ss{line};
        std::string cluster_id, node_count, edge_count;
        std::getline(ss, cluster_id, ',');
        std::getline(ss, node_count, ',');
        std::getline(ss, edge_count, ',');

        ClusterInfo cluster_info;
        cluster_info.cluster_id = std::stoi(cluster_id);
        cluster_info.node_count = std::stoi(node_count);
        cluster_info.edge_count = std::stoi(edge_count);

        clusters.push_back(cluster_info);

        logger.debug("Loaded cluster " + cluster_id +
                    " (nodes=" + node_count +
                    ", edges=" + edge_count + ")");
    }

    logger.info("Loaded " + std::to_string(clusters.size()) + " clusters from " + partitioned_dir);
    return clusters;
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

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int num_workers = use_rank_0_worker ? size : size - 1;

    logger.info("Managing " + std::to_string(num_workers) + " workers");

    int active_workers = num_workers;

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
        } else if (message_type == MessageType::AGGREGATE_DONE) {
            logger.info("Worker " + std::to_string(worker_rank) + " completed worker-level aggregation.");
            --active_workers;
        }
    }

    // Aggregation phase: combine outputs from all workers
    int start_cluster_id = 0;
    std::string clusters_output_dir = work_dir + "/output/";

    fs::remove(output_file);
    std::ofstream out(output_file, std::ios::app);
    out << "node_id,cluster_id\n";

    int first_worker = use_rank_0_worker ? 0 : 1;
    for (int worker_rank = first_worker; worker_rank < size; ++worker_rank) {
        std::string worker_output_file = clusters_output_dir + "worker_" + std::to_string(worker_rank) + ".out";

        // Aggregation logic
        std::ifstream in(worker_output_file);

        std::string line;
        std::getline(in, line);  // Skip header

        int max_cluster_id = -1;
        while (std::getline(in, line)) {
            std::stringstream ss(line);
            std::string node_str, cluster_str;
            std::getline(ss, node_str, ',');
            std::getline(ss, cluster_str, ',');

            int node_id = std::stoi(node_str);
            int cluster_id = std::stoi(cluster_str);
            max_cluster_id = std::max(max_cluster_id, cluster_id);

            out << node_id << "," << (cluster_id + start_cluster_id) << "\n";
        }

        in.close();
        out.flush();

        start_cluster_id += (max_cluster_id + 1);
        logger.info("Scanned worker " + std::to_string(worker_rank) + " output.");
    }

    logger.info("Program-level output aggregation completed.");
    logger.info("LoadBalancer runtime phase ended");
}

// Estimate the cost of a cluster
float LoadBalancer::getCost(int node_count, int edge_count) {
    float density = (2.0f * edge_count) / (node_count * (node_count - 1));
    return node_count + (1.0f / density);
}