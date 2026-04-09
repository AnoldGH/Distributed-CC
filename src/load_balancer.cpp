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
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
namespace fs = std::filesystem;

// Constructor
LoadBalancer::LoadBalancer(const std::string& method,
                          const std::string& edgelist,
                          const std::string& cluster_file,
                          const std::string& work_dir,
                          const std::string& output_file,
                          int log_level,
                          bool use_rank_0_worker,
                          const std::string& partitioned_clusters_dir,
                          bool partition_only,
                          float min_batch_cost,
                          int drop_cluster_under,
                          bool auto_accept_clique)
    : method(method),
      logger(work_dir + "/logs/load_balancer.log", log_level),
      work_dir(work_dir),
      output_file(output_file),
      use_rank_0_worker(use_rank_0_worker),
      min_batch_cost(min_batch_cost),
      drop_cluster_under(drop_cluster_under),
      auto_accept_clique(auto_accept_clique),
      job_queue(CostCompare{this}) {

    const std::string clusters_dir = work_dir + "/" + "clusters";
    std::string summary_filename = partitioned_clusters_dir + "/summary.csv";

    logger.info("LoadBalancer initialization starting");

    /** Arguments Logs */
    logger.info("Method: " + method);
    logger.info("Edgelist: " + edgelist);
    logger.info("Cluster file: " + cluster_file);
    logger.info("Work dir: " + work_dir);
    logger.info("Output file: " + output_file);
    logger.info("Log level: " + std::to_string(log_level));
    logger.info("Use rank 0 worker: " + std::string(use_rank_0_worker ? "true" : "false"));
    logger.info("Partitioned clusters dir: " + partitioned_clusters_dir);
    logger.info("Partition only: " + std::string(partition_only ? "true" : "false"));
    logger.info("Min batch cost: " + std::to_string(min_batch_cost));
    logger.info("Drop cluster under: " + std::to_string(drop_cluster_under));
    logger.info("Auto accept clique: " + std::string(auto_accept_clique ? "true" : "false"));

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
    if (!load_checkpoint()) // attempt to load existing progress first
        initialize_job_queue(created_clusters);

    // TODO: if we load a checkpoint and discovers that there are no jobs left, we should check if the aggregation is also completed - which means no job will be ran

    logger.info("LoadBalancer initialization complete");

    /** Termination Logs */

    logger.flush(); // flush when the program terminates normally
}

/** Helper methods */
bool is_clique(int node_count, int64_t edge_count) {
    return ((int64_t)node_count * (node_count - 1) / 2) == edge_count;
}

bool is_clique(ClusterInfo& cluster_info) {
    return is_clique(cluster_info.node_count, cluster_info.edge_count);
}

// Bypass a cluster - write it directly to output without processing
void LoadBalancer::bypass_cluster(const ClusterInfo& cluster_info, const std::set<int>& nodes) {
    fs::create_directories(work_dir + "/output");

    std::string filename = work_dir + "/output/bypass.out";
    bool file_exists = fs::exists(filename);

    std::ofstream out(filename, std::ios::app);

    if (!out.is_open()) {
        logger.error("Failed to create bypass output file: " + filename);
        throw std::runtime_error("Failed to create bypass output file: " + filename);
    }

    if (!file_exists) {
        out << "node_id,cluster_id\n";
    }

    for (const int node : nodes) {
        out << node << "," << cluster_info.cluster_id << "\n";
    }
    out.close();

    logger.info("Bypassed cluster " + std::to_string(cluster_info.cluster_id) +
                " (nodes=" + std::to_string(cluster_info.node_count) + ")");
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

    // Read clustering file: node_id -> cluster_id
    logger.debug("Reading clustering file...");
    std::unordered_map<int, int> node_to_cluster;
    std::unordered_map<int, std::set<int>> cluster_to_node;
    std::unordered_map<int, ClusterInfo> clusters;

    auto add_cluster_entry = [&](int node_id, int cluster_id) {
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
    };

    int clustering_lines = 0;
    if (is_binary_cluster(cluster_file)) {
        int fd = open(cluster_file.c_str(), O_RDONLY);
        if (fd < 0) {
            logger.error("Failed to open clustering file: " + cluster_file);
            throw std::runtime_error("Failed to open clustering file: " + cluster_file);
        }
        struct stat st;
        fstat(fd, &st);
        void* mapped = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("mmap failed for clustering file: " + cluster_file);
        }
        madvise(mapped, st.st_size, MADV_SEQUENTIAL | MADV_WILLNEED);

        const char* data = static_cast<const char*>(mapped);
        uint32_t num_entries;
        memcpy(&num_entries, data, sizeof(num_entries));
        const int32_t* pairs = reinterpret_cast<const int32_t*>(data + sizeof(num_entries));
        for (uint32_t i = 0; i < num_entries; ++i) {
            add_cluster_entry(pairs[i * 2], pairs[i * 2 + 1]);
            clustering_lines++;
        }

        munmap(mapped, st.st_size);
        close(fd);
    } else {
        int fd = open(cluster_file.c_str(), O_RDONLY);
        if (fd < 0) {
            logger.error("Failed to open clustering file: " + cluster_file);
            throw std::runtime_error("Failed to open clustering file: " + cluster_file);
        }
        struct stat st;
        fstat(fd, &st);
        void* mapped = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("mmap failed for clustering file: " + cluster_file);
        }
        madvise(mapped, st.st_size, MADV_SEQUENTIAL | MADV_WILLNEED);

        const char* data = static_cast<const char*>(mapped);
        const char* end = data + st.st_size;
        char cluster_delimiter = get_delimiter(cluster_file);

        // Skip header line
        while (data < end && *data != '\n') ++data;
        if (data < end) ++data;

        while (data < end) {
            // Parse node_id
            int node_id = 0;
            bool neg = false;
            if (data < end && *data == '-') { neg = true; ++data; }
            while (data < end && *data >= '0' && *data <= '9') {
                node_id = node_id * 10 + (*data - '0');
                ++data;
            }
            if (neg) node_id = -node_id;
            if (data < end && *data == cluster_delimiter) ++data;

            // Parse cluster_id
            int cluster_id = 0;
            neg = false;
            if (data < end && *data == '-') { neg = true; ++data; }
            while (data < end && *data >= '0' && *data <= '9') {
                cluster_id = cluster_id * 10 + (*data - '0');
                ++data;
            }
            if (neg) cluster_id = -cluster_id;

            // Skip to next line
            while (data < end && *data != '\n') ++data;
            if (data < end) ++data;

            add_cluster_entry(node_id, cluster_id);
            clustering_lines++;
        }

        munmap(mapped, st.st_size);
        close(fd);
    }
    logger.debug("Read " + std::to_string(clustering_lines) + " nodes in " +
                std::to_string(clusters.size()) + " clusters");

    // Create storage for edges per cluster
    std::unordered_map<int, std::vector<std::pair<int, int>>> cluster_edges;

    // Read edgelist and partition edges
    logger.debug("Reading edgelist file...");
    int64_t total_edges = 0;
    int64_t intra_cluster_edges = 0;

    auto add_edge = [&](int source, int target) {
        total_edges++;
        if (node_to_cluster.find(source) != node_to_cluster.end() &&
            node_to_cluster.find(target) != node_to_cluster.end()) {
            int source_cluster = node_to_cluster[source];
            int target_cluster = node_to_cluster[target];
            if (source_cluster == target_cluster) {
                cluster_edges[source_cluster].emplace_back(source, target);
                intra_cluster_edges++;
            }
        }
    };

    if (is_binary_edgelist(edgelist)) {
        int fd = open(edgelist.c_str(), O_RDONLY);
        if (fd < 0) {
            logger.error("Failed to open edgelist file: " + edgelist);
            throw std::runtime_error("Failed to open edgelist file: " + edgelist);
        }
        struct stat st;
        fstat(fd, &st);
        void* mapped = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("mmap failed for edgelist file: " + edgelist);
        }
        madvise(mapped, st.st_size, MADV_SEQUENTIAL | MADV_WILLNEED);

        const char* data = static_cast<const char*>(mapped);
        uint64_t num_edges;
        memcpy(&num_edges, data, sizeof(num_edges));
        const int32_t* pairs = reinterpret_cast<const int32_t*>(data + sizeof(num_edges));
        for (uint64_t i = 0; i < num_edges; ++i) {
            add_edge(pairs[i * 2], pairs[i * 2 + 1]);
        }

        munmap(mapped, st.st_size);
        close(fd);
    } else {
        int fd = open(edgelist.c_str(), O_RDONLY);
        if (fd < 0) {
            logger.error("Failed to open edgelist file: " + edgelist);
            throw std::runtime_error("Failed to open edgelist file: " + edgelist);
        }
        struct stat st;
        fstat(fd, &st);
        void* mapped = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("mmap failed for edgelist file: " + edgelist);
        }
        madvise(mapped, st.st_size, MADV_SEQUENTIAL | MADV_WILLNEED);

        const char* data = static_cast<const char*>(mapped);
        const char* end = data + st.st_size;
        char edgelist_delimiter = get_delimiter(edgelist);

        // Skip header line
        while (data < end && *data != '\n') ++data;
        if (data < end) ++data;

        while (data < end) {
            // Parse source
            int source = 0;
            bool neg = false;
            if (data < end && *data == '-') { neg = true; ++data; }
            while (data < end && *data >= '0' && *data <= '9') {
                source = source * 10 + (*data - '0');
                ++data;
            }
            if (neg) source = -source;
            if (data < end && *data == edgelist_delimiter) ++data;

            // Parse target
            int target = 0;
            neg = false;
            if (data < end && *data == '-') { neg = true; ++data; }
            while (data < end && *data >= '0' && *data <= '9') {
                target = target * 10 + (*data - '0');
                ++data;
            }
            if (neg) target = -target;

            // Skip to next line
            while (data < end && *data != '\n') ++data;
            if (data < end) ++data;

            add_edge(source, target);
        }

        munmap(mapped, st.st_size);
        close(fd);
    }
    logger.debug("Read " + std::to_string(total_edges) + " edges, " +
                std::to_string(intra_cluster_edges) + " intra-cluster edges");

    // Write out cluster files to output_dir
    logger.info("Writing cluster files to " + output_dir);
    int files_written = 0;

    float accumulated_cost = 0;
    ClusterInfo batch_head_cluster_info{};
    std::string output_edgelist;
    std::string output_cluster_file;
    std::vector<std::pair<int, int>> batch_edges;
    std::vector<std::pair<int, int>> batch_cluster_entries;  // (node_id, cluster_id)
    for (auto& [cluster_id, cluster_info] : clusters) {
        int64_t edge_count = cluster_edges[cluster_id].size();
        cluster_info.edge_count = edge_count;

        // Clique bypass
        if (auto_accept_clique && is_clique(cluster_info)) {
            bypass_cluster(cluster_info, cluster_to_node[cluster_id]);
            continue;
        }

        if (edge_count == 0) {
            logger.debug("Dropping cluster " + std::to_string(cluster_id) +
                " (no edges, nodes=" + std::to_string(cluster_info.node_count) + ")");
            continue;
        }
        if (cluster_info.node_count < drop_cluster_under) {
            logger.debug("Dropping cluster " + std::to_string(cluster_id) +
                " (nodes=" + std::to_string(cluster_info.node_count) +
                " < drop_cluster_under=" + std::to_string(drop_cluster_under) + ")");
            continue;
        }

        // Batch very small clusters together, by min_batch_size
        if (accumulated_cost == 0) {    // make new file only when we start to form a batch
            batch_head_cluster_info = ClusterInfo{};
            batch_head_cluster_info.cluster_id = cluster_id;    // form new batch head cluster

            output_edgelist = output_dir + "/" + std::to_string(cluster_id) + ".bedgelist";
            output_cluster_file = output_dir + "/" + std::to_string(cluster_id) + ".bcluster";

            batch_edges.clear();
            batch_cluster_entries.clear();
        }

        accumulated_cost += get_cost(cluster_info);
        batch_head_cluster_info.node_count += cluster_info.node_count;
        batch_head_cluster_info.edge_count += cluster_info.edge_count;

        // Accumulate edges and cluster entries for this cluster
        batch_edges.insert(batch_edges.end(),
                           cluster_edges[cluster_id].begin(),
                           cluster_edges[cluster_id].end());
        for (int node : cluster_to_node[cluster_id]) {
            batch_cluster_entries.emplace_back(node, cluster_id);
        }

        // Check if batch formation is completed
        if (accumulated_cost >= this->min_batch_cost) {
            accumulated_cost = 0;
            write_binary_edgelist(output_edgelist, batch_edges);
            write_binary_cluster(output_cluster_file, batch_cluster_entries);
            ++files_written;
            created_clusters.emplace_back(batch_head_cluster_info);
        }
    }

    // Flush remaining batch that didn't reach min_batch_cost
    if (accumulated_cost > 0) {
        write_binary_edgelist(output_edgelist, batch_edges);
        write_binary_cluster(output_cluster_file, batch_cluster_entries);
        ++files_written;
        created_clusters.emplace_back(batch_head_cluster_info);
    }

    // Write summary file for quicker load
    std::string summary_filename = output_dir + "/summary.csv";
    std::ofstream out_summary(summary_filename);
    out_summary << "cluster_id,node_count,edge_count\n";
    for (const auto& cluster : created_clusters)
        out_summary << cluster.cluster_id << "," << cluster.node_count << "," << cluster.edge_count << "\n";

    logger.info("partition_clustering completed successfully. " +
               std::to_string(clusters.size()) + " clusters written to " +
               std::to_string(files_written) + " batched files");

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
        cluster_info.edge_count = std::stoll(edge_count);

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

    for (const auto& c : created_clusters) {
        job_queue.push(c);
        job_queue_active++;
    }

    logger.info("Job queue initialized with " + std::to_string(job_queue_active) + " unprocessed clusters.");
}

// Pop clusters from job_queue (skipping dropped entries), batch up to min_batch_cost,
// assign to worker_rank via MPI. Returns true if work was assigned.
bool LoadBalancer::assign_batch(int worker_rank) {
    std::vector<AssignedCluster> assign_clusters;
    float batch_cost = 0;

    while (!job_queue.empty() && batch_cost < min_batch_cost) {
        ClusterInfo cluster_info = job_queue.top();
        job_queue.pop();

        // Lazy deletion: skip dropped clusters
        if (dropped_clusters.erase(cluster_info.cluster_id)) {
            continue;
        }

        job_queue_active--;
        int is_yielded = yield_to_root.count(cluster_info.cluster_id) ? 1 : 0;
        assign_clusters.push_back({cluster_info.cluster_id, is_yielded});
        in_flight_clusters[cluster_info.cluster_id] = cluster_info;

        float cost = get_cost(cluster_info);
        batch_cost += cost;

        logger.info("Assigning cluster " + std::to_string(cluster_info.cluster_id) +
            " (nodes: " + std::to_string(cluster_info.node_count) +
            ", edges: " + std::to_string(cluster_info.edge_count) +
            ", estimated cost: " + std::to_string(cost) +
            ", yielded: " + std::to_string(is_yielded) + ")" +
            " to worker " + std::to_string(worker_rank) +
            " (" + std::to_string(job_queue_active) + " jobs remaining)");

        std::ofstream pending_out(work_dir + "/" + "pending" + "/" + std::to_string(cluster_info.cluster_id));
    }

    if (assign_clusters.empty()) return false;

    // Send as flat int array: [cluster_id, is_yielded, cluster_id, is_yielded, ...]
    MPI_Send(assign_clusters.data(), assign_clusters.size() * 2, MPI_INT, worker_rank,
             to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD);
    return true;
}

// Runtime phase: Distribute jobs to workers
void LoadBalancer::run() {
    logger.info("LoadBalancer runtime phase started");

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int num_workers = use_rank_0_worker ? size : size - 1;

    logger.info("Managing " + std::to_string(num_workers) + " workers");

    int active_workers = num_workers;

    // Workers whose WORK_REQUEST is deferred because the queue is empty
    // but in-flight clusters may still yield new work.
    std::vector<int> pending_work_requests;

    while (active_workers > 0) {
        // Listen to incoming messages from workers
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        int worker_rank = status.MPI_SOURCE;
        MessageType message_type = static_cast<MessageType>(status.MPI_TAG);

        // Worker report: 3-int message, handle separately
        if (message_type == MessageType::WORKER_REPORT) {
            int report_data[3];
            MPI_Recv(report_data, 3, MPI_INT, worker_rank, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            worker_reports[worker_rank] = {report_data[0], report_data[1], report_data[2]};
            continue;
        }

        // Yield report: {parent_id, child_id, node_count, edge_count} sent as raw bytes
        if (message_type == MessageType::YIELD_REPORT) {
            struct { int parent_id; int child_id; int node_count; int64_t edge_count; } yield_data;
            MPI_Recv(&yield_data, sizeof(yield_data), MPI_BYTE, worker_rank, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            int parent_id = yield_data.parent_id;
            int child_id = yield_data.child_id;
            int node_count = yield_data.node_count;
            int64_t edge_count = yield_data.edge_count;

            // Ensure parent exists in yield_tree (may not if WORK_DONE hasn't arrived yet).
            // Create as root (parent_id=-1) if this is an original cluster.
            if (!yield_tree.count(parent_id)) {
                yield_tree[parent_id] = {parent_id, -1, false, false, 0, 0, {}};
            }

            YieldNode& parent_node = yield_tree[parent_id];

            // If any ancestor is aborted, discard this child.
            // Count it as resolved so the aborted ancestor can eventually resolve.
            if (is_ancestor_aborted(parent_id)) {
                logger.info("Discarding YIELD_REPORT for aborted ancestor chain"
                    " (parent=" + std::to_string(parent_id) +
                    ", child=" + std::to_string(child_id) + ")");
                parent_node.resolved_children++;
                try_resolve(parent_id, pending_work_requests);
                continue;
            }

            // Create child node in yield_tree
            yield_tree[child_id] = {child_id, parent_id, false, false, 0, 0, {}};
            parent_node.children.push_back(child_id);

            // Track yielded cluster → root mapping (persistent across resolution)
            int root = parent_id;
            while (yield_tree.count(root) && yield_tree[root].parent_id != -1)
                root = yield_tree[root].parent_id;
            yield_to_root[child_id] = root;

            // Add child to queue
            ClusterInfo yielded = {child_id, node_count, edge_count};
            job_queue.push(yielded);
            job_queue_active++;

            logger.info("Yield: parent=" + std::to_string(parent_id) +
                " child=" + std::to_string(child_id) +
                " (nodes=" + std::to_string(node_count) +
                ", edges=" + std::to_string(edge_count) +
                ", cost=" + std::to_string(get_cost(node_count, edge_count)) + ")" +
                " resolved=" + std::to_string(parent_node.resolved_children) +
                "/" + std::to_string(parent_node.expected_yields) +
                " (" + std::to_string(job_queue_active) + " jobs in queue)");

            // Service any workers that were waiting for work
            while (!pending_work_requests.empty()) {
                int waiting_rank = pending_work_requests.back();
                if (assign_batch(waiting_rank)) {
                    pending_work_requests.pop_back();
                } else {
                    break;  // queue effectively empty
                }
            }

            continue;
        }

        if (message_type == MessageType::WORK_REQUEST) {
            int message;
            MPI_Recv(&message, 1, MPI_INT, worker_rank, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (assign_batch(worker_rank)) {
                // Work assigned
            } else if (!in_flight_clusters.empty()) {
                // Queue is effectively empty but in-flight clusters may still yield new work.
                // Defer this worker's request — respond when work becomes available
                // or when all in-flight clusters complete.
                pending_work_requests.push_back(worker_rank);
                logger.info("Worker " + std::to_string(worker_rank) +
                    " is waiting for work (" + std::to_string(in_flight_clusters.size()) +
                    " clusters still in flight)");
            } else {
                // Queue empty and nothing in flight — truly done
                int no_more = NO_MORE_JOBS;
                MPI_Send(&no_more, 1, MPI_INT, worker_rank,
                         to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD);
                logger.info("Sending termination signal to worker " + std::to_string(worker_rank));
            }
        } else if (message_type == MessageType::WORK_DONE || message_type == MessageType::WORK_ABORTED) {
            // Completion message: [cluster_id, yield_count]
            // yield_count is the number of sub-clusters directly yielded during processing.
            // All YIELD_REPORTs for those sub-clusters are guaranteed sent before this message
            // on the worker side, but may arrive later due to MPI cross-tag reordering.
            int done_data[2];
            MPI_Recv(done_data, 2, MPI_INT, worker_rank, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            int cluster_id = done_data[0];
            int yield_count = done_data[1];
            bool is_aborted = (message_type == MessageType::WORK_ABORTED);

            logger.info("Worker " + std::to_string(worker_rank) +
                (is_aborted ? " aborted" : " completed") + " cluster " +
                std::to_string(cluster_id) + " (yield_count=" + std::to_string(yield_count) + ")");

            handle_cluster_completion(cluster_id, pending_work_requests, yield_count, is_aborted);
        } else if (message_type == MessageType::AGGREGATE_DONE) {
            int message;
            MPI_Recv(&message, 1, MPI_INT, worker_rank, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            logger.info("Worker " + std::to_string(worker_rank) + " completed worker-level aggregation.");
            --active_workers;
        }

        logger.flush(); // flush per assignment
    }

    // Aggregation phase: combine outputs from all workers
    int next_cluster_id = 0;
    std::string clusters_output_dir = work_dir + "/output/";

    fs::remove(output_file);
    std::ofstream out(output_file, std::ios::app);
    out << "node_id,cluster_id\n";

    // Helper lambda for aggregating a single output file
    auto aggregate_file = [&](const std::string& filepath, const std::string& source_name) {
        std::ifstream in(filepath);
        if (!in.is_open()) return;

        std::string line;
        std::getline(in, line);  // Skip header

        std::unordered_map<int, int> cluster_mapping;  // per-file mapping

        while (std::getline(in, line)) {
            std::stringstream ss(line);
            std::string node_str, cluster_str;
            std::getline(ss, node_str, ',');
            std::getline(ss, cluster_str, ',');

            int node_id = std::stoi(node_str);
            int cluster_id = std::stoi(cluster_str);

            // Assign new global ID if this cluster_id hasn't been seen in this file's output
            if (cluster_mapping.find(cluster_id) == cluster_mapping.end()) {
                cluster_mapping[cluster_id] = next_cluster_id++;
            }

            out << node_id << "," << cluster_mapping[cluster_id] << "\n";
        }

        in.close();
        out.flush();

        logger.info("Scanned " + source_name + " output.");
    };

    // Aggregate bypass file
    std::string bypass_file = clusters_output_dir + "bypass.out";
    aggregate_file(bypass_file, "bypass");

    // Aggregate worker outputs
    int first_worker = use_rank_0_worker ? 0 : 1;
    for (int worker_rank = first_worker; worker_rank < size; ++worker_rank) {
        std::string worker_output_file = clusters_output_dir + "worker_" + std::to_string(worker_rank) + ".out";
        aggregate_file(worker_output_file, "worker " + std::to_string(worker_rank));
    }

    // Aggregate yielded cluster outputs (only for non-aborted roots)
    std::string yield_dir = work_dir + "/yield";
    if (fs::exists(yield_dir)) {
        for (const auto& entry : fs::directory_iterator(yield_dir)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".output") continue;
            int cluster_id = std::stoi(entry.path().stem());
            // Skip outputs belonging to aborted root clusters
            if (yield_to_root.count(cluster_id) && aborted_clusters.count(yield_to_root[cluster_id])) {
                logger.info("Skipping yield output for cluster " + std::to_string(cluster_id) +
                    " (root " + std::to_string(yield_to_root[cluster_id]) + " aborted)");
                continue;
            }
            // Root cluster that yielded: check if the root itself is aborted
            if (aborted_clusters.count(cluster_id)) {
                logger.info("Skipping yield output for aborted root cluster " + std::to_string(cluster_id));
                continue;
            }
            aggregate_file(entry.path().string(), "yield " + std::to_string(cluster_id));
        }
    }

    logger.info("Program-level output aggregation completed.");

    // Log worker report summary
    if (worker_reports.empty()) {
        logger.info("Worker report summary: no reports received (worker reporting may be disabled)");
    } else {
        int total_oom = 0, total_timeout = 0, global_peak_mb = 0;
        for (const auto& [rank, r] : worker_reports) {
            total_oom += r.oom_count;
            total_timeout += r.timeout_count;
            if (r.peak_memory_mb > global_peak_mb) global_peak_mb = r.peak_memory_mb;
        }
        logger.info("Worker report summary: " + std::to_string(total_oom) + " OOM kills, "
                    + std::to_string(total_timeout) + " timeouts, peak cluster memory " + std::to_string(global_peak_mb) + " MB");
    }

    logger.info("LoadBalancer runtime phase ended");

    std::string checkpoint_file = work_dir + "/checkpoint.csv";
    if (in_flight_clusters.size() == 0) {   // all clusters are processed
        if (fs::exists(checkpoint_file)) {
            fs::remove(checkpoint_file);
            logger.info("Checkpoint file removed");
        }
    } else {    // some clusters failed to be processed
        logger.info("Saving checkpoint due to unfinished jobs: " + std::to_string(in_flight_clusters.size()) + " jobs remaining");
        save_checkpoint();
    }

}

// Shared completion logic for WORK_DONE and WORK_ABORTED.
// Uses tree-based yield tracking: each node stays in yield_tree until fully resolved
// (work_done, all yields received, all children resolved), then cascades upward.
bool LoadBalancer::handle_cluster_completion(int cluster_id, std::vector<int>& pending_work_requests, int yield_count, bool aborted) {
    // Clean up pending file
    try {
        fs::remove(work_dir + "/" + "pending" + "/" + std::to_string(cluster_id));
    } catch(const std::exception& e) {
        logger.error("No pending file found for cluster " + std::to_string(cluster_id));
    }

    // Simple case: no yields and not already in yield_tree (never yielded, never was yielded)
    if (yield_count == 0 && !yield_tree.count(cluster_id)) {
        if (aborted) {
            aborted_clusters[cluster_id] = in_flight_clusters[cluster_id];
            logger.info("Cluster " + std::to_string(cluster_id) + " aborted (simple, no yields)");
        } else {
            logger.info("Cluster " + std::to_string(cluster_id) + " completed (simple, no yields)");
        }
        in_flight_clusters.erase(cluster_id);

        // Deferred termination check
        if (job_queue_active == 0 && in_flight_clusters.empty() && !pending_work_requests.empty()) {
            for (int waiting_rank : pending_work_requests) {
                int no_more = NO_MORE_JOBS;
                MPI_Send(&no_more, 1, MPI_INT, waiting_rank,
                         to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD);
                logger.info("Sending termination signal to deferred worker " + std::to_string(waiting_rank));
            }
            pending_work_requests.clear();
        }
        return in_flight_clusters.empty();
    }

    // Ensure node exists in yield_tree (may already exist if YIELD_REPORTs arrived first)
    if (!yield_tree.count(cluster_id)) {
        yield_tree[cluster_id] = {cluster_id, -1, false, false, 0, 0, {}};
    }

    YieldNode& node = yield_tree[cluster_id];
    node.work_done = true;
    node.expected_yields = yield_count;
    node.aborted = aborted;

    // If this is a child node (not a root), remove from in_flight_clusters.
    // The tree tracks it; only roots remain in in_flight.
    if (node.parent_id != -1) {
        in_flight_clusters.erase(cluster_id);
    }

    logger.info("Cluster " + std::to_string(cluster_id) +
        (aborted ? " aborted" : " done") +
        " (parent=" + std::to_string(node.parent_id) +
        ", expected_yields=" + std::to_string(node.expected_yields) +
        ", resolved_children=" + std::to_string(node.resolved_children) + ")");

    // If aborted, sweep descendants: remove unprocessed children from queue,
    // mark in-flight children for discard
    if (aborted) {
        sweep_aborted_descendants(cluster_id);

        // Find the root of this cluster's yield tree and mark it as aborted
        int root = cluster_id;
        while (yield_tree.count(root) && yield_tree[root].parent_id != -1) {
            root = yield_tree[root].parent_id;
        }
        if (!aborted_clusters.count(root) && in_flight_clusters.count(root)) {
            aborted_clusters[root] = in_flight_clusters[root];
            logger.info("Root cluster " + std::to_string(root) +
                " marked as aborted (triggered by descendant " + std::to_string(cluster_id) + ")");
        }
    }

    // Attempt resolution (may cascade upward)
    try_resolve(cluster_id, pending_work_requests);

    return in_flight_clusters.empty();
}

// Check resolution condition and cascade upward.
// A node resolves when: work_done && resolved_children == expected_yields
void LoadBalancer::try_resolve(int cluster_id, std::vector<int>& pending_work_requests) {
    if (!yield_tree.count(cluster_id)) return;

    YieldNode& node = yield_tree[cluster_id];

    if (!node.work_done) return;
    if (node.resolved_children < node.expected_yields) return;

    // Node is fully resolved
    int parent_id = node.parent_id;
    logger.info("Yield node " + std::to_string(cluster_id) + " fully resolved"
        " (parent=" + std::to_string(parent_id) + ")");

    erase_subtree(cluster_id);

    if (parent_id == -1) {
        // Root resolved — remove from in_flight
        in_flight_clusters.erase(cluster_id);
        logger.info("Root cluster " + std::to_string(cluster_id) + " fully complete (all descendants resolved)");

        // Deferred termination check
        if (job_queue_active == 0 && in_flight_clusters.empty() && !pending_work_requests.empty()) {
            for (int waiting_rank : pending_work_requests) {
                int no_more = NO_MORE_JOBS;
                MPI_Send(&no_more, 1, MPI_INT, waiting_rank,
                         to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD);
                logger.info("Sending termination signal to deferred worker " + std::to_string(waiting_rank));
            }
            pending_work_requests.clear();
        }
    } else {
        // Child resolved — increment parent's resolved count and try to resolve parent
        if (yield_tree.count(parent_id)) {
            yield_tree[parent_id].resolved_children++;
            logger.info("Child " + std::to_string(cluster_id) + " resolved under parent " +
                std::to_string(parent_id) + " (" +
                std::to_string(yield_tree[parent_id].resolved_children) + "/" +
                std::to_string(yield_tree[parent_id].expected_yields) + " resolved)");
            try_resolve(parent_id, pending_work_requests);
        }
    }
}

// Recursively remove a node and all its descendants from yield_tree.
void LoadBalancer::erase_subtree(int cluster_id) {
    if (!yield_tree.count(cluster_id)) return;

    // Copy children list before erasing (iterator invalidation)
    std::vector<int> children = yield_tree[cluster_id].children;
    yield_tree.erase(cluster_id);

    for (int child_id : children) {
        erase_subtree(child_id);
    }
}

// Walk the parent chain to check if any ancestor (including self) has been aborted.
bool LoadBalancer::is_ancestor_aborted(int cluster_id) {
    int current = cluster_id;
    while (yield_tree.count(current)) {
        if (yield_tree[current].aborted) return true;
        if (yield_tree[current].parent_id == -1) break;
        current = yield_tree[current].parent_id;
    }
    return false;
}

// For an aborted node: mark its unprocessed children for lazy deletion from job_queue.
// In-flight children will be resolved normally (their WORK_DONE/WORK_ABORTED will arrive);
// the aborted flag on the ancestor prevents new grandchildren from being enqueued.
void LoadBalancer::sweep_aborted_descendants(int cluster_id) {
    if (!yield_tree.count(cluster_id)) return;

    YieldNode& node = yield_tree[cluster_id];
    int swept = 0;

    // Copy children list since we modify it
    std::vector<int> children_copy = node.children;
    for (int child_id : children_copy) {
        if (!yield_tree.count(child_id)) continue;
        YieldNode& child = yield_tree[child_id];

        // Only drop children that are still in the queue (not yet assigned to a worker)
        if (!child.work_done && !in_flight_clusters.count(child_id)) {
            dropped_clusters.insert(child_id);
            job_queue_active--;
            ++swept;

            node.resolved_children++;
            node.children.erase(std::remove(node.children.begin(), node.children.end(), child_id), node.children.end());
            yield_tree.erase(child_id);
        }
    }

    if (swept > 0) {
        logger.info("Swept " + std::to_string(swept) + " unprocessed children of aborted cluster " +
            std::to_string(cluster_id));
    }
}

// Estimate the cost of a cluster given node_count and edge_count
float LoadBalancer::get_cost(int node_count, int64_t edge_count) {
    double density = (2.0 * edge_count) / ((double)node_count * (node_count - 1));
    return node_count + (1.0f / density);
}

// Estimate the cost of a cluster given cluster_info
float LoadBalancer::get_cost(const ClusterInfo& cluster_info) {
    return get_cost(cluster_info.node_count, cluster_info.edge_count);
}

// Save checkpoint - usually due to SIGTERM
// Yielded children are ephemeral and not checkpointed. Their root ancestors
// are saved instead, so on recovery the root is re-processed from scratch.
void LoadBalancer::save_checkpoint() {
    std::string path = work_dir + "/checkpoint.csv";
    std::string tmp_path = path + ".tmp";   // tmp file containing incomplete results
    std::ofstream out(tmp_path);
    out << "cluster_id,node_count,edge_count\n";
    // Drain job_queue (save_checkpoint is called at termination, queue won't be reused)
    int queued = 0;
    while (!job_queue.empty()) {
        ClusterInfo c = job_queue.top();
        job_queue.pop();
        if (dropped_clusters.erase(c.cluster_id)) continue;
        // Skip yielded children (ephemeral; their root will be re-processed on recovery)
        if (yield_tree.count(c.cluster_id) && yield_tree.at(c.cluster_id).parent_id != -1) continue;
        out << c.cluster_id << "," << c.node_count << "," << c.edge_count << "\n";
        ++queued;
    }
    for (const auto& [k, c] : in_flight_clusters) {
        // in_flight only contains roots (children are removed from in_flight on creation)
        out << c.cluster_id << "," << c.node_count << "," << c.edge_count << "\n";
    }
    for (const auto& [k, c] : aborted_clusters) {
        // Aborted root clusters whose yield trees have already resolved and been erased
        // (so they are no longer in in_flight_clusters). They must be re-processed on recovery.
        if (!in_flight_clusters.count(k)) {
            out << c.cluster_id << "," << c.node_count << "," << c.edge_count << "\n";
        }
    }

    out.close();
    fs::rename(tmp_path, path);
    logger.info("Checkpoint saved: " + std::to_string(queued) + " queued, "
                + std::to_string(in_flight_clusters.size()) + " in-flight"
                + ", " + std::to_string(aborted_clusters.size()) + " aborted");
    logger.flush();  // Ensure log is written before the program is terminated
}

// Load checkpoint
bool LoadBalancer::load_checkpoint() {
    std::string path = work_dir + "/checkpoint.csv";
    if (!fs::exists(path)) return false;

    logger.info("Resuming from checkpoint: " + path);

    // Clean up stale yield files from previous run (ephemeral, not recoverable)
    std::string yield_dir = work_dir + "/yield";
    if (fs::exists(yield_dir)) {
        fs::remove_all(yield_dir);
        logger.info("Cleaned up stale yield directory");
    }

    // Clear queue state
    while (!job_queue.empty()) job_queue.pop();
    job_queue_active = 0;
    dropped_clusters.clear();
    aborted_clusters.clear();
    yield_tree.clear();  // yield tree is ephemeral, not recoverable from checkpoint
    yield_to_root.clear();

    std::ifstream in(path);
    std::string line;
    std::getline(in, line);
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string cid, nc, ec;
        std::getline(ss, cid, ',');
        std::getline(ss, nc, ',');
        std::getline(ss, ec, ',');
        job_queue.push({std::stoi(cid), std::stoi(nc), std::stoll(ec)});
        job_queue_active++;
    }

    logger.info("Checkpoint loaded: " + std::to_string(job_queue_active) + " clusters to process");
    return true;
}
