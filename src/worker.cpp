#include <mpi.h>
#include <worker.hpp>
#include <constants.hpp>
#include <cm.h>
#include <mincut_only.h>
#include <constrained.h>

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <algorithm>
#include <unordered_map>

namespace fs = std::filesystem;

// Constructor
Worker::Worker(const std::string& method, Logger& logger, const std::string& work_dir,
               const std::string& clusters_dir,
               const std::string& algorithm, double clustering_parameter,
               int log_level, const std::string& connectedness_criterion,
               const std::string& mincut_type, bool prune,
               int time_limit_per_cluster,
               int report_interval,
               int num_processors,
               int yield_node_threshold)
    : method(method), logger(logger), work_dir(work_dir), clusters_dir(clusters_dir),
      algorithm(algorithm), clustering_parameter(clustering_parameter),
      log_level(log_level), connectedness_criterion(connectedness_criterion),
      mincut_type(mincut_type), prune(prune),
      time_limit_per_cluster(time_limit_per_cluster),
      report_interval(report_interval),
      num_processors(num_processors),
      yield_node_threshold(yield_node_threshold) {
    // Use rank-based offset for yield IDs to avoid collisions between workers
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    yield_id_counter = rank * 10000000; // TODO: find a better way to name yielded sub-clusters
}

// Main run function
void Worker::run() {
    logger.info("Worker runtime phase started");

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Create needed directory
    fs::create_directories(work_dir + "/output/worker_" + std::to_string(rank) + "/");
    fs::create_directories(work_dir + "/history/worker_" + std::to_string(rank) + "/");

    // Worker main loop
    int request_count = 0;
    while (true) {
        // Send work request to load balancer (rank 0)
        logger.info("Requesting cluster from the load balancer");
        int request_msg = to_int(MessageType::WORK_REQUEST);
        MPI_Send(&request_msg, 1, MPI_INT, 0, to_int(MessageType::WORK_REQUEST), MPI_COMM_WORLD);

        // Send cumulative report periodically (best-effort)
        if (report_interval > 0 && ++request_count % report_interval == 0) {
            int report_data[3] = {report.oom_count, report.timeout_count, report.peak_memory_mb};
            MPI_Send(report_data, 3, MPI_INT, 0, to_int(MessageType::WORKER_REPORT), MPI_COMM_WORLD);
        }

        // Receive cluster IDs from load balancer
        MPI_Status status;
        MPI_Probe(0, to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD, &status);

        // Learn how many assigned clusters there are
        int count;
        MPI_Get_count(&status, MPI_INT, &count);

        // Resize vector and receive assigned clusters
        std::vector<int> assigned_clusters(count);
        MPI_Recv(assigned_clusters.data(), count, MPI_INT, 0, to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Check for termination signal
        if (assigned_clusters[0] == NO_MORE_JOBS) {
            logger.info("No more jobs available, terminating worker");
            break;
        }

        for (const auto& cluster : assigned_clusters) {
            logger.info("Received cluster " + std::to_string(cluster));

            // Process the cluster
            auto [success, yield_count] = process_cluster(cluster);

            // Send completion status: [cluster_id, yield_count]
            // yield_count lets the LB know whether YIELD_REPORTs are in transit
            // (they are always fully sent before this message, but may arrive out of order
            // due to MPI cross-tag reordering).
            MessageType status_type = success ? MessageType::WORK_DONE : MessageType::WORK_ABORTED;
            int done_data[2] = {cluster, yield_count};
            MPI_Send(done_data, 2, MPI_INT, 0, to_int(status_type), MPI_COMM_WORLD);

            if (success) {
                logger.info("Completed cluster " + std::to_string(cluster) +
                    " (yield_count=" + std::to_string(yield_count) + ")");
            } else {
                logger.info("Aborted cluster " + std::to_string(cluster) +
                    " (yield_count=" + std::to_string(yield_count) + ")");
            }
        }
    }

    // Send final report before aggregation
    if (report_interval > 0) {
        int report_data[3] = {report.oom_count, report.timeout_count, report.peak_memory_mb};
        MPI_Send(report_data, 3, MPI_INT, 0, to_int(MessageType::WORKER_REPORT), MPI_COMM_WORLD);
    }

    // Aggregation phase: combine all output files into one worker-specific file
    logger.info("Starting output aggregation");

    std::string output_dir = work_dir + "/output/";

    // The worker tries to aggregate for other workers (in case total worker count changes)
    int size, delegating_worker = rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::string worker_subdir = output_dir + "worker_" + std::to_string(delegating_worker) + "/";
    std::string worker_output_file = output_dir + "worker_" + std::to_string(delegating_worker) + ".out";

    while (fs::exists(worker_subdir)) {
        // Aggregation logic
        std::ofstream out(worker_output_file);
        out << "node_id,cluster_id\n";  // Header

        int next_cluster_id = 0;

        // Iterate over all files in the worker-specific subdirectory
        for (const auto& entry : fs::directory_iterator(worker_subdir)) {
            if (entry.is_regular_file()) {
                std::string input_file = entry.path().string();
                std::ifstream in(input_file);

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

                    // Assign new global ID if this cluster_id hasn't been seen in this file
                    if (cluster_mapping.find(cluster_id) == cluster_mapping.end()) {
                        cluster_mapping[cluster_id] = next_cluster_id++;
                    }

                    out << node_id << "," << cluster_mapping[cluster_id] << "\n";
                }

                in.close();
            }
        }

        out.close();
        logger.info("Output aggregation complete. Worker output: " + worker_output_file);

        // Attempt to aggregate for the next worker (outside of range)
        delegating_worker += (size - 1);
        worker_subdir = output_dir + "worker_" + std::to_string(delegating_worker) + "/";
        worker_output_file = output_dir + "worker_" + std::to_string(delegating_worker) + ".out";
    }

    // Send AGGREGATE_DONE signal to load balancer
    int aggregate_msg = to_int(MessageType::AGGREGATE_DONE);
    MPI_Send(&aggregate_msg, 1, MPI_INT, 0, to_int(MessageType::AGGREGATE_DONE), MPI_COMM_WORLD);
    logger.info("Sent AGGREGATE_DONE signal to load balancer");

    logger.info("Worker runtime phase ended");
}

// Process a single cluster
std::pair<bool, int> Worker::process_cluster(int cluster_id) {
    // TODO: implement actual cluster processing
    // For now, this is a placeholder that simulates work

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    std::string cluster_edgelist = clusters_dir + "/" + std::to_string(cluster_id) + ".bedgelist";
    if (!std::filesystem::exists(cluster_edgelist)) {
        cluster_edgelist = clusters_dir + "/" + std::to_string(cluster_id) + ".edgelist";
    }
    if (!std::filesystem::exists(cluster_edgelist)) {
        // Yielded clusters live in work_dir/yield/ (ephemeral, not checkpointed)
        cluster_edgelist = work_dir + "/yield/" + std::to_string(cluster_id) + ".bedgelist";
    }
    std::string cluster_clustering_file = clusters_dir + "/" + std::to_string(cluster_id) + ".bcluster";
    if (!std::filesystem::exists(cluster_clustering_file)) {
        cluster_clustering_file = clusters_dir + "/" + std::to_string(cluster_id) + ".cluster";
    }
    if (!std::filesystem::exists(cluster_clustering_file)) {
        cluster_clustering_file = work_dir + "/yield/" + std::to_string(cluster_id) + ".bcluster";
    }
    logger.debug("Processing cluster file: " + cluster_edgelist);

    // Set up yield pipe for this cluster (if yield is enabled)
    // The yield directory is created on-demand by WriteYieldCluster only when a yield actually happens.
    int yield_count = 0;  // number of sub-clusters directly yielded; incremented by yield monitor
    std::string yield_dir;
    int yield_pipe[2] = {-1, -1};
    if (yield_node_threshold > 0) {
        yield_dir = work_dir + "/yield/" + std::to_string(cluster_id);
        if (pipe(yield_pipe) != 0) {
            logger.error("Failed to create yield pipe for cluster " + std::to_string(cluster_id));
            yield_pipe[0] = yield_pipe[1] = -1;
        }
    }

    logger.flush(); // to avoid duplicate logs after fork()

    // Spawn a child process and call CM processing logic on it
    // This is to gracefully handle OOM kills

    int pid = fork();
    if (pid == 0) {  // child process
        // Close read end of yield pipe (child only writes)
        if (yield_pipe[0] >= 0) close(yield_pipe[0]);

        logger.info("Child process starts on cluster " + std::to_string(cluster_id));

        // Output file paths
        std::string output_file = work_dir + "/output/worker_" + std::to_string(rank) + "/" + std::to_string(cluster_id) + ".output";
        std::string history_file = work_dir + "/history/worker_" + std::to_string(rank) + "/" + std::to_string(cluster_id) + ".hist";
        std::string log_file = work_dir + "/logs/clusters/" + std::to_string(cluster_id) + ".log"; // TODO: since CC was built as a standalone app with its own logging system, we have to use a different file. In the future we should try to integrate the two systems into one unified logging system.

        // CM or WCC
        ConstrainedClustering* cc;
        if (method == "CM") {
            cc = new CM(cluster_edgelist, this->algorithm, this->clustering_parameter, cluster_clustering_file, this->num_processors, output_file, log_file, history_file, this->log_level, this->connectedness_criterion, this->prune, this->mincut_type);
        } else if (method == "WCC") {
            cc = new MincutOnly(cluster_edgelist, cluster_clustering_file, this->num_processors, output_file, log_file, this->log_level, this->connectedness_criterion, this->mincut_type);
        }

        // Configure yield if enabled
        if (yield_node_threshold > 0) {
            cc->set_yield_config(yield_dir, yield_node_threshold, yield_pipe[1]);
        }

        cc->main();  // run constrained clustering

        if (yield_pipe[1] >= 0) close(yield_pipe[1]);
        logger.info("Child process finishes cluster " + std::to_string(cluster_id));
        _exit(0);   // use _exit to avoid static destructor issues in forked process
    } else if (pid > 0) {    // control process
        // Close write end of yield pipe (parent only reads)
        if (yield_pipe[1] >= 0) close(yield_pipe[1]);

        // Start yield monitor thread: reads yield notifications from the pipe
        // and sends YIELD_REPORT to LB in real-time while the child is running.
        std::thread yield_monitor;
        if (yield_pipe[0] >= 0) {
            yield_monitor = std::thread([&, pipe_read_fd = yield_pipe[0]]() {
                int32_t record[3];  // [yield_id, node_count, edge_count]
                while (true) {
                    // Read exactly 12 bytes (one yield record)
                    ssize_t total = 0;
                    char* buf = reinterpret_cast<char*>(record);
                    while (total < (ssize_t)sizeof(record)) {
                        ssize_t n = ::read(pipe_read_fd, buf + total, sizeof(record) - total);
                        if (n <= 0) goto monitor_done;  // EOF or error — child exited
                        total += n;
                    }

                    int local_yield_id = record[0];
                    int node_count = record[1];
                    int edge_count = record[2];
                    int global_id = yield_id_counter++;
                    yield_count++;

                    // Rename yield files to flat yield dir with global ID (ephemeral)
                    std::string yield_base = work_dir + "/yield";
                    std::string src_edgelist = yield_dir + "/" + std::to_string(local_yield_id) + ".bedgelist";
                    std::string src_cluster = yield_dir + "/" + std::to_string(local_yield_id) + ".bcluster";
                    std::string dst_edgelist = yield_base + "/" + std::to_string(global_id) + ".bedgelist";
                    std::string dst_cluster = yield_base + "/" + std::to_string(global_id) + ".bcluster";

                    try {
                        fs::rename(src_edgelist, dst_edgelist);
                        fs::rename(src_cluster, dst_cluster);
                    } catch (const std::exception& e) {
                        logger.error("Failed to move yield files for sub-cluster " +
                            std::to_string(local_yield_id) + ": " + e.what());
                        continue;
                    }

                    // Send YIELD_REPORT to LB: [parent_id, child_id, node_count, edge_count]
                    int yield_data[4] = {cluster_id, global_id, node_count, edge_count};
                    MPI_Send(yield_data, 4, MPI_INT, 0,
                             to_int(MessageType::YIELD_REPORT), MPI_COMM_WORLD);

                    logger.info("Yield (real-time): cluster " + std::to_string(cluster_id) +
                        " sub-cluster " + std::to_string(local_yield_id) +
                        " -> global ID " + std::to_string(global_id) +
                        " (nodes=" + std::to_string(node_count) +
                        ", edges=" + std::to_string(edge_count) + ")");
                }
                monitor_done:;
            });
        }

        int status;
        bool timed_out = false;
        struct rusage usage;

        // With timeout: use timer thread
        if (time_limit_per_cluster > 0) {
            bool child_done = false;
            std::mutex mtx;
            std::condition_variable cv;

            std::thread timer([&]() {
                std::unique_lock<std::mutex> lock(mtx);
                // Wait for timeout or early wake-up
                if (!cv.wait_for(lock, std::chrono::seconds(time_limit_per_cluster), [&] { return child_done; })) {
                    // Timeout occurred
                    timed_out = true;
                    kill(pid, SIGKILL);
                }
            });

            wait4(pid, &status, 0, &usage);
            logger.flush();

            // Wake up timer thread
            {
                std::lock_guard<std::mutex> lock(mtx);
                child_done = true;
            }
            cv.notify_one();
            timer.join();
        } else {    // no timeout: execute and wait for status
            wait4(pid, &status, 0, &usage);
            logger.flush();
        }

        // Child has exited — pipe write end is closed.
        // Wait for monitor thread to drain remaining data and exit.
        if (yield_monitor.joinable()) {
            yield_monitor.join();
        }
        if (yield_pipe[0] >= 0) close(yield_pipe[0]);

        // Clean up yield directory
        if (!yield_dir.empty() && fs::exists(yield_dir)) {
            fs::remove_all(yield_dir);
        }

        // Log and track peak memory usage
        int memory_mb = static_cast<int>(usage.ru_maxrss / 1024);
        logger.log("Cluster " + std::to_string(cluster_id) + " peak memory: " + std::to_string(memory_mb) + " MB");
        if (memory_mb > report.peak_memory_mb)
            report.peak_memory_mb = memory_mb;

        if (timed_out) {
            logger.log("Timeout. Child was killed after " + std::to_string(time_limit_per_cluster) + " seconds");
            ++report.timeout_count;
            return {false, yield_count};
        }

        // Check how child terminated
        bool success;
        if (WIFEXITED(status)) {
            logger.log("Child exited with code: " + std::to_string(WEXITSTATUS(status)));
            success = (WEXITSTATUS(status) == 0);
        } else {
            logger.log("Child killed by signal: " + std::to_string(WTERMSIG(status)));
            if (WTERMSIG(status) == SIGKILL)
                ++report.oom_count;  // SIGKILL without timeout is likely OOM
            success = false;
        }
        return {success, yield_count};
    } else {
        logger.log("Fork failed");  // TODO: this is serious. Need explicit handling
    }

    return {false, 0};   // fallback
}