#include <mpi.h>
#include <worker.hpp>
#include <constants.hpp>
#include <cm.h>
#include <constrained.h>

#include <fstream>
#include <sstream>
#include <filesystem>
#include <unistd.h>
#include <sys/wait.h>
#include <algorithm>

namespace fs = std::filesystem;

// Constructor
Worker::Worker(Logger& logger, const std::string& work_dir,
               const std::string& algorithm, double clustering_parameter,
               int log_level, const std::string& connectedness_criterion,
               const std::string& mincut_type, bool prune)
    : logger(logger), work_dir(work_dir),
      algorithm(algorithm), clustering_parameter(clustering_parameter),
      log_level(log_level), connectedness_criterion(connectedness_criterion),
      mincut_type(mincut_type), prune(prune) {}

// Main run function
void Worker::run() {
    logger.info("Worker runtime phase started");

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Create needed directory
    fs::create_directories(work_dir + "/output/worker_" + std::to_string(rank) + "/");
    fs::create_directories(work_dir + "/history/worker_" + std::to_string(rank) + "/");

    // Worker main loop
    while (true) {
        // Send work request to load balancer (rank 0)
        logger.info("Requesting cluster from the load balancer");
        int request_msg = to_int(MessageType::WORK_REQUEST);
        MPI_Send(&request_msg, 1, MPI_INT, 0, to_int(MessageType::WORK_REQUEST), MPI_COMM_WORLD);

        // Receive cluster ID from load balancer
        int assigned_cluster;
        MPI_Recv(&assigned_cluster, 1, MPI_INT, 0, to_int(MessageType::DISTRIBUTE_WORK), MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Check for termination signal
        if (assigned_cluster == NO_MORE_JOBS) {
            logger.info("No more jobs available, terminating worker");
            break;
        }

        logger.info("Received cluster " + std::to_string(assigned_cluster));

        // Process the cluster
        bool success = process_cluster(assigned_cluster);

        // Send completion status
        MessageType status_type = success ? MessageType::WORK_DONE : MessageType::WORK_ABORTED;
        MPI_Send(&assigned_cluster, 1, MPI_INT, 0, to_int(status_type), MPI_COMM_WORLD);    // inform load balancer success / failure

        if (success) {
            logger.info("Completed cluster " + std::to_string(assigned_cluster));
        } else {
            logger.info("Aborted cluster " + std::to_string(assigned_cluster));
        }
    }

    // Aggregation phase: combine all output files into one worker-specific file
    logger.info("Starting output aggregation");

    std::string output_dir = work_dir + "/output/";
    std::string worker_subdir = output_dir + "worker_" + std::to_string(rank) + "/";
    std::string worker_output_file = output_dir + "worker_" + std::to_string(rank) + ".out";

    std::ofstream out(worker_output_file);
    out << "node_id,cluster_id\n";  // Header

    int start_cluster_id = 0;

    // Iterate over all files in the worker-specific subdirectory
    for (const auto& entry : fs::directory_iterator(worker_subdir)) {
        if (entry.is_regular_file()) {
            std::string input_file = entry.path().string();
            std::ifstream in(input_file);

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
            start_cluster_id += (max_cluster_id + 1);
        }
    }

    out.close();
    logger.info("Output aggregation complete. Worker output: " + worker_output_file);

    // Send AGGREGATE_DONE signal to load balancer
    int aggregate_msg = to_int(MessageType::AGGREGATE_DONE);
    MPI_Send(&aggregate_msg, 1, MPI_INT, 0, to_int(MessageType::AGGREGATE_DONE), MPI_COMM_WORLD);
    logger.info("Sent AGGREGATE_DONE signal to load balancer");

    logger.info("Worker runtime phase ended");
}

// Process a single cluster
bool Worker::process_cluster(int cluster_id) {
    // TODO: implement actual cluster processing
    // For now, this is a placeholder that simulates work

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    std::string cluster_edgelist = work_dir + "/clusters/" + std::to_string(cluster_id) + ".edgelist";
    std::string cluster_clustering_file = work_dir + "/clusters/" + std::to_string(cluster_id) + ".cluster";
    logger.debug("Processing cluster file: " + cluster_edgelist);

    logger.flush(); // to avoid duplicate logs after fork()

    // TODO: spawn a child process and call CM processing logic on it
    // This is to gracefully handle OOM kills

    int pid = fork();
    if (pid == 0) {  // child process
        logger.info("Child process starts on cluster " + std::to_string(cluster_id));

        // TODO: CM logic
        std::string output_file = work_dir + "/output/worker_" + std::to_string(rank) + "/" + std::to_string(cluster_id) + ".output";
        std::string history_file = work_dir + "/history/worker_" + std::to_string(rank) + "/" + std::to_string(cluster_id) + ".hist";
        std::string log_file = work_dir + "/logs/clusters/" + std::to_string(cluster_id) + ".log"; // TODO: since CC was built as a standalone app with its own logging system, we have to use a different file. In the future we should try to integrate the two systems into one unified logging system.

        ConstrainedClustering* connectivity_modifier = new CM(cluster_edgelist, this->algorithm, this->clustering_parameter, cluster_clustering_file, 1, output_file, log_file, history_file, this->log_level, this->connectedness_criterion, this->prune, this->mincut_type);

        connectivity_modifier->main();  // run CM

        logger.info("Child process finishes cluster " + std::to_string(cluster_id));
        exit(0);    // exits successfully
    } else if (pid > 0) {    // control process
        int status;
        waitpid(pid, &status, 0);

        // Check how child terminates
        if (WIFEXITED(status)) {
            logger.log("Child exited with code: " + std::to_string(WEXITSTATUS(status)));
            return WEXITSTATUS(status) == 0;
        } else {
            logger.log("Child killed by signal: " + std::to_string(WTERMSIG(status)));
            return false;
        }
    } else {
        logger.log("Fork failed");  // TODO: this is serious. Need explicit handling
    }

    return true;
}