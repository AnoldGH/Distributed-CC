#include <csignal>
#include <iostream>
#include <thread>
#include <filesystem>

#include <mpi.h>
#include <argparse.h>
#include <load_balancer.hpp>
#include <worker.hpp>
#include <utils.hpp>

namespace fs = std::filesystem; // for brevity

// Signal handling
LoadBalancer* global_lb_ptr = nullptr;
void signal_handler(int signum) {
    if (global_lb_ptr) {
        global_lb_ptr->save_checkpoint();
    }
    _exit(0);
}

int main(int argc, char** argv) {
    // Initialize MPI
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (provided < MPI_THREAD_MULTIPLE) {
        // We don't have multi-thread MPI support
        std::cerr << "No multi-thread MPI support!" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);   // TODO: error handling

        // TODO: fallback to using the entire rank 0 as the load balancer
    }

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /**
     * Use rank 0 as a worker only if there is only one rank (i.e., there is essentially no need for a load balancer), and jobs run sequentially.
     *  The load balancer and worker 0 will be two threads living on the same rank. The overhead is low because there isn't much communication.
     * Otherwise, rank 0 is entirely the load balancer to reduce the burden.
     */
    bool use_rank_0_worker = (size == 1);

    // Load balancer reference
    std::unique_ptr<LoadBalancer> lb;
    std::thread lb_thread; // Load balancer thread (only used by rank 0)

    // Declarations
    std::string method;  // "CM" or "WCC"
    std::string edgelist;
    std::string existing_clustering;
    std::string output_file;
    std::string work_dir;
    int log_level;
    std::string connectedness_criterion;
    bool prune;
    std::string mincut_type;
    int time_limit_per_cluster;
    bool partition_only;
    float min_batch_cost;
    int drop_cluster_under;
    bool bypass_cluster;
    int report_interval;

    std::string algorithm;
    double clustering_parameter;

    std::string clusters_dir;
    std::string logs_dir;
    std::string logs_clusters_dir;
    std::string output_dir;
    std::string pending_dir;
    std::string partitioned_clusters_dir;

    // Rank 0 (root) parses arguments and launches load balancer
    try {
        if (rank == 0) {
            argparse::ArgumentParser main_program("distributed-constrained-clustering");
            argparse::ArgumentParser common("Common");

            /** Common Arguments */
            common.add_argument("--edgelist")
                .required()
                .help("Network edge-list file");
            common.add_argument("--existing-clustering")
                .required() // NOTE: for first version of distributed CC, clustering is required
                .help("Existing clustering file");
            common.add_argument("--output-file")
                .required()
                .help("Output clustering file");
            common.add_argument("--work-dir")
                .default_value("dcm-work-dir")
                .help("Directory to store intermediate results. Can be used to restore progress.");
            common.add_argument("--log-level")
                .default_value(int(1))
                .help("Log level where 0 = silent, 1 = info, 2 = verbose")
                .scan<'d', int>();
            common.add_argument("--connectedness-criterion")
                .default_value("1log_10(n)")
                .help("String in the form of Clog_x(n) or Cn^x for well-connectedness");
            common.add_argument("--prune")
                .default_value(false)
                .implicit_value(true) // default false, implicit true
                .help("Whether to prune nodes using mincuts");
            common.add_argument("--mincut-type")
                .default_value("cactus")
                .help("Mincut type used (cactus or noi)");

            /** Control Arguments */
            common.add_argument("--time-limit-per-cluster")
                .default_value(int(-1))
                .help("Time limit in seconds for each cluster (-1 = no limit)")
                .scan<'d', int>();
            common.add_argument("--partitioned-clusters-dir")
                .default_value(std::string(""))
                .help("Path to pre-partitioned clusters directory (skips partitioning if provided)");
            common.add_argument("--partition-only")
                .default_value(false)
                .implicit_value(true)
                .help("Stop after partitioning (Phase 1) without launching computation jobs");
            common.add_argument("--min-batch-cost")
                .default_value(float(1))
                .help("Minimum total cost per batch when assigning clusters to workers")
                .scan<'f', float>();
            common.add_argument("--report-interval")
                .default_value(int(10))
                .help("Workers send status reports to LB every N requests (-1 = disabled)")
                .scan<'d', int>();

            /**
             * Finer control arguments
             * These are reserved for expert users. They decide some subtle behaviors of Dist CM, which may conflict with other arguments. Use with caution.
             * Default values for these arguments result in most general, consistent behaviors
             */
            common.add_argument("--drop-cluster-under")
                .default_value(-1)
                .help("Drop cluster with less than (strictly) specified number of nodes")
                .scan<'d', int>();
            common.add_argument("--bypass-clique")
                .default_value(false)
                .implicit_value(true)
                .help("The load balancer always accepts cliques, regardless of user-specific connectedness criterion");

            /** CM arguments */
            argparse::ArgumentParser cm("CM");
            cm.add_description("CM");
            cm.add_parents(common);

            // CM specific arguments
            cm.add_argument("--algorithm")
                .help("Clustering algorithm to be used (leiden-cpm, leiden-mod, louvain)")
                .action([](const std::string& value) {
                    static const std::vector<std::string> choices = {"leiden-cpm", "leiden-mod", "louvain"};
                    if (std::find(choices.begin(), choices.end(), value) != choices.end()) {
                        return value;
                    }
                    throw std::invalid_argument("--algorithm can only take in leiden-cpm, leiden-mod, or louvain.");
                });
            cm.add_argument("--clustering-parameter")
                .default_value(double(0.01))
                .help("Clustering parameter e.g., 0.01 for Leiden-CPM")
                .scan<'f', double>();

            // TODO: support WCC
            argparse::ArgumentParser wcc("WCC");
            wcc.add_description("WCC");
            wcc.add_parents(common);

            // WCC specific arguments

            main_program.add_subparser(cm);
            main_program.add_subparser(wcc);

            try {
                main_program.parse_args(argc, argv);
            } catch (const std::runtime_error& err) {
                std::cerr << err.what() << std::endl;
                std::cerr << main_program;
                MPI_Abort(MPI_COMM_WORLD, 1);   // TODO: error handling
            }

            std::cerr << "Arguments parsed" << std::endl;

            if (main_program.is_subcommand_used(cm)) {
                method = "CM";
                edgelist = cm.get<std::string>("--edgelist");
                algorithm = cm.get<std::string>("--algorithm");
                clustering_parameter = cm.get<double>("--clustering-parameter");
                existing_clustering = cm.get<std::string>("--existing-clustering");
                output_file = cm.get<std::string>("--output-file");
                work_dir = cm.get<std::string>("--work-dir");
                log_level = cm.get<int>("--log-level") - 1; // so that enum is cleaner
                connectedness_criterion = cm.get<std::string>("--connectedness-criterion");
                prune = false;
                if (cm["--prune"] == true) {
                    prune = true;
                    std::cerr << "pruning" << std::endl;
                }
                mincut_type = cm.get<std::string>("--mincut-type");
                time_limit_per_cluster = cm.get<int>("--time-limit-per-cluster");
                partitioned_clusters_dir = cm.get<std::string>("--partitioned-clusters-dir");
                if (partitioned_clusters_dir == "") {   // default: use work-dir clusters
                    partitioned_clusters_dir = work_dir + "/clusters";
                }

                partition_only = cm.get<bool>("--partition-only");
                min_batch_cost = cm.get<float>("--min-batch-cost");
                drop_cluster_under = cm.get<int>("--drop-cluster-under");
                bypass_cluster = cm.get<bool>("--bypass-clique");
                report_interval = cm.get<int>("--report-interval");

                // Ensure work-dir and sub-dir's exist
                clusters_dir = work_dir + "/" + "clusters";
                logs_dir = work_dir + "/" + "logs";
                logs_clusters_dir = logs_dir + "/" + "clusters";
                fs::create_directories(clusters_dir);
                fs::create_directories(logs_clusters_dir);

                // Initialize LoadBalancer (this partitions clustering and initializes job queue)
                lb = std::make_unique<LoadBalancer>(method, edgelist, existing_clustering, work_dir, output_file, log_level, use_rank_0_worker, partitioned_clusters_dir, partition_only, min_batch_cost, drop_cluster_under, bypass_cluster);

                // Signal handling - Slurm sends SIGTERM before SIGKILL a job
                // Also handle SIGABRT for internal errors (e.g., memory corruption, assertion failures)
                global_lb_ptr = lb.get();
                std::signal(SIGTERM, signal_handler);   // register signal handler
                std::signal(SIGABRT, signal_handler);   // save checkpoint on abort (e.g., free() errors)

                if (partition_only) {
                    std::cerr << "Partition-only mode: won't start the load balancer" << std::endl;
                } else {
                    // Spawn thread for runtime phase (job distribution)
                    lb_thread = std::thread(&LoadBalancer::run, lb.get());
                }
            } else if (main_program.is_subcommand_used(wcc)) {
                method = "WCC";
                edgelist = wcc.get<std::string>("--edgelist");
                existing_clustering = wcc.get<std::string>("--existing-clustering");
                output_file = wcc.get<std::string>("--output-file");
                work_dir = wcc.get<std::string>("--work-dir");
                log_level = wcc.get<int>("--log-level") - 1; // so that enum is cleaner
                connectedness_criterion = wcc.get<std::string>("--connectedness-criterion");
                prune = false;
                if (wcc["--prune"] == true) {
                    prune = true;
                    std::cerr << "pruning" << std::endl;
                }
                mincut_type = wcc.get<std::string>("--mincut-type");
                time_limit_per_cluster = wcc.get<int>("--time-limit-per-cluster");
                partitioned_clusters_dir = wcc.get<std::string>("--partitioned-clusters-dir");
                if (partitioned_clusters_dir == "") {   // default: use work-dir clusters
                    partitioned_clusters_dir = work_dir + "/clusters";
                }

                partition_only = wcc.get<bool>("--partition-only");
                min_batch_cost = wcc.get<float>("--min-batch-cost");
                drop_cluster_under = wcc.get<int>("--drop-cluster-under");
                bypass_cluster = wcc.get<bool>("--bypass-clique");
                report_interval = wcc.get<int>("--report-interval");

                // Ensure work-dir and sub-dir's exist
                clusters_dir = work_dir + "/" + "clusters";
                logs_dir = work_dir + "/" + "logs";
                logs_clusters_dir = logs_dir + "/" + "clusters";
                fs::create_directories(clusters_dir);
                fs::create_directories(logs_clusters_dir);

                // Initialize LoadBalancer (this partitions clustering and initializes job queue)
                lb = std::make_unique<LoadBalancer>(method, edgelist, existing_clustering, work_dir, output_file, log_level, use_rank_0_worker, partitioned_clusters_dir, partition_only, min_batch_cost, drop_cluster_under, bypass_cluster);

                // Signal handling - Slurm sends SIGTERM before SIGKILL a job
                // Also handle SIGABRT for internal errors (e.g., memory corruption, assertion failures)
                global_lb_ptr = lb.get();
                std::signal(SIGTERM, signal_handler);   // register signal handler
                std::signal(SIGABRT, signal_handler);   // save checkpoint on abort (e.g., free() errors)

                if (partition_only) {
                    std::cerr << "Partition-only mode: won't start the load balancer" << std::endl;
                } else {
                    // Spawn thread for runtime phase (job distribution)
                    lb_thread = std::thread(&LoadBalancer::run, lb.get());
                }
            }

        }
    } catch (const std::exception& err) {
        if (rank == 0) {
            std::cerr << err.what() << std::endl;
        }
        MPI_Abort(MPI_COMM_WORLD, 1);   // TODO: error handling
    }

    // Synchronize arguments
    bcast_string(method, 0, MPI_COMM_WORLD);
    bcast_string(work_dir, 0, MPI_COMM_WORLD);
    bcast_string(connectedness_criterion, 0, MPI_COMM_WORLD);
    bcast_string(mincut_type, 0, MPI_COMM_WORLD);
    bcast_string(algorithm, 0, MPI_COMM_WORLD);
    bcast_string(partitioned_clusters_dir, 0, MPI_COMM_WORLD);

    MPI_Bcast(&clustering_parameter, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&log_level, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&prune, 1, MPI_CXX_BOOL, 0, MPI_COMM_WORLD);
    MPI_Bcast(&use_rank_0_worker, 1, MPI_CXX_BOOL, 0, MPI_COMM_WORLD);
    MPI_Bcast(&time_limit_per_cluster, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&report_interval, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&partition_only, 1, MPI_CXX_BOOL, 0, MPI_COMM_WORLD);

    clusters_dir = work_dir + "/" + "clusters";
    if (!partitioned_clusters_dir.empty()) {
        clusters_dir = partitioned_clusters_dir;
    }
    logs_dir = work_dir + "/" + "logs";
    pending_dir = work_dir + "/" + "pending";

    bool is_worker = (rank != 0) || use_rank_0_worker;

    if (is_worker) {
        output_dir = work_dir + "/" + "output/worker_" + std::to_string(rank);
        fs::create_directories(output_dir);
    }
    fs::create_directory(pending_dir);

    MPI_Barrier(MPI_COMM_WORLD);

    if (!partition_only) {  // Partition-only mode, no need to spawn worker
        if (is_worker) {
            Logger worker_logger(logs_dir + "/" + "worker_" + std::to_string(rank) + ".log", log_level);
            std::unique_ptr<Worker> worker = std::make_unique<Worker>(
                method, worker_logger, work_dir, clusters_dir, algorithm, clustering_parameter, log_level, connectedness_criterion, mincut_type, prune, time_limit_per_cluster, report_interval);

            worker->run();
        }

        if (rank == 0 && lb_thread.joinable()) {
            lb_thread.join();
        }
    }

    MPI_Finalize();
    return 0;
}