#include <iostream>
#include <thread>
#include <filesystem>

#include <mpi.h>
#include <argparse.h>
#include <load_balancer.hpp>
#include <worker.hpp>
#include <utils.hpp>

namespace fs = std::filesystem; // for brevity

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
    std::string edgelist;
    std::string existing_clustering;
    std::string output_file;
    std::string work_dir;
    int log_level;
    std::string connectedness_criterion;
    bool prune;
    std::string mincut_type;
    int time_limit_per_cluster;

    std::string algorithm;
    double clustering_parameter;

    std::string clusters_dir;
    std::string logs_dir;
    std::string logs_clusters_dir;
    std::string output_dir;
    std::string pending_dir;

    // Rank 0 (root) parses arguments and launches load balancer
    try {
        if (rank == 0) {
            argparse::ArgumentParser main_program("distributed-constrained-clustering");
            argparse::ArgumentParser cm("CM");
            cm.add_description("CM");

            /* CM arguments */
            cm.add_argument("--edgelist")
                .required()
                .help("Network edge-list file");
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
            cm.add_argument("--existing-clustering")
                .required() // NOTE: for first version of distributed CM, clustering is required
                .help("Existing clustering file");
            cm.add_argument("--output-file")
                .required()
                .help("Output clustering file");
            cm.add_argument("--work-dir")
                .default_value("dcm-work-dir")
                .help("Directory to store intermediate results. Can be used to restore progress.");

            /* Each rank handles its own log */
            // cm.add_argument("--log-file")
            //     .required()
            //     .help("Output log file");

            cm.add_argument("--log-level")
                .default_value(int(1))
                .help("Log level where 0 = silent, 1 = info, 2 = verbose")
                .scan<'d', int>();
            cm.add_argument("--connectedness-criterion")
                .default_value("1log_10(n)")
                .help("String in the form of Clog_x(n) or Cn^x for well-connectedness");
            cm.add_argument("--prune")
                .default_value(false) // default false, implicit true
                .implicit_value(true) // default false, implicit true
                .help("Whether to prune nodes using mincuts");
            cm.add_argument("--mincut-type")
                .default_value("cactus")
                .help("Mincut type used (cactus or noi)");
            cm.add_argument("--time-limit-per-cluster")
                .default_value(int(-1))
                .help("Time limit in seconds for each cluster (-1 = no limit)")
                .scan<'d', int>();

            // TODO: support WCC in the future?

            main_program.add_subparser(cm);

            try {
                main_program.parse_args(argc, argv);
            } catch (const std::runtime_error& err) {
                std::cerr << err.what() << std::endl;
                std::cerr << main_program;
                MPI_Abort(MPI_COMM_WORLD, 1);   // TODO: error handling
            }

            std::cerr << "Arguments parsed" << std::endl;

            if (main_program.is_subcommand_used(cm)) {
                edgelist = cm.get<std::string>("--edgelist");
                algorithm = cm.get<std::string>("--algorithm");
                clustering_parameter = cm.get<double>("--clustering-parameter");
                existing_clustering = cm.get<std::string>("--existing-clustering");
                output_file = cm.get<std::string>("--output-file");
                work_dir = cm.get<std::string>("--work-dir");
                // std::string log_file = cm.get<std::string>("--log-file");
                log_level = cm.get<int>("--log-level") - 1; // so that enum is cleaner
                connectedness_criterion = cm.get<std::string>("--connectedness-criterion");
                prune = false;
                if (cm["--prune"] == true) {
                    prune = true;
                    std::cerr << "pruning" << std::endl;
                }
                mincut_type = cm.get<std::string>("--mincut-type");
                time_limit_per_cluster = cm.get<int>("--time-limit-per-cluster");

                /**
                 * TODO: checkpointing
                 * We want to check if there is existing progress, and if so we should restore them.
                 */

                // Ensure work-dir and sub-dir's exist
                clusters_dir = work_dir + "/" + "clusters";
                logs_dir = work_dir + "/" + "logs";
                logs_clusters_dir = logs_dir + "/" + "clusters";
                fs::create_directories(clusters_dir);
                fs::create_directories(logs_clusters_dir);

                // Initialize LoadBalancer (this partitions clustering and initializes job queue)
                lb = std::make_unique<LoadBalancer>(edgelist, existing_clustering, work_dir, output_file, log_level, use_rank_0_worker);

                // Spawn thread for runtime phase (job distribution)
                lb_thread = std::thread(&LoadBalancer::run, lb.get());
            }

        }
    } catch (const std::exception& err) {
        if (rank == 0) {
            std::cerr << err.what() << std::endl;
        }
        MPI_Abort(MPI_COMM_WORLD, 1);   // TODO: error handling
    }

    // Synchronize arguments
    bcast_string(work_dir, 0, MPI_COMM_WORLD);
    bcast_string(connectedness_criterion, 0, MPI_COMM_WORLD);
    bcast_string(mincut_type, 0, MPI_COMM_WORLD);
    bcast_string(algorithm, 0, MPI_COMM_WORLD);

    MPI_Bcast(&clustering_parameter, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&log_level, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&prune, 1, MPI_CXX_BOOL, 0, MPI_COMM_WORLD);
    MPI_Bcast(&use_rank_0_worker, 1, MPI_CXX_BOOL, 0, MPI_COMM_WORLD);
    MPI_Bcast(&time_limit_per_cluster, 1, MPI_INT, 0, MPI_COMM_WORLD);

    clusters_dir = work_dir + "/" + "clusters";
    logs_dir = work_dir + "/" + "logs";
    pending_dir = work_dir + "/" + "pending";

    bool is_worker = (rank != 0) || use_rank_0_worker;

    if (is_worker) {
        output_dir = work_dir + "/" + "output/worker_" + std::to_string(rank);
        fs::create_directories(output_dir);
    }
    fs::create_directory(pending_dir);

    MPI_Barrier(MPI_COMM_WORLD);

    if (is_worker) {
        Logger worker_logger(logs_dir + "/" + "worker_" + std::to_string(rank) + ".log", log_level);
        std::unique_ptr<Worker> worker = std::make_unique<Worker>(
            worker_logger, work_dir, algorithm, clustering_parameter, log_level, connectedness_criterion, mincut_type, prune, time_limit_per_cluster);

        worker->run();
    }

    if (rank == 0 && lb_thread.joinable()) {
        lb_thread.join();
    }

    MPI_Finalize();
    return 0;
}