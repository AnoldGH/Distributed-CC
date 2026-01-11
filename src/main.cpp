#include <iostream>
#include <thread>
#include <filesystem>

#include <mpi.h>
#include <argparse.h>
#include <load_balancer.hpp>

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

            // TODO: support WCC in the future?

            main_program.add_subparser(cm);

            try {
                main_program.parse_args(argc, argv);
            } catch (const std::runtime_error& err) {
                std::cerr << err.what() << std::endl;
                std::cerr << main_program;
                MPI_Abort(MPI_COMM_WORLD, 1);   // TODO: error handling
            }

            // TODO: Spawn load balancer thread
            // std::thread load_balancer_thread(LoadBalancer::load_balancer_function);
            // load_balancer_thread.detach();

            std::cerr << "Arguments parsed" << std::endl;

            if (main_program.is_subcommand_used(cm)) {
                std::string edgelist = cm.get<std::string>("--edgelist");
                // std::string algorithm = cm.get<std::string>("--algorithm");
                // double clustering_parameter = cm.get<double>("--clustering-parameter");
                std::string existing_clustering = cm.get<std::string>("--existing-clustering");
                std::string output_file = cm.get<std::string>("--output-file");
                std::string work_dir = cm.get<std::string>("--work-dir");
                // std::string log_file = cm.get<std::string>("--log-file");
                int log_level = cm.get<int>("--log-level") - 1; // so that enum is cleaner
                std::string connectedness_criterion = cm.get<std::string>("--connectedness-criterion");
                bool prune = false;
                if (cm["--prune"] == true) {
                    prune = true;
                    std::cerr << "pruning" << std::endl;
                }
                std::string mincut_type = cm.get<std::string>("--mincut-type");
                // ConstrainedClustering* connectivity_modifier = new CM(edgelist, algorithm, clustering_parameter, existing_clustering, num_processors, output_file, log_file, log_level, connectedness_criterion, prune, mincut_type);
                // random_functions::setSeed(0);
                // connectivity_modifier->main();
                // delete connectivity_modifier;

                /**
                 * TODO: checkpointing
                 * We want to check if there is existing progress, and if so we should restore them.
                 */

                // Ensure work-dir and sub-dir's exist
                const std::string clusters_dir = work_dir + "/" + "clusters";
                const std::string logs_dir = work_dir + "/" + "logs";
                fs::create_directories(clusters_dir);
                fs::create_directory(logs_dir);

                // Initialize LoadBalancer (this partitions clustering and initializes job queue)
                LoadBalancer lb(edgelist, existing_clustering, work_dir, log_level);

                // Spawn thread for runtime phase (job distribution)
                std::thread lb_thread(&LoadBalancer::run, &lb);
                lb_thread.detach();
            }

        }
    } catch (const std::exception& err) {
        if (rank == 0) {
            std::cerr << err.what() << std::endl;
        }
        MPI_Abort(MPI_COMM_WORLD, 1);   // TODO: error handling
    }

    // All ranks wait here until rank 0 completes initialization
    MPI_Barrier(MPI_COMM_WORLD);

    // TODO: Implement worker_function
    // All ranks (including rank 0) now act as workers
    std::cerr << "Rank " << rank << " ready to start working" << std::endl;

    // Placeholder: workers will request jobs from load balancer
    // worker_function(rank, size);

    MPI_Finalize();
    return 0;
}