# Distributed Constrained Clustering (Distributed CC)

A distributed implementation of Connectivity Modifier (CM) and Well-Connected Clusters (WCC) using MPI for parallel processing of large-scale graph clustering.

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd distributed_CM

# Fetch all git submodules
git submodule update --init --recursive

# Run setup script (installs dependencies)
./setup.sh

# Build and compile
./easy_build_and_compile.sh
```

## Usage

The program supports two methods: **CM** (Connectivity Modifier) and **WCC** (Well-Connected Clusters).

```bash
# CM mode
mpirun -np <num_processes> ./distributed-constrained-clustering CM [OPTIONS]

# WCC mode
mpirun -np <num_processes> ./distributed-constrained-clustering WCC [OPTIONS]
```

## Arguments

### Common Arguments (CM and WCC)

#### Required Arguments

| Argument | Description |
|----------|-------------|
| `--edgelist <path>` | Path to the network edge-list file. The file should contain edges in CSV format with a header row. |
| `--existing-clustering <path>` | Path to the existing clustering file. The file should contain node-to-cluster mappings in CSV format with a header row. |
| `--output-file <path>` | Path to the output clustering file where the final results will be written. |

#### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--work-dir <path>` | `dcm-work-dir` | Directory to store intermediate results. Can be used to restore progress via checkpointing. |
| `--log-level <level>` | `1` | Logging verbosity level. `0` = silent, `1` = info, `2` = verbose/debug. |
| `--connectedness-criterion <expr>` | `1log_10(n)` | Well-connectedness criterion. Format: `Clog_x(n)` or `Cn^x` where C is a constant, x is the base/exponent, and n is the cluster size. |
| `--prune` | `false` | Enable pruning of nodes using mincuts. Flag argument (no value needed). |
| `--mincut-type <type>` | `cactus` | Mincut algorithm to use. Options: `cactus`, `noi`. |
| `--time-limit-per-cluster <seconds>` | `-1` | Time limit in seconds for processing each cluster. `-1` means no limit. Clusters exceeding this limit are aborted. |
| `--partitioned-clusters-dir <path>` | `<work-dir>/clusters` | Path to pre-partitioned clusters directory. If provided with a valid `summary.csv`, skips the partitioning phase. |
| `--partition-only` | `false` | Stop after partitioning (Phase 1) without launching computation jobs. Useful for preparing clusters for later processing. |
| `--min-batch-cost <value>` | `1.0` | Minimum total estimated cost per batch when assigning clusters to workers. Higher values mean more clusters per batch, reducing communication overhead. |

#### Finer Control Arguments

These arguments are reserved for expert users. They control subtle behaviors that may conflict with other arguments. Use with caution.

| Argument | Default | Description |
|----------|---------|-------------|
| `--drop-cluster-under <n>` | `-1` | Drop clusters with fewer than `n` nodes during partitioning. `-1` means no filtering. |
| `--bypass-clique` | `false` | Automatically accept cliques without processing, regardless of the connectedness criterion. Bypassed clusters are written directly to output. |

### CM-Specific Arguments

These arguments are only available when using the CM method.

| Argument | Default | Description |
|----------|---------|-------------|
| `--algorithm <name>` | - | Clustering algorithm to use. Options: `leiden-cpm`, `leiden-mod`, `louvain`. |
| `--clustering-parameter <value>` | `0.01` | Clustering parameter (e.g., resolution parameter for Leiden-CPM). |

### WCC-Specific Arguments

WCC uses only the common arguments. It performs mincut-based well-connectedness checking without re-clustering.

## Examples

### CM: Basic Usage

```bash
mpirun -np 4 ./distributed-constrained-clustering CM \
    --edgelist network.csv \
    --existing-clustering initial_clustering.csv \
    --output-file output.csv \
    --algorithm leiden-cpm
```

### CM: With Custom Parameters

```bash
mpirun -np 8 ./distributed-constrained-clustering CM \
    --edgelist network.csv \
    --existing-clustering initial_clustering.csv \
    --output-file output.csv \
    --algorithm leiden-cpm \
    --clustering-parameter 0.001 \
    --work-dir my-work-dir \
    --log-level 2 \
    --time-limit-per-cluster 300 \
    --prune \
    --mincut-type cactus
```

### WCC: Basic Usage

```bash
mpirun -np 4 ./distributed-constrained-clustering WCC \
    --edgelist network.csv \
    --existing-clustering initial_clustering.csv \
    --output-file output.csv
```

### WCC: With Custom Parameters

```bash
mpirun -np 8 ./distributed-constrained-clustering WCC \
    --edgelist network.csv \
    --existing-clustering initial_clustering.csv \
    --output-file output.csv \
    --work-dir my-work-dir \
    --log-level 2 \
    --connectedness-criterion "2log_10(n)" \
    --mincut-type cactus
```

### Partition-Only Mode

First, partition the clustering (works with either CM or WCC):

```bash
mpirun -np 1 ./distributed-constrained-clustering CM \
    --edgelist network.csv \
    --existing-clustering initial_clustering.csv \
    --output-file output.csv \
    --work-dir my-work-dir \
    --partition-only
```

Then, process the partitioned clusters (can be run multiple times with checkpointing):

```bash
mpirun -np 16 ./distributed-constrained-clustering CM \
    --edgelist network.csv \
    --existing-clustering initial_clustering.csv \
    --output-file output.csv \
    --work-dir my-work-dir \
    --partitioned-clusters-dir my-work-dir/clusters \
    --algorithm leiden-cpm
```

## File Formats

### Edge-list File

CSV format with header:
```
source,target
0,1
0,2
1,2
...
```

### Clustering File

CSV format with header:
```
node_id,cluster_id
0,0
1,0
2,1
...
```

## Checkpointing

The program automatically saves checkpoints to `<work-dir>/checkpoint.csv` when:
- The job receives a `SIGTERM` signal (e.g., Slurm time limit)
- The job receives a `SIGABRT` signal (e.g., internal errors)
- Some clusters fail to process (timeout, OOM, etc.)

To resume from a checkpoint, simply re-run the program with the same `--work-dir`. The program will automatically detect and load the checkpoint file.

## Work Directory Structure

```
<work-dir>/
├── checkpoint.csv          # Checkpoint file (if any)
├── clusters/               # Partitioned cluster files
│   ├── summary.csv         # Cluster metadata
│   ├── <id>.edgelist       # Cluster edge-lists
│   └── <id>.cluster        # Cluster node mappings
├── logs/
│   ├── load_balancer.log   # Load balancer log
│   ├── worker_<rank>.log   # Worker logs
│   └── clusters/           # Per-cluster CM logs
├── output/
│   ├── worker_<rank>/      # Per-worker output files
│   ├── worker_<rank>.out   # Aggregated worker output
│   └── bypass.out          # Bypassed clusters (e.g., cliques)
├── history/                # CM history files
└── pending/                # Pending cluster markers
```

## Architecture

- **Rank 0**: Runs the load balancer (and optionally a worker if only 1 process)
- **Rank 1+**: Run workers that process clusters in parallel

The load balancer distributes clusters to workers based on estimated cost (function of node count and edge density). Workers process clusters using forked child processes to gracefully handle OOM kills and timeouts.