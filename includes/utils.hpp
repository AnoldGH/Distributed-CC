#pragma once
#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <set>
#include <cstdint>

#include <mpi.h>

inline bool has_suffix(const std::string& filepath, const std::string& suffix) {
    if (filepath.size() < suffix.size()) return false;
    return filepath.compare(filepath.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline bool is_binary_edgelist(const std::string& filepath) {
    return has_suffix(filepath, ".bedgelist");
}

inline bool is_binary_cluster(const std::string& filepath) {
    return has_suffix(filepath, ".bcluster");
}

inline char get_delimiter(std::string filepath) {
    std::ifstream clustering(filepath);
    std::string line;
    getline(clustering, line);
    if (line.find(',') != std::string::npos) {
        return ',';
    } else if (line.find('\t') != std::string::npos) {
        return '\t';
    } else if (line.find(' ') != std::string::npos) {
        return ' ';
    }
    throw std::invalid_argument("Could not detect filetype for " + filepath);
}

inline void write_binary_edgelist(const std::string& filepath,
                                  const std::vector<std::pair<int, int>>& edges) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open binary edgelist for writing: " + filepath);
    }
    uint64_t num_edges = edges.size();
    out.write(reinterpret_cast<const char*>(&num_edges), sizeof(num_edges));
    out.write(reinterpret_cast<const char*>(edges.data()),
              num_edges * sizeof(std::pair<int, int>));
}

inline void write_binary_cluster(const std::string& filepath,
                                 const std::set<int>& nodes, int cluster_id) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open binary cluster for writing: " + filepath);
    }
    uint32_t num_entries = static_cast<uint32_t>(nodes.size());
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    for (int node : nodes) {
        int32_t n = static_cast<int32_t>(node);
        int32_t c = static_cast<int32_t>(cluster_id);
        out.write(reinterpret_cast<const char*>(&n), sizeof(n));
        out.write(reinterpret_cast<const char*>(&c), sizeof(c));
    }
}

inline void write_binary_cluster(const std::string& filepath,
                                 const std::vector<std::pair<int, int>>& entries) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open binary cluster for writing: " + filepath);
    }
    uint32_t num_entries = static_cast<uint32_t>(entries.size());
    out.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    for (const auto& [node_id, cluster_id] : entries) {
        int32_t n = static_cast<int32_t>(node_id);
        int32_t c = static_cast<int32_t>(cluster_id);
        out.write(reinterpret_cast<const char*>(&n), sizeof(n));
        out.write(reinterpret_cast<const char*>(&c), sizeof(c));
    }
}

// Broadcast a string
inline void bcast_string(std::string& s, int root, MPI_Comm comm) {
    int rank;
    MPI_Comm_rank(comm, &rank);

    // Broadcast length of the string
    int length = 0;
    if (rank == root) {
        length = static_cast<int>(s.size());
    }
    MPI_Bcast(&length, 1, MPI_INT, root, comm);

    // Receiver
    if (rank != root) {
        s.resize(length);
    }

    // Broadcast actual data
    if (length > 0) {
        MPI_Bcast(&s[0], length, MPI_CHAR, root, comm);
    }
}