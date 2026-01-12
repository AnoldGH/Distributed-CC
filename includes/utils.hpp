#pragma once
#include <string>
#include <fstream>
#include <iostream>

#include <mpi.h>

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