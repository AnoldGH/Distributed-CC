#include <string>
#include <fstream>
#include <iostream>

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