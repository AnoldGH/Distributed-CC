#pragma once

#include <string>
#include <fstream>
#include <chrono>
#include <iostream>

enum class LogLevel {
    ERROR = -1,
    INFO = 0,
    DEBUG = 1
};

class Logger {
private:
    std::ofstream log_file_handle;
    LogLevel log_level;
    std::chrono::steady_clock::time_point start_time;
    int num_calls_to_log_write;
    bool enabled;

public:
    Logger(std::string log_file, LogLevel level)
        : log_level(level), num_calls_to_log_write(0), enabled(true) {
        start_time = std::chrono::steady_clock::now();
        log_file_handle.open(log_file);
        if (!log_file_handle.is_open()) {
            std::cerr << "[WARNING] Failed to open log file: " << log_file << std::endl;
            enabled = false;
        }
    }

    // Constructor accepting int log level (for compatibility with existing code)
    // int: -1 = ERROR, 0 = INFO, 1+ = DEBUG
    Logger(std::string log_file, int level)
        : num_calls_to_log_write(0), enabled(true) {
        // Convert int to LogLevel
        if (level >= 1) {
            log_level = LogLevel::DEBUG;
        } else if (level == 0) {
            log_level = LogLevel::INFO;
        } else {
            log_level = LogLevel::ERROR;
        }

        start_time = std::chrono::steady_clock::now();
        log_file_handle.open(log_file);
        if (!log_file_handle.is_open()) {
            std::cerr << "[WARNING] Failed to open log file: " << log_file << std::endl;
            enabled = false;
        }
    }

    // Constructor for disabled logging
    Logger() : log_level(LogLevel::ERROR), num_calls_to_log_write(0), enabled(false) {
        start_time = std::chrono::steady_clock::now();
    }

    ~Logger() {
        if (log_file_handle.is_open()) {
            log_file_handle.close();
        }
    }

    void log(const std::string& message, LogLevel message_type = LogLevel::INFO) {
        if (!enabled || message_type > log_level) {
            return;
        }

        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        std::string log_message_prefix;

        // Add log level prefix
        if (message_type == LogLevel::INFO) {
            log_message_prefix = "[INFO]";
        } else if (message_type == LogLevel::DEBUG) {
            log_message_prefix = "[DEBUG]";
        } else if (message_type == LogLevel::ERROR) {
            log_message_prefix = "[ERROR]";
        }

        // Calculate elapsed time
        auto total_seconds_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
        auto total_minutes = total_seconds_elapsed.count() / 60;
        auto total_hours = total_minutes / 60;
        auto total_days = total_hours / 24;

        auto hours_elapsed = total_hours % 24;
        auto minutes_elapsed = total_minutes % 60;
        auto seconds_elapsed = total_seconds_elapsed.count() % 60;

        // Format timestamp
        log_message_prefix += "[";
        log_message_prefix += std::to_string(total_days);
        log_message_prefix += "-";
        log_message_prefix += std::to_string(hours_elapsed);
        log_message_prefix += ":";
        log_message_prefix += std::to_string(minutes_elapsed);
        log_message_prefix += ":";
        log_message_prefix += std::to_string(seconds_elapsed);
        log_message_prefix += "]";

        log_message_prefix += "(t=";
        log_message_prefix += std::to_string(total_seconds_elapsed.count());
        log_message_prefix += "s)";

        // Write to log file
        log_file_handle << log_message_prefix << " " << message << '\n';

        // Periodic flush
        if (num_calls_to_log_write % 10 == 0) {
            std::flush(log_file_handle);
        }
        num_calls_to_log_write++;
    }

    void info(const std::string& message) {
        log(message, LogLevel::INFO);
    }

    void debug(const std::string& message) {
        log(message, LogLevel::DEBUG);
    }

    void error(const std::string& message) {
        log(message, LogLevel::ERROR);
    }

    void flush() {
        if (enabled && log_file_handle.is_open()) {
            std::flush(log_file_handle);
        }
    }
};
