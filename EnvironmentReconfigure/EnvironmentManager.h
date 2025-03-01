// EnvironmentManager.h
#ifndef ENVIRONMENTMANAGER_H
#define ENVIRONMENTMANAGER_H

#include <unordered_map>
#include <functional>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <spdlog/spdlog.h>
#include "../customQueue.h"

class EnvironmentManager
{
public:
    using Task = std::function<void(int)>;

    // Constructor
    EnvironmentManager();

    // Destructor
    ~EnvironmentManager();

    // Register an operation (e.g., write, decompress)
    void registerOperation(const std::string &name, Task task, std::shared_ptr<customQueue<int>> poisonQueue);

    // Configure the number of threads for an operation
    void configureThreads(const std::string &name, int new_thread_count);

    // Start the reconfiguration manager
    void start();

    // Stop the reconfiguration manager and all threads
    void stop();

    // Join all threads for a specific operation
    void joinThreads(const std::string &name);

private:
    struct Operation
    {
        Task task;
        std::shared_ptr<customQueue<int>> poisonQueue;
        int active_threads = 0;
        int desired_threads = 0;
        std::vector<std::thread> threads;
    };

    void run(); // Main loop that handles thread creation and termination

    std::unordered_map<std::string, Operation> operations_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> terminate_;
    std::atomic<bool> config_update_;
    std::atomic<bool> config_over_;
    std::thread reconfig_thread_;
};

// Sample code for user:
/*
 EnvironmentManager reconfig_manager;

// Register operations with lambdas to bind arguments

// Register ANALYTICS operation with specific arguments
reconfig_manager.registerOperation("ANALYTICS",
    [&](int thr) {
        int min = 0, max = 0;
        long sum = 0, cnt = 0, totalcnt = 0;
        analyticsThread(thr, min, max, sum, cnt, totalcnt);  // Using thread index 'thr' and other arguments
    },
    writeBufferIds);

// Register STORAGE operation with specific arguments
reconfig_manager.registerOperation("STORAGE",
    [&](int thr) {
        std::string filename = "data_file";  // Dynamically generate filename based on thread ID
        storageThread(thr, filename);  // Using thread index 'thr' and filename
    },
    decompressedBufferIds);

// Start the reconfiguration manager
reconfig_manager.start();

// Configure threads dynamically
reconfig_manager.configureThreads("ANALYTICS", 5);  // Start 5 threads for analytics
reconfig_manager.configureThreads("STORAGE", 3);    // Start 3 threads for storage

// Simulate reconfiguration at runtime
reconfig_manager.configureThreads("ANALYTICS", 2);  // Reduce threads for ANALYTICS
reconfig_manager.configureThreads("STORAGE", 4);    // Increase threads for STORAGE

// Join threads for both operations
reconfig_manager.joinThreads("ANALYTICS");
reconfig_manager.joinThreads("STORAGE");

// Stop the manager and all threads
reconfig_manager.stop();
*/

#endif // EnvironmentManager_H
