// EnvironmentManager.cpp
#include "EnvironmentManager.h"

EnvironmentManager::EnvironmentManager() : terminate_(false), config_update_(false), config_over_(false) {}

EnvironmentManager::~EnvironmentManager()
{
    stop(); // Ensure all threads are stopped before destruction
}

void EnvironmentManager::registerOperation(const std::string &name, Task task, std::shared_ptr<customQueue<int>> poisonQueue)
{
    std::unique_lock<std::mutex> lock(mutex_);
    operations_[name] = {task, poisonQueue, 0, 0};
    // cv_.notify_all(); // Notify that a new operation is registered
}

void EnvironmentManager::configureThreads(const std::string &name, int new_thread_count)
{
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = operations_.find(name);
    if (it != operations_.end())
    {
        it->second.desired_threads = new_thread_count;
        config_update_ = true;
        config_over_ = false;
        cv_.notify_all();

        // Wait until all requested threads are actually started

        cv_.wait(lock, [this]
                 { return config_over_.load(); });
    }
}

void EnvironmentManager::start()
{
    reconfig_thread_ = std::thread(&EnvironmentManager::run, this);
}

void EnvironmentManager::joinThreads(const std::string &name)
{
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = operations_.find(name);
    if (it != operations_.end())
    {
        Operation &op = it->second;

        for (auto &thread : op.threads)
        {
            if (thread.joinable())
            {
                thread.join(); // Wait for the thread to finish
            }
            else
            {
                spdlog::info("Thread with ID: {} is not joinable.", std::hash<std::thread::id>{}(thread.get_id()));
            }
        }

        op.threads.clear();     // Clear the threads after joining
        op.active_threads = 0;  // Reset the active thread count
        op.desired_threads = 0; // Reset the desired thread count
    }
    else
    {
        spdlog::warn("Operation '{}' not found. No threads to join.", name);
    }
}

void EnvironmentManager::stop()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        terminate_ = true;
        cv_.notify_all();
    }

    if (reconfig_thread_.joinable())
    {
        reconfig_thread_.join();
    }

    // Join all threads before exiting
    for (auto &op : operations_)
    {
        for (auto &t : op.second.threads)
        {
            if (t.joinable())
            {
                t.join();
            }
        }
    }
}

void EnvironmentManager::run()
{
    while (!terminate_)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]
                 { return terminate_.load() || config_update_; });

        config_update_ = false;

        for (auto &[name, operation] : operations_)
        {
            int delta_threads = operation.desired_threads - operation.active_threads;

            if (delta_threads > 0)
            {

                for (int i = 0; i < delta_threads; ++i)
                {
                    int thread_id = operation.active_threads + i;

                    if (!operation.task)
                    {
                        spdlog::error("Task is null for operation {}", name);
                        continue;
                    }

                    // Push a new thread instead of accessing via index
                    operation.threads.emplace_back([this, task = operation.task, thread_id, name]
                                                   {
                        try
                        {
                            task(thread_id);
                        }
                        catch (const std::exception &e)
                        {
                            spdlog::error("Exception in thread {}: {}", thread_id, e.what());
                        }
                        catch (...)
                        {
                            spdlog::error("Unknown exception in thread {}", thread_id);
                        } });
                }
                spdlog::info("Reconfigure thread for operation {0} by {1}", name, delta_threads);
            }
            else if (delta_threads < 0)
            {

                for (int i = 0; i < -delta_threads; ++i)
                {
                    if (!operation.poisonQueue)
                    {
                        spdlog::error("poisonQueue is null for operation {}", name);
                        continue;
                    }
                    operation.poisonQueue->push(-1);
                }
                spdlog::info("Reconfigure thread for operation {0} by {1}", name, delta_threads);
            }

            operation.active_threads = operation.desired_threads;
        }
        config_over_ = true;
        cv_.notify_all();
    }
}
