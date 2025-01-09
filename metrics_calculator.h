#ifndef METRICS_CALCULATOR_H
#define METRICS_CALCULATOR_H

#include <chrono>
#include <unordered_map>
#include <vector>
#include <string>
#include <numeric>
#include <map>
#include <cmath>

// Define the Metrics struct
struct Metrics {
    double waiting_time_ms;
    double processing_time_ms;
    double overall_time_ms;
    double total_throughput;         // in MB/s
    double per_buffer_throughput;    // in MB/s
    double waiting_time_stddev;
    double processing_time_stddev;
    double overall_time_stddev;
    double total_throughput_stddev;
    double per_buffer_throughput_stddev;
};

// Helper function to calculate standard deviation
double calculate_stddev(const std::vector<double> &values, double mean) {
    double sum = 0.0;
    for (const auto &value: values) {
        sum += (value - mean) * (value - mean);
    }
    return std::sqrt(sum / values.size());
}

// Function to calculate metrics per component and then aggregate them
std::unordered_map<std::string, Metrics>
calculate_metrics(const std::vector<ProfilingTimestamps> &timestamps, size_t buffer_size_kb) {
    size_t buffer_size_bytes = buffer_size_kb * 1024; // Convert buffer size to bytes
    std::unordered_map<std::string, std::unordered_map<int, std::vector<ProfilingTimestamps>>> events_per_component_thread;

    // Group timestamps by component and thread
    for (const auto &ts: timestamps) {
        events_per_component_thread[ts.component][ts.thread].push_back(ts);
    }

    std::unordered_map<std::string, Metrics> component_metrics;

    // Calculate metrics per component
    for (const auto &[component, events_per_thread]: events_per_component_thread) {
        std::vector<Metrics> thread_metrics;
        size_t total_buffers_processed = 0;

        for (const auto &[thread_id, events]: events_per_thread) {
            Metrics metrics = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

            std::chrono::high_resolution_clock::time_point start_time, push_time, pop_time, end_time;
            std::chrono::duration<double, std::micro> waiting_time = std::chrono::duration<double, std::micro>::zero();
            std::chrono::duration<double, std::micro> processing_time = std::chrono::duration<double, std::micro>::zero();
            bool has_pop_time = false;
            bool has_start_time = false;
            size_t thread_buffers_processed = 0;

            for (const auto &event: events) {
                if (event.event == "start") {
                    start_time = event.timestamp;
                    has_start_time = true;
                } else if (event.event == "pop") {
                    pop_time = event.timestamp;
                    if (has_pop_time) {
                        waiting_time += pop_time - push_time; // Waiting time is pop_time - previous push_time
                    } else if (has_start_time) {
                        waiting_time += pop_time - start_time; // Initial waiting time is pop_time - start_time
                    }
                    has_pop_time = true;
                } else if (event.event == "push") {
                    push_time = event.timestamp;
                    processing_time += push_time - pop_time; // Processing time is push_time - pop_time
                    thread_buffers_processed++;
                } else if (event.event == "end") {
                    end_time = event.timestamp;
                    if (has_pop_time) {
                        processing_time += end_time - push_time; // Finalize the processing time
                    }
                }
            }

            metrics.waiting_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(waiting_time).count();
            metrics.processing_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(processing_time).count();
            metrics.overall_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::duration<double>(end_time - start_time)).count();
            total_buffers_processed += thread_buffers_processed;

            // Calculate the total throughput in bytes per second for this thread
            if (metrics.overall_time_ms > 0) {
                metrics.total_throughput =
                        (thread_buffers_processed * buffer_size_bytes) / (metrics.overall_time_ms / 1000.0);
            }

            // Calculate the per buffer throughput in bytes per second for this thread
            if (metrics.processing_time_ms > 0) {
                metrics.per_buffer_throughput =
                        (thread_buffers_processed * buffer_size_bytes) / (metrics.processing_time_ms / 1000.0);
            }

            // Convert throughput to MB/s
            metrics.total_throughput /= (1024 * 1024);
            metrics.per_buffer_throughput /= (1024 * 1024);

            thread_metrics.push_back(metrics);
        }

        // Aggregate metrics
        Metrics aggregated_metrics;
        size_t num_threads = thread_metrics.size();
        aggregated_metrics.waiting_time_ms = std::accumulate(thread_metrics.begin(), thread_metrics.end(), 0.0,
                                                             [](const auto &sum, const auto &m) {
                                                                 return sum + m.waiting_time_ms;
                                                             }) / num_threads;
        aggregated_metrics.processing_time_ms = std::accumulate(thread_metrics.begin(), thread_metrics.end(), 0.0,
                                                                [](const auto &sum, const auto &m) {
                                                                    return sum + m.processing_time_ms;
                                                                }) / num_threads;
        aggregated_metrics.overall_time_ms = std::accumulate(thread_metrics.begin(), thread_metrics.end(), 0.0,
                                                             [](const auto &sum, const auto &m) {
                                                                 return sum + m.overall_time_ms;
                                                             }) / num_threads;
        aggregated_metrics.total_throughput = std::accumulate(thread_metrics.begin(), thread_metrics.end(), 0.0,
                                                              [](const auto &sum, const auto &m) {
                                                                  return sum + m.total_throughput;
                                                              });
        aggregated_metrics.per_buffer_throughput = std::accumulate(thread_metrics.begin(), thread_metrics.end(), 0.0,
                                                                   [](const auto &sum, const auto &m) {
                                                                       return sum + m.per_buffer_throughput;
                                                                   }) / num_threads;

        // Calculate standard deviations
        std::vector<double> waiting_times, processing_times, overall_times, total_throughputs, per_buffer_throughputs;
        for (const auto &m: thread_metrics) {
            waiting_times.push_back(m.waiting_time_ms);
            processing_times.push_back(m.processing_time_ms);
            overall_times.push_back(m.overall_time_ms);
            total_throughputs.push_back(m.total_throughput);
            per_buffer_throughputs.push_back(m.per_buffer_throughput);
        }

        aggregated_metrics.waiting_time_stddev = calculate_stddev(waiting_times, aggregated_metrics.waiting_time_ms);
        aggregated_metrics.processing_time_stddev = calculate_stddev(processing_times,
                                                                     aggregated_metrics.processing_time_ms);
        aggregated_metrics.overall_time_stddev = calculate_stddev(overall_times, aggregated_metrics.overall_time_ms);
        aggregated_metrics.total_throughput_stddev = calculate_stddev(total_throughputs,
                                                                      aggregated_metrics.total_throughput);
        aggregated_metrics.per_buffer_throughput_stddev = calculate_stddev(per_buffer_throughputs,
                                                                           aggregated_metrics.per_buffer_throughput);

        component_metrics[component] = aggregated_metrics;
    }

    return component_metrics;
}

std::tuple<double, double, double, double> printAndReturnAverageLoad(RuntimeEnv &_xdbcenv) {
    long long totalTimestamps = 0;
    size_t totalReadBufferIdsSize = 0;
    size_t totalDeserBufferIdsSize = 0;
    size_t totalCompressedBufferIdsSize = 0;
    size_t totalNetworkBufferIdsSize = 0;
    size_t recordCount = _xdbcenv.queueSizes.size();

    std::tuple<double, double, double, double> ret(0, 0, 0, 0);
    for (const auto &record: _xdbcenv.queueSizes) {
        totalTimestamps += std::get<0>(record);
        totalReadBufferIdsSize += std::get<1>(record);
        totalDeserBufferIdsSize += std::get<2>(record);
        totalCompressedBufferIdsSize += std::get<3>(record);
        totalNetworkBufferIdsSize += std::get<4>(record);
    }

    if (recordCount > 0) {
        double avgReadBufferIdsSize = static_cast<double>(totalReadBufferIdsSize) / recordCount;
        double avgDeserBufferIdsSize = static_cast<double>(totalDeserBufferIdsSize) / recordCount;
        double avgCompressedBufferIdsSize = static_cast<double>(totalDeserBufferIdsSize) / recordCount;
        double avgNetworkBufferIdsSize = static_cast<double>(totalNetworkBufferIdsSize) / recordCount;

        ret = std::tuple(avgReadBufferIdsSize, avgDeserBufferIdsSize, avgCompressedBufferIdsSize,
                         avgNetworkBufferIdsSize);

        spdlog::get("XDBC.SERVER")->info("Average Load of Queues: Free, Deserializer, Compressor, Sender");
        spdlog::get("XDBC.SERVER")->info("{0}\t{1}\t{2}\t{3}",
                                         avgReadBufferIdsSize, avgDeserBufferIdsSize,
                                         avgCompressedBufferIdsSize, avgNetworkBufferIdsSize);
    } else {
        spdlog::get("XDBC.SERVER")->info("No records available to calculate averages.");
    }

    return ret;
}

#endif // METRICS_CALCULATOR_H
