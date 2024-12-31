#include <absl/numeric/int128.h>
#include <iostream>
#include "xdbcserver.h"
#include <chrono>
#include <boost/program_options.hpp>
#include <fstream>
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "metrics_calculator.h"

using namespace std;
namespace po = boost::program_options;


void handleCMDParams(int ac, char *av[], RuntimeEnv &env) {
    // Declare the supported options.
    po::options_description desc("Usage: ./xdbc-server [options]\n\nAllowed options");
    desc.add_options()
            ("help,h", "Produce this help message.")
            ("system,y", po::value<string>()->default_value("csv"),
             "Set system: \nDefault:\n  csv\nOther:\n  postgres, clickhouse")
            ("compression-type,c", po::value<string>()->default_value("nocomp"),
             "Set Compression algorithm: \nDefault:\n  nocomp\nOther:\n  zstd\n  snappy\n  lzo\n  lz4\n zlib\n cols")
            ("intermediate-format,f", po::value<int>()->default_value(1),
             "Set intermediate-format: \nDefault:\n  1 (row)\nOther:\n  2 (col)")
            ("buffer-size,b", po::value<int>()->default_value(64),
             "Set buffer-size of buffers (in KiB).\nDefault: 64")
            ("bufferpool-size,p", po::value<int>()->default_value(4096),
             "Set bufferpool memory size (in KiB).\nDefault: 4096")
            //("tuple-size,t", po::value<int>()->default_value(48), "Set the tuple size.\nDefault: 48")
            ("sleep-time,s", po::value<int>()->default_value(5), "Set a sleep-time in milli seconds.\nDefault: 5ms")
            ("read-parallelism,rp", po::value<int>()->default_value(4), "Set the read parallelism grade.\nDefault: 4")
            ("read-partitions,rpp", po::value<int>()->default_value(1),
             "Set the number of read partitions.\nDefault: 1")
            ("deser-parallelism,dp", po::value<int>()->default_value(1),
             "Set the number of deserialization parallelism.\nDefault: 1")
            ("network-parallelism,np", po::value<int>()->default_value(1),
             "Set the send parallelism grade.\nDefault: 4")
            ("compression-parallelism,cp", po::value<int>()->default_value(1),
             "Set the compression parallelism grade.\nDefault: 1")
            ("transfer-id,tid", po::value<long>()->default_value(0),
             "Set the transfer id.\nDefault: 0")
            ("profiling-interval", po::value<int>()->default_value(1000),
             "Set profiling interval.\nDefault: 1000");

    po::positional_options_description p;
    p.add("compression-type", 1);

    po::variables_map vm;
    po::store(po::command_line_parser(ac, av).options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
        exit(0);
    }


    if (vm.count("system")) {
        spdlog::get("XDBC.SERVER")->info("system: {0}", vm["system"].as<string>());
        env.system = vm["system"].as<string>();
    }

    if (vm.count("intermediate-format")) {
        spdlog::get("XDBC.SERVER")->info("Intermediate format: {0}", vm["intermediate-format"].as<int>());
        env.iformat = vm["intermediate-format"].as<int>();
    }

    if (vm.count("compression-type")) {
        spdlog::get("XDBC.SERVER")->info("Compression algorithm: {0}", vm["compression-type"].as<string>());
        env.compression_algorithm = vm["compression-type"].as<string>();
    }
    if (vm.count("buffer-size")) {
        spdlog::get("XDBC.SERVER")->info("Buffer-size: {0} KiB", vm["buffer-size"].as<int>());
        env.buffer_size = vm["buffer-size"].as<int>();
    }
    if (vm.count("bufferpool-size")) {
        spdlog::get("XDBC.SERVER")->info("Bufferpool-size: {0} KiB", vm["bufferpool-size"].as<int>());
        env.buffers_in_bufferpool = vm["bufferpool-size"].as<int>() / vm["buffer-size"].as<int>();
        spdlog::get("XDBC.SERVER")->info("Buffers in Bufferpool: {0}", env.buffers_in_bufferpool);
    }
    /*if (vm.count("tuple-size")) {
        spdlog::get("XDBC.SERVER")->info("Tuple size: {0}", vm["tuple-size"].as<int>());
        env.tuple_size = vm["tuple-size"].as<int>();
    }*/
    if (vm.count("sleep-time")) {
        spdlog::get("XDBC.SERVER")->info("Sleep time: {0}ms", vm["sleep-time"].as<int>());
        env.sleep_time = std::chrono::milliseconds(vm["sleep-time"].as<int>());
    }
    if (vm.count("read-parallelism")) {
        spdlog::get("XDBC.SERVER")->info("Read parallelism: {0}", vm["read-parallelism"].as<int>());
        env.read_parallelism = vm["read-parallelism"].as<int>();
    }
    if (vm.count("read-partitions")) {
        spdlog::get("XDBC.SERVER")->info("Read partitions: {0}", vm["read-partitions"].as<int>());
        env.read_partitions = vm["read-partitions"].as<int>();
    }
    if (vm.count("network-parallelism")) {
        spdlog::get("XDBC.SERVER")->info("Network parallelism: {0}", vm["network-parallelism"].as<int>());
        env.network_parallelism = vm["network-parallelism"].as<int>();
    }
    if (vm.count("deser-parallelism")) {
        spdlog::get("XDBC.SERVER")->info("Deserialization parallelism: {0}", vm["deser-parallelism"].as<int>());
        env.deser_parallelism = vm["deser-parallelism"].as<int>();
    }
    if (vm.count("compression-parallelism")) {
        spdlog::get("XDBC.SERVER")->info("Compression parallelism: {0}", vm["compression-parallelism"].as<int>());
        env.compression_parallelism = vm["compression-parallelism"].as<int>();
    }
    if (vm.count("transfer-id")) {
        spdlog::get("XDBC.SERVER")->info("Transfer id: {0}", vm["transfer-id"].as<long>());
        env.transfer_id = vm["transfer-id"].as<long>();
    }
    if (vm.count("profiling-interval")) {
        spdlog::get("XDBC.SERVER")->info("Profiling interval: {0}", vm["profiling-interval"].as<int>());
        env.profilingInterval = vm["profiling-interval"].as<int>();
    }

    env.tuple_size = 0;
    env.tuples_per_buffer = 0;


}


int main(int argc, char *argv[]) {

    auto console = spdlog::stdout_color_mt("XDBC.SERVER");

    RuntimeEnv xdbcEnv;
    handleCMDParams(argc, argv, xdbcEnv);

    auto start = std::chrono::steady_clock::now();

    XDBCServer xdbcserver = XDBCServer(xdbcEnv);
    xdbcserver.serve();

    auto end = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    spdlog::get("XDBC.SERVER")->info("Total elapsed time: {} ms", total_time);

    auto pts = std::vector<ProfilingTimestamps>(xdbcEnv.pts->size());
    while (xdbcEnv.pts->size() != 0)
        pts.push_back(xdbcEnv.pts->pop());

    auto component_metrics = calculate_metrics(pts, xdbcEnv.buffer_size);
    std::ostringstream totalTimes;
    std::ostringstream procTimes;
    std::ostringstream waitingTimes;
    std::ostringstream totalThroughput;
    std::ostringstream perBufferThroughput;

    for (const auto &[component, metrics]: component_metrics) {

        if (!component.empty()) {
            totalTimes << component << ":\t" << metrics.overall_time_ms << "ms, ";
            procTimes << component << ":\t" << metrics.processing_time_ms << "ms, ";
            waitingTimes << component << ":\t" << metrics.waiting_time_ms << "ms, ";
            totalThroughput << component << ":\t" << metrics.total_throughput << "mb/s, ";
            perBufferThroughput << component << ":\t" << metrics.per_buffer_throughput << "mb/s, ";
        }

    }

    spdlog::get("XDBC.SERVER")->info(
            "xdbc server | \n all:\t {} \n proc:\t{} \n wait:\t{} \n thr:\t {} \n thr/b:\t {}",
            totalTimes.str(), procTimes.str(), waitingTimes.str(), totalThroughput.str(), perBufferThroughput.str());

    auto loads = printAndReturnAverageLoad(xdbcEnv);

    const std::string filename = "/tmp/xdbc_server_timings.csv";

    std::ostringstream headerStream;
    headerStream << "transfer_id,total_time,"
                 << "read_wait_time,read_proc_time,read_throughput,read_throughput_pb,read_load,"
                 << "deser_wait_time,deser_proc_time,deser_throughput,deser_throughput_pb,deser_load,"
                 << "comp_wait_time,comp_proc_time,comp_throughput,comp_throughput_pb,comp_load,"
                 << "send_wait_time,send_proc_time,send_throughput,send_throughput_pb,send_load\n";

    std::ifstream file_check(filename);
    bool is_empty = file_check.peek() == std::ifstream::traits_type::eof();
    file_check.close();

    std::ofstream csv_file(filename,
                           std::ios::out | std::ios::app);

    if (is_empty)
        csv_file << headerStream.str();

    csv_file << std::fixed << std::setprecision(2)
             << std::to_string(xdbcEnv.transfer_id) << "," << total_time << ","
             << component_metrics["read"].waiting_time_ms << ","
             << component_metrics["read"].processing_time_ms << ","
             << component_metrics["read"].total_throughput << ","
             << component_metrics["read"].per_buffer_throughput << ","
             << std::get<0>(loads) << ","
             << component_metrics["deser"].waiting_time_ms << ","
             << component_metrics["deser"].processing_time_ms << ","
             << component_metrics["deser"].total_throughput << ","
             << component_metrics["deser"].per_buffer_throughput << ","
             << std::get<1>(loads) << ","
             << component_metrics["comp"].waiting_time_ms << ","
             << component_metrics["comp"].processing_time_ms << ","
             << component_metrics["comp"].total_throughput << ","
             << component_metrics["comp"].per_buffer_throughput << ","
             << std::get<2>(loads) << ","
             << component_metrics["send"].waiting_time_ms << ","
             << component_metrics["send"].processing_time_ms << ","
             << component_metrics["send"].total_throughput << ","
             << component_metrics["send"].per_buffer_throughput << ","
             << std::get<3>(loads) << "\n";
    csv_file.close();

    return 0;
}
