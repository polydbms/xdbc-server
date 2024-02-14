#include <absl/numeric/int128.h>
#include <iostream>
#include "pg.h"
#include "ch.h"
#include "xdbcserver.h"
#include <chrono>
#include <thread>
#include <boost/program_options.hpp>
#include <utility>
#include <fstream>
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

using namespace std;
namespace po = boost::program_options;

SchemaAttribute createSchemaAttribute(std::string name, std::string tpe, int size) {
    SchemaAttribute att;
    att.name = std::move(name);
    att.tpe = std::move(tpe);
    att.size = size;
    return att;
}

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
            ("buffer-size,b", po::value<int>()->default_value(1000),
             "Set buffer-size of buffers used to read data from the database.\nDefault: 1000")
            ("bufferpool-size,p", po::value<int>()->default_value(1000),
             "Set the amount of buffers used.\nDefault: 1000")
            ("tuple-size,t", po::value<int>()->default_value(48), "Set the tuple size.\nDefault: 48")
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
             "Set the transfer id.\nDefault: 0");

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
        spdlog::get("XDBC.SERVER")->info("Buffer-size: {0}", vm["buffer-size"].as<int>());
        env.buffer_size = vm["buffer-size"].as<int>();
    }
    if (vm.count("bufferpool-size")) {
        spdlog::get("XDBC.SERVER")->info("Bufferpool-size: {0}", vm["bufferpool-size"].as<int>());
        env.bufferpool_size = vm["bufferpool-size"].as<int>();
    }
    if (vm.count("tuple-size")) {
        spdlog::get("XDBC.SERVER")->info("Tuple size: {0}", vm["tuple-size"].as<int>());
        env.tuple_size = vm["tuple-size"].as<int>();
    }
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

    //create schema
    std::vector<SchemaAttribute> schema;
    schema.emplace_back(createSchemaAttribute("l_orderkey", "INT", 4));
    schema.emplace_back(createSchemaAttribute("l_partkey", "INT", 4));
    schema.emplace_back(createSchemaAttribute("l_suppkey", "INT", 4));
    schema.emplace_back(createSchemaAttribute("l_linenumber", "INT", 4));
    schema.emplace_back(createSchemaAttribute("l_quantity", "DOUBLE", 8));
    schema.emplace_back(createSchemaAttribute("l_extendedprice", "DOUBLE", 8));
    schema.emplace_back(createSchemaAttribute("l_discount", "DOUBLE", 8));
    schema.emplace_back(createSchemaAttribute("l_tax", "DOUBLE", 8));

    env.read_time = 0;
    env.deser_time = 0;
    env.compression_time = 0;
    env.network_time = 0;

    env.read_wait_time = 0;
    env.deser_wait_time = 0;
    env.compression_wait_time = 0;
    env.network_wait_time = 0;

    env.schema = schema;

}

int main(int argc, char *argv[]) {

    auto console = spdlog::stdout_color_mt("XDBC.SERVER");

    RuntimeEnv xdbcEnv;
    handleCMDParams(argc, argv, xdbcEnv);

    int option = 5;

    auto start = std::chrono::steady_clock::now();
    string op = "";
    //TODO: Refactor legacy benchmarks to different module
    switch (option) {

        case 1:
            op = "pg row";
            pg_row();
            break;
        case 2:
            op = "pg col";
            pg_col();
            break;
        case 3:
            op = "ch row";
            ch_row();
            break;
        case 4:
            op = "ch col";
            ch_col();
            break;
        case 5: {
            op = "xdbc server";
            XDBCServer xdbcserver = XDBCServer(xdbcEnv);
            xdbcserver.serve();
            break;
        }
        case 6: {
            op = "pg copy";
            pg_copy();
            break;
        }
        default:
            cout << "No valid option" << endl;
            break;
    }
    auto end = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    spdlog::get("XDBC.SERVER")->info("{0} | Total elapsed time: {1} ms", op, total_time);

    long long read_wait_time = xdbcEnv.read_wait_time.load(std::memory_order_relaxed) / 1000 / xdbcEnv.read_parallelism;
    long long read_time = (xdbcEnv.read_time.load(std::memory_order_relaxed) / 1000 - read_wait_time);

    long long deser_wait_time =
            xdbcEnv.deser_wait_time.load(std::memory_order_relaxed) / 1000 / xdbcEnv.deser_parallelism;
    long long deser_time = (xdbcEnv.deser_time.load(std::memory_order_relaxed) / 1000 - deser_wait_time);

    long long cmp_wait_time =
            xdbcEnv.compression_wait_time.load(std::memory_order_relaxed) / 1000 / xdbcEnv.compression_parallelism;
    long long cmp_time = (xdbcEnv.compression_time.load(std::memory_order_relaxed) / 1000 - cmp_wait_time);

    long long net_wait_time =
            xdbcEnv.network_wait_time.load(std::memory_order_relaxed) / 1000 / xdbcEnv.network_parallelism;
    long long net_time = (xdbcEnv.network_time.load(std::memory_order_relaxed) / 1000 - net_wait_time);

    spdlog::get("XDBC.SERVER")->info(
            "{0} | read time: {1} ms, deser time: {2} ms, compress time {3} ms, network time {4} ms",
            op, read_time, deser_time, cmp_time, net_time);

    spdlog::get("XDBC.SERVER")->info(
            "{0} | read wait time: {1} ms, deser wait time: {2} ms, compress wait time {3} ms, network wait time {4} ms",
            op, read_wait_time, deser_wait_time, cmp_wait_time, net_wait_time);

    std::ofstream csv_file("/tmp/xdbc_server_timings.csv",
                           std::ios::out | std::ios::app);

    csv_file << std::fixed << std::setprecision(2)
             << std::to_string(xdbcEnv.transfer_id) << "," << total_time << ","
             << read_wait_time << ","
             << read_time << ","
             << deser_wait_time << ","
             << deser_time << ","
             << cmp_wait_time << ","
             << cmp_time << ","
             << net_wait_time << ","
             << net_time << "\n";
    csv_file.close();

    return 0;
}


class Lineitem {
public:
    int l_orderkey;
    int l_partkey;
    int l_suppkey;
    int l_linenumber;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    double l_tax;
    string l_returnflag;
    string l_linestatus;
    string l_shipdate;
    string l_commitdate;
    string l_receiptdate;
    string l_shipinstruct;
    string l_shipmode;
    string l_comment;

    Lineitem() {}
};

class Supplier {
public:
    int s_suppkey;
    string s_name;
    string s_address;
    int s_nationkey;
    string s_phone;
    float s_acctbal;
    string s_comment;

    Supplier() {}
};