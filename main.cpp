#include <absl/numeric/int128.h>
#include <iostream>
#include "pg.h"
#include "ch.h"
#include "xdbcserver.h"
#include <chrono>
#include <thread>
#include <boost/program_options.hpp>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

using namespace std;
namespace po = boost::program_options;

RuntimeEnv handleCMDParams(int ac, char *av[]) {
    // Declare the supported options.
    po::options_description desc("Usage: ./xdbc-server [options]\n\nAllowed options");
    desc.add_options()
            ("help,h", "Produce this help message.")
            ("system,y", po::value<string>()->default_value("postgres"),
             "Set system: \nDefault:\n  postgres\nOther:\n  clickhouse")
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
            ("network-parallelism,np", po::value<int>()->default_value(4),
             "Set the send parallelism grade.\nDefault: 4");

    po::positional_options_description p;
    p.add("compression-type", 1);

    po::variables_map vm;
    po::store(po::command_line_parser(ac, av).options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
        exit(0);
    }

    RuntimeEnv env;

    if (vm.count("system")) {
        cout << "System "
             << vm["system"].as<string>() << ".\n";
        env.system = vm["system"].as<string>();
    }

    if (vm.count("intermediate-format")) {
        cout << "Intermediate format "
             << vm["intermediate-format"].as<int>() << ".\n";
        env.iformat = vm["intermediate-format"].as<int>();
    }

    if (vm.count("compression-type")) {
        cout << "Compression algorithm was set to "
             << vm["compression-type"].as<string>() << ".\n";
        env.compression_algorithm = vm["compression-type"].as<string>();
    }
    if (vm.count("buffer-size")) {
        cout << "Buffer-size: "
             << vm["buffer-size"].as<int>() << ".\n";
        env.buffer_size = vm["buffer-size"].as<int>();
    }
    if (vm.count("bufferpool-size")) {
        cout << "Bufferpool-size: "
             << vm["bufferpool-size"].as<int>() << ".\n";
        env.bufferpool_size = vm["bufferpool-size"].as<int>();
    }
    if (vm.count("tuple-size")) {
        cout << "Tuple-size "
             << vm["tuple-size"].as<int>() << ".\n";
        env.tuple_size = vm["tuple-size"].as<int>();
    }
    if (vm.count("sleep-time")) {
        cout << "Sleep-time "
             << vm["sleep-time"].as<int>() << "ms.\n";
        env.sleep_time = std::chrono::milliseconds(vm["sleep-time"].as<int>());
    }
    if (vm.count("read-parallelism")) {
        cout << "Read Parallelism "
             << vm["read-parallelism"].as<int>() << ".\n";
        env.read_parallelism = vm["read-parallelism"].as<int>();
    }
    if (vm.count("network-parallelism")) {
        cout << "Network Parallelism "
             << vm["network-parallelism"].as<int>() << ".\n";
        env.network_parallelism = vm["network-parallelism"].as<int>();
    }

    //create schema
    std::vector<std::tuple<std::string, std::string, int>> schema;
    schema.emplace_back("l_orderkey", "INT", 4);
    schema.emplace_back("l_partkey", "INT", 4);
    schema.emplace_back("l_suppkey", "INT", 4);
    schema.emplace_back("l_linenumber", "INT", 4);
    schema.emplace_back("l_quantity", "DOUBLE", 8);
    schema.emplace_back("l_extendedprice", "DOUBLE", 8);
    schema.emplace_back("l_discount", "DOUBLE", 8);
    schema.emplace_back("l_tax", "DOUBLE", 8);


    env.schema = schema;
    return env;
}

int main(int argc, char *argv[]) {

    RuntimeEnv xdbcEnv = handleCMDParams(argc, argv);

    int option = 5;

    auto start = std::chrono::steady_clock::now();
    string op = "";
    //TODO: benchmark all baselines libpq, libpqxx, csv, binary etc
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
            auto console = spdlog::stdout_color_mt("XDBC.SERVER");
            XDBCServer xdbcserver = XDBCServer(xdbcEnv);

            xdbcserver.serve(xdbcEnv.network_parallelism);
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

    spdlog::get("XDBC.SERVER")->info("{0} | Total elapsed time: {1} ms",
                                     op, std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());

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