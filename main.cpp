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
            ("compression-type,c", po::value<string>()->default_value("nocomp"),
             "Set Compression algorithm: \nDefault:\n  nocomp\nOther:\n  zstd\n  snappy\n  lzo\n  lz4\n zlib")
            ("intermediate-format,f", po::value<int>()->default_value(1),
             "Set intermediate-format: \nDefault:\n  1 (row)\nOther:\n  2 (col)")
            ("buffer-size,b", po::value<int>()->default_value(1000),
             "Set buffer-size of buffers used to read data from the database.\nDefault: 1000")
            ("bufferpool-size,p", po::value<int>()->default_value(1000),
             "Set the amount of buffers used.\nDefault: 1000")
            ("tuple-size,t", po::value<int>()->default_value(48), "Set the tuple size.\nDefault: 48")
            ("sleep-time,s", po::value<int>()->default_value(5), "Set a sleep-time in milli seconds.\nDefault: 5ms")
            ("parallelism,P", po::value<int>()->default_value(4), "Set the parallelism grade.\nDefault: 4");

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
        env.sleep_time = vm["sleep-time"].as<int>();
    }
    if (vm.count("parallelism")) {
        cout << "Parallelism "
             << vm["parallelism"].as<int>() << ".\n";
        env.parallelism = vm["parallelism"].as<int>();
    }

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