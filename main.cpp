#include <absl/numeric/int128.h>
#include <iostream>
#include "pg.h"
#include "ch.h"
#include "pgserver.h"
#include <chrono>
#include <thread>
#include <boost/program_options.hpp>

using namespace std;
namespace po = boost::program_options;

int main() {

    int option = 5;

    auto start = std::chrono::steady_clock::now();
    string op = "";
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
            op = "pg server";
            PGServer pgserver = PGServer();

            pgserver.serve();
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

    cout << op << " | Total Elapsed time in milliseconds: "
         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
         << " ms" << endl;


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