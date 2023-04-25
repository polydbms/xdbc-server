#include <iostream>
#include "pg.h"
#include <pqxx/pqxx>
#include <chrono>
#include "/usr/include/postgresql/libpq-fe.h"


using namespace std;
using namespace pqxx;

int pg_row() {

    try {

        //start initialization
        auto start = chrono::steady_clock::now();
        vector<tuple<int, int, int, int, double, double, double, double, string, string, string, string, string, string, string, string>> lineitems;
        lineitems.reserve(6001215);
        tuple<int, int, int, int, double, double, double, double, string, string, string, string, string, string, string, string> lineitem;

        auto end = chrono::steady_clock::now();
        cout << "Initialize | Elapsed time in milliseconds: "
             << chrono::duration_cast<chrono::milliseconds>(end - start).count()
             << " ms" << endl;

        //start reading
        start = chrono::steady_clock::now();

        pqxx::connection C("dbname = db1 user = postgres password = 123456 hostaddr = 127.0.0.1 port = 15432");
        work tx(C);
        // stream_from::query
        auto stream = pqxx::stream_from(
                tx, "SELECT name, points FROM score");

        while (stream >> lineitem)
            lineitems.push_back(lineitem);

        stream.complete();
        tx.commit();

        end = chrono::steady_clock::now();
        cout << "Read | Elapsed time in milliseconds: "
             << chrono::duration_cast<chrono::milliseconds>(end - start).count()
             << " ms" << endl;

        //start writing
        start = chrono::steady_clock::now();
        work tx1(C);
        tx1.exec0("TRUNCATE tmp");
        // stream_to::raw_table
        auto stream1 = pqxx::stream_to(tx1, "tmp");

        for (auto const &entry: lineitems)
            stream1 << entry;

        stream1.complete();
        tx1.commit();
        end = chrono::steady_clock::now();
        cout << "Write | Elapsed time in milliseconds: "
             << chrono::duration_cast<chrono::milliseconds>(end - start).count()
             << " ms" << endl;
        //C.close();

    } catch (const std::exception &e) {
        cerr << e.what() << std::endl;
        return 1;

    }

    return 0;
}

int pg_col() {

    //start initialization
    auto start = chrono::steady_clock::now();
    vector<int> l_orderkey;
    l_orderkey.reserve(6001215);
    vector<int> l_partkey;
    l_partkey.reserve(6001215);
    vector<int> l_suppkey;
    l_suppkey.reserve(6001215);
    vector<int> l_linenumber;
    l_linenumber.reserve(6001215);
    vector<double> l_quantity;
    l_quantity.reserve(6001215);
    vector<double> l_extendedprice;
    l_extendedprice.reserve(6001215);
    vector<double> l_discount;
    l_discount.reserve(6001215);
    vector<double> l_tax;
    l_tax.reserve(6001215);
    vector<string> l_returnflag;
    l_returnflag.reserve(6001215);
    vector<string> l_linestatus;
    l_linestatus.reserve(6001215);
    vector<string> l_shipdate;
    l_shipdate.reserve(6001215);
    vector<string> l_commitdate;
    l_commitdate.reserve(6001215);
    vector<string> l_receiptdate;
    l_receiptdate.reserve(6001215);
    vector<string> l_shipinstruct;
    l_shipinstruct.reserve(6001215);
    vector<string> l_shipmode;
    l_shipmode.reserve(6001215);
    vector<string> l_comment;
    l_comment.reserve(6001215);

    tuple<int, int, int, int, double, double, double, double, string, string, string, string, string, string, string, string> lineitem;

    auto end = chrono::steady_clock::now();
    cout << "Initialize | Elapsed time in milliseconds: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;
    try {

        //start reading
        start = chrono::steady_clock::now();

        connection C("dbname = db1 user = postgres password = 123456 hostaddr = 127.0.0.1 port = 15432");
        work tx(C);
        //stream_from::query
        auto stream = pqxx::stream_from(
                tx, "SELECT * FROM pg1_sf1_lineitem");

        while (stream >> lineitem) {
            l_orderkey.push_back(get<0>(lineitem));
            l_partkey.push_back(get<1>(lineitem));
            l_suppkey.push_back(get<2>(lineitem));
            l_linenumber.push_back(get<3>(lineitem));
            l_quantity.push_back(get<4>(lineitem));
            l_extendedprice.push_back(get<5>(lineitem));
            l_discount.push_back(get<6>(lineitem));
            l_tax.push_back(get<7>(lineitem));
            l_returnflag.push_back(get<8>(lineitem));
            l_linestatus.push_back(get<9>(lineitem));
            l_shipdate.push_back(get<10>(lineitem));
            l_commitdate.push_back(get<11>(lineitem));
            l_receiptdate.push_back(get<12>(lineitem));
            l_shipinstruct.push_back(get<13>(lineitem));
            l_shipmode.push_back(get<14>(lineitem));
            l_comment.push_back(get<15>(lineitem));
        }

        stream.complete();
        tx.commit();

        end = chrono::steady_clock::now();
        cout << "Read | Elapsed time in milliseconds: "
             << chrono::duration_cast<chrono::milliseconds>(end - start).count()
             << " ms" << endl;


        //start writing
        start = chrono::steady_clock::now();

        work tx1(C);
        tx1.exec0("TRUNCATE tmp");
        // stream_to::raw_table
        auto stream1 = pqxx::stream_to(tx1, "tmp");

        for (int i = 0; i < l_orderkey.size(); i++)
            stream1 << make_tuple(l_orderkey[i],
                                  l_partkey[i],
                                  l_suppkey[i],
                                  l_linenumber[i],
                                  l_quantity[i],
                                  l_extendedprice[i],
                                  l_discount[i],
                                  l_tax[i],
                                  l_returnflag[i],
                                  l_linestatus[i],
                                  l_shipdate[i],
                                  l_commitdate[i],
                                  l_receiptdate[i],
                                  l_shipinstruct[i],
                                  l_shipmode[i],
                                  l_comment[i]);

        stream1.complete();
        tx1.commit();
        end = chrono::steady_clock::now();
        cout << "Write: Elapsed time in milliseconds: "
             << chrono::duration_cast<chrono::milliseconds>(end - start).count()
             << " ms" << endl;

        //C.close();
    } catch (const std::exception &e) {
        cerr << e.what() << std::endl;
        return 1;

    }

    return 0;
}

struct shortLineitem {
    int l_orderkey;
    int l_partkey;
    int l_suppkey;
    int l_linenumber;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    double l_tax;
};

int pg_copy() {
    const char *conninfo;
    PGconn *connection = NULL;
    char *receiveBuffer = NULL;
    int consumed = 0;
    int receiveLength = 0;
    const int asynchronous = 1;
    PGresult *res;

    conninfo = "dbname = db1 user = postgres password = 123456 hostaddr = 127.0.0.1 port = 15432";
    connection = PQconnectdb(conninfo);
    res = PQexec(connection, "COPY (SELECT * FROM test LIMIT 2) TO STDOUT WITH (FORMAT text, DELIMITER '|')");
    ExecStatusType resType = PQresultStatus(res);

    if (resType == PGRES_COPY_OUT)
        cout << "Result OK" << endl;
    else
        cout << "Result of COPY is " << resType << endl;

    receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);

    char *end;
    size_t len;

    while (receiveLength > 0) {
        int l03[4];
        double l47[4];
        double l4, l5, l6, l7;
        cout << "Received: " << receiveBuffer << endl;

        char *start = receiveBuffer;


        for (int i = 0; i < 7; i++) {

            end = strchr(start, '|');
            len = end - start;
            char tmp[len + 1];
            memcpy(tmp, start, len);
            tmp[len] = '\0';
            //l0 = stoi(tmp);
            start = end + 1;
            if (i < 4) {
                l03[i] = stoi(tmp);

            } else {
                l47[i - 4] = stod(tmp);

            }

        }
        end = strchr(start, '\0');
        len = end - start;
        char tmp[len + 1];
        memcpy(tmp, start, len);
        tmp[len] = '\0';


        l47[3] = stod(tmp);


        shortLineitem t = {l03[0], l03[1], l03[2], l03[3],
                           l47[0], l47[1], l47[2], l47[3]};


        cout << t.l_orderkey << " | "
             << t.l_partkey << " | "
             << t.l_suppkey << " | "
             << t.l_linenumber << " | "
             << t.l_quantity << " | "
             << t.l_extendedprice << " | "
             << t.l_discount << " | "
             << t.l_tax
             << endl;
        PQfreemem(receiveBuffer);

        receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);
    }


    /* we now check the last received length returned by copy data */
    if (receiveLength == 0) {
        /* we cannot read more data without blocking */
        cout << "Received 0" << endl;
    } else if (receiveLength == -1) {
        /* received copy done message */
        PGresult *result = PQgetResult(connection);
        ExecStatusType resultStatus = PQresultStatus(result);

        if (resultStatus == PGRES_COMMAND_OK) {
            cout << "Copy finished " << endl;
        } else {
            cout << "Copy failed" << endl;
        }

        PQclear(result);
    } else if (receiveLength == -2) {
        /* received an error */
        cout << "Copy failed bc -2" << endl;
    }

    /* if copy out completed, make sure we drain all results from libpq */
    if (receiveLength < 0) {
        PGresult *result = PQgetResult(connection);
        while (result != NULL) {
            PQclear(result);
            result = PQgetResult(connection);
        }
    }
    return 1;

}
