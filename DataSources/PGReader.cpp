#include "PGReader.h"
#include "/usr/include/postgresql/libpq-fe.h"
#include <pqxx/pqxx>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include "spdlog/spdlog.h"

using namespace std;
using namespace pqxx;
using namespace boost::asio;
using ip::tcp;

void PGReader::printSl(shortLineitem *t) {
    cout << t->l_orderkey << " | "
         << t->l_partkey << " | "
         << t->l_suppkey << " | "
         << t->l_linenumber << " | "
         << t->l_quantity << " | "
         << t->l_extendedprice << " | "
         << t->l_discount << " | "
         << t->l_tax
         << endl;
}

PGReader::PGReader(std::string connectionString,
                   RuntimeEnv &xdbcEnv,
                   std::string tableName) : _connectionString(connectionString),
                                            flagArr(*xdbcEnv.flagArrPtr),
                                            bp(*xdbcEnv.bpPtr),
                                            totalReadBuffers(0),
                                            finishedReading(false),
                                            xdbcEnv(&xdbcEnv),
                                            tableName(tableName) {

}

int PGReader::getMaxCtId(std::string tableName) {

    const char *conninfo;
    PGconn *connection = NULL;

    conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
    connection = PQconnectdb(conninfo);

    PGresult *res;
    std::string qStr = "SELECT (MAX(ctid)::text::point)[0]::bigint AS maxctid FROM " + tableName;
    res = PQexec(connection, qStr.c_str());

    int fnum = PQfnumber(res, "maxctid");

    char *maxPtr = PQgetvalue(res, 0, fnum);

    int maxCtId = stoi(maxPtr);

    PQfinish(connection);
    return maxCtId;
}

void PGReader::readData(const std::string &tableName, int x) {
    auto start = std::chrono::steady_clock::now();
    int totalCnt = 0;

    if (x == 1) {
        cout << "Using pqxx::stream_from" << endl;
        //vector<lineitem> ls;
        //array<shortLineitem, BUFFER_SIZE> ls;

        tuple<int, int, int, int, double, double, double, double> lineitemTuple;

        connection C("dbname = db1 user = postgres password = 123456 host = pg1 port = 5432");
        work tx(C);

        //stream_from stream{tx, pqxx::from_table, tableName};
        stream_from stream{tx, tableName};


        int bufferTupleId = 0;
        int bufferId = 0;
        while (stream >> lineitemTuple) {


            /*shortLineitem l{get<0>(lineitemTuple),
                            get<1>(lineitemTuple),
                            get<2>(lineitemTuple),
                            get<3>(lineitemTuple),
                            get<4>(lineitemTuple),
                            get<5>(lineitemTuple),
                            get<6>(lineitemTuple),
                            get<7>(lineitemTuple),
            };*/
            bp[bufferId][bufferTupleId] = {get<0>(lineitemTuple),
                                           get<1>(lineitemTuple),
                                           get<2>(lineitemTuple),
                                           get<3>(lineitemTuple),
                                           get<4>(lineitemTuple),
                                           get<5>(lineitemTuple),
                                           get<6>(lineitemTuple),
                                           get<7>(lineitemTuple),
            };
            totalCnt++;
            bufferTupleId++;

            if (bufferTupleId == xdbcEnv->buffer_size) {
                //cout << "wrote buffer " << bufferId << endl;
                bufferTupleId = 0;
                flagArr[bufferId] = 0;

                bufferId++;

                if (bufferId == xdbcEnv->bufferpool_size)
                    bufferId = 0;

            }

        }

        stream.complete();
        tx.commit();
    } else if (x == 2) {
        cout << "Using libpq with PQexec" << endl;
        const char *conninfo;
        PGconn *conn;
        PGresult *res;
        int nFields;
        int l0_fnum, l1_fnum, l2_fnum, l3_fnum, l4_fnum, l5_fnum, l6_fnum, l7_fnum;

        int i, bufferTupleId;

        int bufferId = 0;

        //TODO: explore PQsetSingleRowMode();

        conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
        conn = PQconnectdb(conninfo);
        res = PQexec(conn, ("SELECT * FROM " + tableName).c_str());


        l0_fnum = PQfnumber(res, "l_orderkey");
        l1_fnum = PQfnumber(res, "l_partkey");
        l2_fnum = PQfnumber(res, "l_suppkey");
        l3_fnum = PQfnumber(res, "l_linenumber");
        l4_fnum = PQfnumber(res, "l_quantity");
        l5_fnum = PQfnumber(res, "l_extendedprice");
        l6_fnum = PQfnumber(res, "l_discount");
        l7_fnum = PQfnumber(res, "l_tax");

        //totalTuples = PQntuples(res);

        for (i = 0; i < PQntuples(res); i++) {
            char *l0ptr;
            char *l1ptr;
            char *l2ptr;
            char *l3ptr;
            char *l4ptr;
            char *l5ptr;
            char *l6ptr;
            char *l7ptr;
            int l0val, l1val, l2val, l3val;
            double l4val, l5val, l6val, l7val;
            //for binary, change endianness:
            // l7val = double_swap(*((double *) l4ptr));
            // l3val = ntohl(*((uint32_t *) l3ptr));
            l0ptr = PQgetvalue(res, i, l0_fnum);
            l1ptr = PQgetvalue(res, i, l1_fnum);
            l2ptr = PQgetvalue(res, i, l2_fnum);
            l3ptr = PQgetvalue(res, i, l3_fnum);
            l4ptr = PQgetvalue(res, i, l4_fnum);
            l5ptr = PQgetvalue(res, i, l5_fnum);
            l6ptr = PQgetvalue(res, i, l6_fnum);
            l7ptr = PQgetvalue(res, i, l7_fnum);

            l0val = stoi(l0ptr);
            l1val = stoi(l1ptr);
            l2val = stoi(l2ptr);
            l3val = stoi(l3ptr);
            l4val = stod(l4ptr);
            l5val = stod(l5ptr);
            l6val = stod(l6ptr);
            l7val = stod(l7ptr);

            /*bp[bufferId][bufferTupleId] = {stoi(PQgetvalue(res, i, l0_fnum)),
                        stoi(PQgetvalue(res, i, l1_fnum)),
                        stoi(PQgetvalue(res, i, l2_fnum)),
                        stoi(PQgetvalue(res, i, l3_fnum)),
                        stod(PQgetvalue(res, i, l4_fnum)),
                        stod(PQgetvalue(res, i, l5_fnum)),
                        stod(PQgetvalue(res, i, l6_fnum)),
                        stod(PQgetvalue(res, i, l7_fnum))
        };*/
            int sleepCtr = 0;
            while (flagArr[bufferId] == 0) {
                if (sleepCtr == 1000) {
                    sleepCtr = 0;
                    cout << "Read: Stuck at buffer " << bufferId << " not ready to be written at tuple " << totalCnt
                         << " and tuple " << i << endl;
                }
                //std::this_thread::sleep_for(SLEEP_TIME);
                sleepCtr++;
            }

            bp[bufferId][bufferTupleId] = {l0val,
                                           l1val,
                                           l2val,
                                           l3val,
                                           l4val,
                                           l5val,
                                           l6val,
                                           l7val
            };

            totalCnt++;
            bufferTupleId++;

            if (bufferTupleId == xdbcEnv->buffer_size) {
                //cout << "wrote buffer " << bufferId << endl;
                bufferTupleId = 0;
                flagArr[bufferId] = 0;

                bufferId++;

                if (bufferId == xdbcEnv->bufferpool_size)
                    bufferId = 0;

            }
        }

        PQfinish(conn);

    } else if (x == 3) {

        spdlog::get("XDBC.SERVER")->info("Using pglib with COPY, parallelism: {0}", xdbcEnv->parallelism);
        spdlog::get("XDBC.SERVER")->info("Using compression: {0}", xdbcEnv->compression_algorithm);

        int threadWrittenTuples[xdbcEnv->parallelism];
        int threadWrittenBuffers[xdbcEnv->parallelism];
        thread threads[xdbcEnv->parallelism];

        // TODO: throw something when table does not exist

        //cout << "deciding partitioning" << endl;
        int maxCtId = getMaxCtId(tableName);

        //cout << "partitioning upper bound: " << maxCtId << endl;
        div_t div1 = div(maxCtId, xdbcEnv->parallelism);
        int partSize = div1.quot;
        if (div1.rem > 0)
            partSize += 1;

        //cout << "starting threads" << endl;
        for (int i = 0; i < xdbcEnv->parallelism; i++) {

            int startOff = i * partSize;
            long endOff = ((i + 1) * partSize) ;

            if (i == xdbcEnv->parallelism - 1)
                endOff = UINT32_MAX;

            threads[i] = std::thread(&PGReader::pqWriteToBp, this, i, startOff, endOff,
                                     std::ref(threadWrittenTuples[i]), std::ref(threadWrittenBuffers[i]));
            threadWrittenTuples[i] = 0;
            threadWrittenBuffers[i] = 0;

        }

        //cout << "Read threads spawned" << endl;

        int total = 0;
        for (int i = 0; i < xdbcEnv->parallelism; i++) {
            threads[i].join();
            total += threadWrittenTuples[i];
        }
        finishedReading = true;
        totalCnt += total;
    } else if (x == 4) {

        cout << "Using pglib with COPY" << endl;
        const char *conninfo;
        PGconn *connection = NULL;
        char *receiveBuffer = NULL;
        int receiveLength = 0;
        const int asynchronous = 0;
        PGresult *res;

        int bufferTupleId;

        int bufferId = 0;

        conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
        connection = PQconnectdb(conninfo);
        res = PQexec(connection,
                     ("COPY (SELECT * FROM " + tableName + ") TO STDOUT WITH (FORMAT text, DELIMITER '|')").c_str());
        ExecStatusType resType = PQresultStatus(res);

        if (resType == PGRES_COPY_OUT)
            cout << "Result OK" << endl;
        else
            cout << "Result of COPY is " << resType << endl;


        receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);


        char *endPtr;
        size_t len;

        while (receiveLength > 0) {
            //cout << "rcv len = " << receiveLength << endl;
            int l03[4];
            double l47[4];

            char *startPtr = receiveBuffer;


            for (int i = 0; i < 7; i++) {

                endPtr = strchr(startPtr, '|');
                len = endPtr - startPtr;
                char tmp[len + 1];
                memcpy(tmp, startPtr, len);
                tmp[len] = '\0';
                //l0 = stoi(tmp);
                startPtr = endPtr + 1;
                if (i < 4) {
                    l03[i] = stoi(tmp);

                } else {
                    l47[i - 4] = stod(tmp);

                }

            }
            endPtr = strchr(startPtr, '\0');
            len = endPtr - startPtr;
            char tmp[len + 1];
            memcpy(tmp, startPtr, len);
            tmp[len] = '\0';


            l47[3] = stod(tmp);


            int sleepCtr = 0;
            while (flagArr[bufferId] == 0) {
                if (sleepCtr == 1000) {
                    sleepCtr = 0;
                    cout << "Read: Stuck at buffer " << bufferId << " not ready to be written at tuple " << totalCnt
                         << " and tuple " << totalCnt << endl;
                }
                std::this_thread::sleep_for(SLEEP_TIME);
                sleepCtr++;
            }


            bp[bufferId][bufferTupleId] = {l03[0], l03[1], l03[2], l03[3],
                                           l47[0], l47[1], l47[2], l47[3]};


            totalCnt++;
            bufferTupleId++;

            if (bufferTupleId == xdbcEnv->buffer_size) {
                //cout << "wrote buffer " << bufferId << endl;
                bufferTupleId = 0;
                flagArr[bufferId] = 0;

                totalReadBuffers.fetch_add(1);

                bufferId++;

                if (bufferId == xdbcEnv->bufferpool_size)
                    bufferId = 0;

            }

            /*cout << t.l_orderkey << " | "
                 << t.l_partkey << " | "
                 << t.l_suppkey << " | "
                 << t.l_linenumber << " | "
                 << t.l_quantity << " | "
                 << t.l_extendedprice << " | "
                 << t.l_discount << " | "
                 << t.l_tax
                 << endl;*/
            PQfreemem(receiveBuffer);

            receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);
        }
        finishedReading = true;

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
    }


    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("Read  | Elapsed time: {0} ms for #tuples: {1}",
                                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalCnt);

//return 0;
}

int PGReader::pqWriteToBp(int thr, int from, long to, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    int minBId = thr * (xdbcEnv->bufferpool_size / xdbcEnv->parallelism);
    int maxBId = (thr + 1) * (xdbcEnv->bufferpool_size / xdbcEnv->parallelism);

    spdlog::get("XDBC.SERVER")->info("Thread {0} assigned ({1},{2})", thr, minBId, maxBId);

    int curBid = minBId;
    const char *conninfo;
    PGconn *connection = NULL;
    char *receiveBuffer = NULL;
    int receiveLength = 0;
    const int asynchronous = 0;
    PGresult *res;

    int bufferTupleId;

    // TODO: attention! `hostAddr` is for IPs while `host` is for hostnames, handle correctly
    conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
    connection = PQconnectdb(conninfo);
    string toStr = std::to_string(to);
    //check if last thread, then max range


    std::string qStr =
            "COPY (SELECT * FROM " + tableName + " WHERE ctid BETWEEN '(" + std::to_string(from) + ",0)'::tid AND '(" +
            std::to_string(to) + ",0)'::tid) TO STDOUT WITH (FORMAT text, DELIMITER '|')";

    spdlog::get("XDBC.SERVER")->info("Thread {0} runs query: {1}", thr, qStr);

    res = PQexec(connection, qStr.c_str());
    ExecStatusType resType = PQresultStatus(res);

    if (resType == PGRES_COPY_OUT)
        spdlog::get("XDBC.SERVER")->info("Thread {0} pg: RESULT OK", thr);
    else
        spdlog::get("XDBC.SERVER")->error("Thread {0} pg: RESULT of COPY is {1}", thr, resType);
    //cout << "Thread " << thr << " Result of COPY is " << resType << endl;

    receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);


    char *endPtr;
    size_t len;
    //cout << "Thread: " << thr << " pg rcv len = " << receiveLength << endl;
    while (receiveLength > 0) {
        //cout << "Thread: " << thr << " pg rcv len = " << receiveLength << endl;
        int l03[4];
        double l47[4];

        char *startPtr = receiveBuffer;


        for (int i = 0; i < 7; i++) {

            endPtr = strchr(startPtr, '|');
            len = endPtr - startPtr;
            char tmp[len + 1];
            memcpy(tmp, startPtr, len);
            tmp[len] = '\0';
            //l0 = stoi(tmp);
            startPtr = endPtr + 1;
            if (i < 4) {
                l03[i] = stoi(tmp);

            } else {
                l47[i - 4] = stod(tmp);

            }

        }
        endPtr = strchr(startPtr, '\0');
        len = endPtr - startPtr;
        char tmp[len + 1];
        memcpy(tmp, startPtr, len);
        tmp[len] = '\0';


        l47[3] = stod(tmp);


        int sleepCtr = 0;


        while (flagArr[curBid] == 0) {
            curBid++;
            if (curBid == maxBId) {
                curBid = minBId;
                //cout << "Thread " << thr << " restarted to buffer id " << bufferId << endl;
            }

            if (sleepCtr == 1000) {
                sleepCtr = 0;
                spdlog::get("XDBC.SERVER")->warn(
                        "Thread {0} Write: Stuck at buffer {1} not ready to be written at tuple {2}. Total read buffers {3}",
                        thr, curBid, totalThreadWrittenBuffers, totalReadBuffers);

            }
            std::this_thread::sleep_for(SLEEP_TIME);
            sleepCtr++;
        }

        //spdlog::get("XDBC.SERVER")->info("writing to {0},{1}: ", curBid, bufferTupleId);

        bp[curBid][bufferTupleId] = {l03[0], l03[1], l03[2], l03[3],
                                     l47[0], l47[1], l47[2], l47[3]};

        /*if (totalThreadWrittenBuffers == 0 && bufferTupleId == 0) {
            spdlog::get("XDBC.SERVER")->warn("tuple in thread {0}, tuple: [{1}]",
                                             thr, slStr(&bp[curBid][bufferTupleId]));
        }*/

        totalThreadWrittenTuples++;
        bufferTupleId++;

        if (bufferTupleId == xdbcEnv->buffer_size) {
            //cout << "wrote buffer " << bufferId << endl;
            bufferTupleId = 0;
            flagArr[curBid] = 0;

            totalReadBuffers.fetch_add(1);
            totalThreadWrittenBuffers++;

            curBid++;

            if (curBid == maxBId)
                curBid = minBId;

        }


        PQfreemem(receiveBuffer);

        receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);

    }

    //remaining tuples
    if (totalReadBuffers > 0 && bufferTupleId != xdbcEnv->buffer_size) {
        spdlog::get("XDBC.SERVER")->info("Thread {0} has {1} remaining tuples", thr,
                                         xdbcEnv->buffer_size - bufferTupleId);

        //TODO: remove dirty fix, potentially with buffer header or resizable buffers
        for (int i = bufferTupleId; i < xdbcEnv->buffer_size; i++)
            bp[curBid][bufferTupleId] = {-1, -1, -1, -1, -1, -1, -1, -1};
        flagArr[curBid] = 0;
        totalReadBuffers.fetch_add(1);
        totalThreadWrittenBuffers++;
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
            spdlog::get("XDBC.SERVER")->info("Thread {0} Copy finished", thr);

        } else {
            spdlog::get("XDBC.SERVER")->warn("Thread {0} Copy failed", thr);
        }

        PQclear(result);
    } else if (receiveLength == -2) {
        /* received an error */
        spdlog::get("XDBC.SERVER")->warn("Thread {0} Copy failed bc -2", thr);
    }

    /* if copy out completed, make sure we drain all results from libpq */
    if (receiveLength < 0) {
        PGresult *result = PQgetResult(connection);
        while (result != NULL) {
            PQclear(result);
            result = PQgetResult(connection);
        }
    }
    spdlog::get("XDBC.SERVER")->info("Thread {0} wrote buffers: {1}, tuples {2}",
                                     thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);

    return 1;
}

std::string PGReader::slStr(shortLineitem *t) {

    return std::to_string(t->l_orderkey) + std::string(", ") +
           std::to_string(t->l_partkey) + std::string(", ") +
           std::to_string(t->l_suppkey) + std::string(", ") +
           std::to_string(t->l_linenumber) + std::string(", ") +
           std::to_string(t->l_quantity) + std::string(", ") +
           std::to_string(t->l_extendedprice) + std::string(", ") +
           std::to_string(t->l_discount) + std::string(", ") +
           std::to_string(t->l_tax);
}

double double_swap(double d) {
    union {
        double d;
        unsigned char bytes[8];
    } src, dest;

    src.d = d;
    dest.bytes[0] = src.bytes[7];
    dest.bytes[1] = src.bytes[6];
    dest.bytes[2] = src.bytes[5];
    dest.bytes[3] = src.bytes[4];
    dest.bytes[4] = src.bytes[3];
    dest.bytes[5] = src.bytes[2];
    dest.bytes[6] = src.bytes[1];
    dest.bytes[7] = src.bytes[0];
    return dest.d;
}

