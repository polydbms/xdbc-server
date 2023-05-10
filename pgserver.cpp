//
// Created by harry on 11/19/22.
//

#include <chrono>
#include <iostream>
#include "pgserver.h"
#include <pqxx/pqxx>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <thread>
#include "/usr/include/postgresql/libpq-fe.h"
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>

using namespace std;
using namespace pqxx;
using namespace boost::asio;
using ip::tcp;

void compress_buffer(int method, boost::asio::mutable_buffer &buffer) {
    //1 zstd
    //2 snappy
    //3 lzo
    //4 lz4

    if (method == 1) {
        // Get the raw buffer pointer and size
        char *data = boost::asio::buffer_cast<char *>(buffer);
        size_t size = boost::asio::buffer_size(buffer);

        // Compress the buffer in place
        size_t compressed_size = ZSTD_compress(data, size, data, size, 1);

        // Resize the buffer to the compressed size
        buffer = boost::asio::buffer(data, compressed_size);
    }
    if (method == 2) {
        char *data = boost::asio::buffer_cast<char *>(buffer);
        size_t size = boost::asio::buffer_size(buffer);

        std::vector<char> compressed_data(snappy::MaxCompressedLength(size));
        size_t compressed_size = 0;

        snappy::RawCompress(data, size, compressed_data.data(), &compressed_size);

        // Copy the compressed data back into the input buffer
        buffer = boost::asio::buffer(compressed_data.data(), compressed_size);
    }
    if (method == 3) {

        const std::size_t size = boost::asio::buffer_size(buffer);

        // Create a temporary buffer to hold the compressed data
        std::vector<char> compressed_data(size + size / 16 + 64 + 3);
        lzo_voidp wrkmem = (lzo_voidp)
        malloc(LZO1X_1_MEM_COMPRESS);
        // Compress the data
        lzo_uint compressed_size;
        const int result = lzo1x_1_compress(
                reinterpret_cast<const unsigned char *>(boost::asio::buffer_cast<const char *>(buffer)),
                static_cast<lzo_uint>(size),
                reinterpret_cast<unsigned char *>(&compressed_data[0]),
                &compressed_size,
                wrkmem
        );

        if (result != LZO_E_OK) {
            throw std::runtime_error("lzo1x_1_compress failed");
        }

        // Copy the compressed data back to the input buffer
        boost::asio::buffer_copy(buffer, boost::asio::buffer(compressed_data.data(), compressed_size));
        free(wrkmem);
    }
    if (method == 4) {
        const size_t input_size = boost::asio::buffer_size(buffer);
        const char *input_data = boost::asio::buffer_cast<const char *>(buffer);

        const size_t max_compressed_size = LZ4_compressBound(input_size);
        std::vector<char> compressed_data(max_compressed_size);

        const int compressed_size = LZ4_compress_default(input_data, compressed_data.data(), input_size,
                                                         max_compressed_size);
        if (compressed_size <= 0) {
            throw std::runtime_error("LZ4 compression failed");
        }

        buffer = boost::asio::mutable_buffer(compressed_data.data(), compressed_size);
    }
}

int getMaxCtId(std::string tableName) {

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

string read_(tcp::socket &socket) {
    boost::asio::streambuf buf;
    boost::asio::read_until(socket, buf, "\n");
    string data = boost::asio::buffer_cast<const char *>(buf.data());
    return data;
}

int PGServer::pqWriteToBp(int thr, int from, long to, int &totalCnt) {

    int minBId = thr * (BUFFERPOOL_SIZE / PARALLELISM);
    int maxBId = (thr + 1) * (BUFFERPOOL_SIZE / PARALLELISM);

    cout << "Thread " << thr << " assigned (" << minBId << "," << maxBId << ")" << endl;
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
    cout << "Thread " << thr << " query: " << qStr << endl;
    res = PQexec(connection, qStr.c_str());
    ExecStatusType resType = PQresultStatus(res);

    if (resType == PGRES_COPY_OUT)
        cout << "Thread " << thr << " Result OK" << endl;
    else
        cout << "Thread " << thr << " Result of COPY is " << resType << endl;

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

        while (flagArr[curBid] == 0) {
            curBid++;
            if (curBid == maxBId) {
                curBid = minBId;
                //cout << "Thread " << thr << " restarted to buffer id " << bufferId << endl;
            }

            if (sleepCtr == 1000) {
                sleepCtr = 0;
                cout << "Thread " << thr << " Write: Stuck at buffer " << curBid << " not ready to be written at tuple "
                     << totalCnt << endl;
            }
            std::this_thread::sleep_for(SLEEP_TIME);
            sleepCtr++;
        }

        //flagArr[bufferId] = 0;



        bp[curBid][bufferTupleId] = {l03[0], l03[1], l03[2], l03[3],
                                     l47[0], l47[1], l47[2], l47[3]};


        totalCnt++;
        bufferTupleId++;

        if (bufferTupleId == BUFFER_SIZE) {
            //cout << "wrote buffer " << bufferId << endl;
            bufferTupleId = 0;
            flagArr[curBid] = 0;

            curBid++;

            if (curBid == maxBId)
                curBid = minBId;

        }


        PQfreemem(receiveBuffer);

        receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);

    }

    //remaining tuples
    cout << "Thread " << thr << " has " << BUFFER_SIZE - bufferTupleId << " remaining tuples" << endl;
    //TODO: remove dirty fix
    for (int i = bufferTupleId; i < BUFFER_SIZE; i++)
        bp[curBid][bufferTupleId] = {-1, -1, -1, -1, -1, -1, -1, -1};
    flagArr[curBid] = 0;


    /* we now check the last received length returned by copy data */
    if (receiveLength == 0) {
        /* we cannot read more data without blocking */
        cout << "Received 0" << endl;
    } else if (receiveLength == -1) {
        /* received copy done message */
        PGresult *result = PQgetResult(connection);
        ExecStatusType resultStatus = PQresultStatus(result);

        if (resultStatus == PGRES_COMMAND_OK) {
            cout << "Thread " << thr << " Copy finished " << endl;
        } else {
            cout << "Copy failed" << endl;
        }

        PQclear(result);
    } else if (receiveLength == -2) {
        /* received an error */
        cout << "Thread " << thr << " Copy failed bc -2" << endl;
    }

    /* if copy out completed, make sure we drain all results from libpq */
    if (receiveLength < 0) {
        PGresult *result = PQgetResult(connection);
        while (result != NULL) {
            PQclear(result);
            result = PQgetResult(connection);
        }
    }

    cout << "Thread " << thr << " wrote " << totalCnt << " tuples" << endl;

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

PGServer::PGServer(const RuntimeEnv& env) : bp(), flagArr(), totalRead(), finishedReading(false), tableName() {
    bp.resize(BUFFERPOOL_SIZE * TUPLE_SIZE);
    for (int i = 0; i < BUFFERPOOL_SIZE; i++)
        flagArr[i] = 1;

}


void PGServer::readFromDB(int x) {

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

            if (bufferTupleId == BUFFER_SIZE) {
                //cout << "wrote buffer " << bufferId << endl;
                bufferTupleId = 0;
                flagArr[bufferId] = 0;

                bufferId++;

                if (bufferId == BUFFERPOOL_SIZE)
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

            if (bufferTupleId == BUFFER_SIZE) {
                //cout << "wrote buffer " << bufferId << endl;
                bufferTupleId = 0;
                flagArr[bufferId] = 0;

                bufferId++;

                if (bufferId == BUFFERPOOL_SIZE)
                    bufferId = 0;

            }
        }

        PQfinish(conn);

    } else if (x == 3) {

        cout << "Using pglib with COPY, parallelism: " << PARALLELISM << endl;


        int xs[PARALLELISM];
        thread threads[PARALLELISM];

        // TODO: throw something when table does not exist

        cout << "deciding partitioning" << endl;
        int maxCtId = getMaxCtId(tableName);

        cout << "partitioning upper bound: " << maxCtId << endl;
        int partSize = maxCtId / PARALLELISM;

        cout << "starting threads" << endl;
        for (int i = 0; i < PARALLELISM; i++) {

            int startOff = i * partSize;
            long endOff = (i + 1) * partSize;

            /*if(i==0) {
                startOff = 0;
                endOff = 24000;
            }
            if(i==1) {
                startOff = 24000;
                endOff = 48000;
            }
            if(i==2) {
                startOff = 48000;
                endOff = 72000;
            }
            if(i==3) {
                startOff = 72000;
                endOff = 100000;
            }*/
            if (i == PARALLELISM - 1)
                endOff = UINT32_MAX;

            threads[i] = std::thread(&PGServer::pqWriteToBp, this, i, startOff, endOff, std::ref(xs[i]));
            xs[i] = 0;
            //thread th2(&PGServer::pqWriteToBp, this, 1, 24000, 48000, std::ref(x2));
            //thread th3(&PGServer::pqWriteToBp, this, 2, 48000, 72000, std::ref(x3));
            //thread th4(&PGServer::pqWriteToBp, this, 3, 72000, 100000, std::ref(x4));
        }

        cout << "finished threads" << endl;

        int total = 0;
        for (int i = 0; i < PARALLELISM; i++) {
            threads[i].join();
            total += xs[i];
        }
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

            if (bufferTupleId == BUFFER_SIZE) {
                //cout << "wrote buffer " << bufferId << endl;
                bufferTupleId = 0;
                flagArr[bufferId] = 0;

                bufferId++;

                if (bufferId == BUFFERPOOL_SIZE)
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

    totalRead = totalCnt;
    auto end = std::chrono::steady_clock::now();
    cout << "Read  | Elapsed time in milliseconds: "
         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
         << " ms" << " for #tuples: " << totalCnt << endl;

    finishedReading = true;
//return 0;
}

bool PGServer::hasUnsent() {

    if (!finishedReading)
        return true;
    for (int i: flagArr) {
        if (i == 0)
            return true;
    }
    return false;
}

void PGServer::send(tcp::socket &socket, bool compress) {


    auto start = std::chrono::steady_clock::now();

    int bufferId = 0;
    int totalSent = 0;
    int totalSentBuffers = 0;
    /*while (totalTuples == -1)
        std::this_thread::sleep_for(SLEEP_TIME);*/

    //while (totalSent < TOTAL_TUPLES) {
    bool send = true;
    while (hasUnsent()) {
        while (flagArr[bufferId] == 1) {
            bufferId++;
            if (bufferId == BUFFERPOOL_SIZE) {
                bufferId = 0;
                if (finishedReading) {
                    send = false;
                    break;
                }
            }
        }
        //cout << "tuple cnt = " << totalCnt << endl;
        /*int sleepCtr = 0;
        while (flagArr[bufferId] == 1 && !finishedReading) {
            if (sleepCtr == 1000) {
                sleepCtr = 0;
                cout << "Stuck at buffer " << bufferId << " not ready to be read" << endl;
            }
            std::this_thread::sleep_for(SLEEP_TIME);
            sleepCtr++;
        }*/
        if (send) {
            boost::asio::mutable_buffer buffer = boost::asio::buffer(bp[bufferId]);


            if (compress) {
                std::vector<boost::asio::mutable_buffer> buffers;
                compress_buffer(1, buffer);
                //TODO: create more sophisticated header with checksum etc
                std::array<size_t, 1> size{buffer.size()};
                boost::asio::write(socket, boost::asio::buffer(size));
            }


            size_t bytes_sent = boost::asio::write(socket, boost::asio::buffer(buffer));
            //cout << "Sent bytes:" << bytes_sent << endl;
            totalSentBuffers += 1;
            //cout << "sent buffer " << bufferId << endl;
            flagArr[bufferId] = 1;
            totalSent += bp[bufferId].size();
        }
        //totalSent += BUFFER_SIZE;
        /*for (int i = 0; i < BUFFERPOOL_SIZE; i++)
            cout << flagArr[i];
        cout << endl;*/
    }

    socket.close();

    auto end = std::chrono::steady_clock::now();
    cout << "Send  | Elapsed time in milliseconds: "
         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
         << " ms" << " for #tuples: " << totalSent << " and #buffers: " << totalSentBuffers << endl;
}


int PGServer::serve() {

    boost::asio::io_service io_service;
    //listen for new connection
    tcp::acceptor acceptor_(io_service, tcp::endpoint(tcp::v4(), 1234));
    //socket creation
    tcp::socket socket_(io_service);
    //waiting for connection
    acceptor_.accept(socket_);
    //read operation
    tableName = read_(socket_);

    tableName.erase(std::remove(tableName.begin(), tableName.end(), '\n'), tableName.cend());

    cout << "Get table " << tableName << endl;


    std::thread t1(&PGServer::readFromDB, this, 3);
    std::thread t2(&PGServer::send, this, std::ref(socket_), false);

    t1.join();
    t2.join();


    return 1;
}


/*void send_(tcp::socket &socket, const string &message) {
    const string msg = message + "\n";
    boost::asio::write(socket, boost::asio::buffer(message));
}*/





