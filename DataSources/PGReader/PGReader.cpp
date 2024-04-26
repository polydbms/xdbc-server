#include "PGReader.h"
#include "/usr/include/postgresql/libpq-fe.h"
#include <pqxx/pqxx>
#include <boost/asio.hpp>
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include "spdlog/spdlog.h"
#include <stack>
#include "../fast_float.h"
#include <charconv>

using namespace std;
using namespace pqxx;
using namespace boost::asio;
using ip::tcp;

//TODO: refactor for new buffer_size -> tuples_per_buffer and deserialization method

std::vector<std::string> splitStr(std::string const &original, char separator) {
    std::vector<std::string> results;
    std::string::const_iterator start = original.begin();
    std::string::const_iterator end = original.end();
    std::string::const_iterator next = std::find(start, end, separator);
    while (next != end) {
        results.emplace_back(start, next);
        start = next + 1;
        next = std::find(start, end, separator);
    }
    results.emplace_back(start, next);
    return results;
}

int fast_atoi(const char *str) {
    int val = 0;
    while (*str) {
        val = val * 10 + (*str++ - '0');
    }
    return val;
}

unsigned int naive(const char *p) {
    unsigned int x = 0;
    while (*p != '\0') {
        x = (x * 10) + (*p - '0');
        ++p;
    }
    return x;
}

enum STR2INT_ERROR {
    SUCCESS, OVERFLOW, UNDERFLOW, INCONVERTIBLE
};

STR2INT_ERROR str2int(int &i, char const *s, int base = 0) {
    char *end;
    long l;
    errno = 0;
    l = strtol(s, &end, base);
    if ((errno == ERANGE && l == LONG_MAX) || l > INT_MAX) {
        return OVERFLOW;
    }
    if ((errno == ERANGE && l == LONG_MIN) || l < INT_MIN) {
        return UNDERFLOW;
    }
    if (*s == '\0' || *end != '\0') {
        return INCONVERTIBLE;
    }
    i = l;
    return SUCCESS;
}

PGReader::PGReader(RuntimeEnv &xdbcEnv, const std::string &tableName) :
        DataSource(xdbcEnv, tableName),
        //flagArr(*xdbcEnv.flagArrPtr),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false),
        xdbcEnv(&xdbcEnv),
        tableName(tableName),
        partStack(),
        partStackMutex(),
        qs() {


    spdlog::get("XDBC.SERVER")->info("PG Reader, table schema:\n{0}", formatSchema(xdbcEnv.schema));
}

int PGReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool PGReader::getFinishedReading() const {
    return finishedReading;
}

int PGReader::getMaxCtId(const std::string &tableName) {

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

int PGReader::read_pqxx_stream() {
    int totalCnt = 0;
    spdlog::get("XDBC.SERVER")->info("Using pqxx::stream_from");

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

        //TODO: fix dynamic schema
        int mv = bufferTupleId;
        memcpy(bp[bufferId].data() + mv, &get<0>(lineitemTuple), 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &get<1>(lineitemTuple), 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &get<2>(lineitemTuple), 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &get<3>(lineitemTuple), 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &get<4>(lineitemTuple), 8);
        mv += 8;
        memcpy(bp[bufferId].data() + mv, &get<5>(lineitemTuple), 8);
        mv += 8;
        memcpy(bp[bufferId].data() + mv, &get<6>(lineitemTuple), 8);
        mv += 8;
        memcpy(bp[bufferId].data() + mv, &get<7>(lineitemTuple), 8);


/*            bp[bufferId][bufferTupleId] = {get<0>(lineitemTuple),
                                           get<1>(lineitemTuple),
                                           get<2>(lineitemTuple),
                                           get<3>(lineitemTuple),
                                           get<4>(lineitemTuple),
                                           get<5>(lineitemTuple),
                                           get<6>(lineitemTuple),
                                           get<7>(lineitemTuple),
            };*/
        totalCnt++;
        bufferTupleId++;

        if (bufferTupleId == xdbcEnv->buffer_size) {
            //cout << "wrote buffer " << bufferId << endl;
            bufferTupleId = 0;
            //flagArr[bufferId] = 0;

            bufferId++;

            if (bufferId == xdbcEnv->bufferpool_size)
                bufferId = 0;

        }

    }

    stream.complete();
    tx.commit();
    return totalCnt;
}

int PGReader::read_pq_exec() {
    int totalCnt = 0;

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
        /*while (flagArr[bufferId] == 0) {
            if (sleepCtr == 1000) {
                sleepCtr = 0;
                cout << "Read: Stuck at buffer " << bufferId << " not ready to be written at tuple " << totalCnt
                     << " and tuple " << i << endl;
            }
            //std::this_thread::sleep_for(SLEEP_TIME);
            sleepCtr++;
        }*/

        //TODO: fix dynamic schema
        int mv = bufferTupleId;
        memcpy(bp[bufferId].data() + mv, &l0val, 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &l1val, 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &l2val, 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &l3val, 4);
        mv += 4;
        memcpy(bp[bufferId].data() + mv, &l4val, 8);
        mv += 8;
        memcpy(bp[bufferId].data() + mv, &l5val, 8);
        mv += 8;
        memcpy(bp[bufferId].data() + mv, &l6val, 8);
        mv += 8;
        memcpy(bp[bufferId].data() + mv, &l7val, 8);


        /*bp[bufferId][bufferTupleId] = {l0val,
                                       l1val,
                                       l2val,
                                       l3val,
                                       l4val,
                                       l5val,
                                       l6val,
                                       l7val
        };*/

        totalCnt++;
        bufferTupleId++;

        if (bufferTupleId == xdbcEnv->buffer_size) {
            //cout << "wrote buffer " << bufferId << endl;
            bufferTupleId = 0;
            //flagArr[bufferId] = 0;

            bufferId++;

            if (bufferId == xdbcEnv->bufferpool_size)
                bufferId = 0;

        }
    }

    PQfinish(conn);
    return totalCnt;
}

int PGReader::read_pq_copy() {

    int totalCnt = 0;
    spdlog::get("XDBC.SERVER")->info("Using pglib with COPY, parallelism: {0}", xdbcEnv->read_parallelism);
    spdlog::get("XDBC.SERVER")->info("Using compression: {0}", xdbcEnv->compression_algorithm);

    int threadWrittenTuples[xdbcEnv->deser_parallelism];
    int threadWrittenBuffers[xdbcEnv->deser_parallelism];
    thread readThreads[xdbcEnv->read_parallelism];
    thread deSerThreads[xdbcEnv->deser_parallelism];

    // TODO: throw something when table does not exist

    int maxCtId = getMaxCtId(tableName);

    int partNum = xdbcEnv->read_partitions;
    div_t partSizeDiv = div(maxCtId, partNum);

    int partSize = partSizeDiv.quot;

    if (partSizeDiv.rem > 0)
        partSize++;


    for (int i = partNum - 1; i >= 0; i--) {
        Part p;
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = UINT32_MAX;

        partStack.push(p);

    }

    //initialize deser queues
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        Q_ptr q(new customQueue<vector<string>>);
        qs.push_back(q);
    }

    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        readThreads[i] = std::thread(&PGReader::pqWriteToBp, this, i);
    }


    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {

        threadWrittenTuples[i] = 0;
        threadWrittenBuffers[i] = 0;
        deSerThreads[i] = std::thread(&PGReader::writeTuplesToBp,
                                      this, i, std::ref(threadWrittenTuples[i]), std::ref(threadWrittenBuffers[i]));
    }

    int total = 0;
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        deSerThreads[i].join();
        total += threadWrittenTuples[i];
    }

    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        readThreads[i].join();
    }
    finishedReading.store(true);
    totalCnt += total;

    return totalCnt;
}

void PGReader::readData() {

    //TODO: expose different read methods
    int x = 3;
    auto start = std::chrono::steady_clock::now();
    int totalCnt = 0;

    if (x == 1)
        totalCnt = read_pqxx_stream();

    if (x == 2)
        totalCnt = read_pq_exec();

    if (x == 3)
        totalCnt = read_pq_copy();

    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("Read  | Elapsed time: {0} ms for #tuples: {1}",
                                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalCnt);

    //return 0;
}

int
PGReader::writeTuplesToBp(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    //spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} locking tuple", thr);
    int emptyCtr = 0;
    int schemaSize = xdbcEnv->schema.size();
    int compQueueId = 0;
    char *endPtr;
    size_t len;
    int celli = -1;
    double celld = -1;
    int bufferTupleId = 0;
    const char *tmpPtr;
    const char *tmpEnd;
    char *startPtr;

    while (emptyCtr < xdbcEnv->read_parallelism) {

        auto src = qs[thr]->pop();
        if (src.empty())
            emptyCtr++;
        else {

            bufferTupleId = 0;
            //int minBId = thr * (xdbcEnv->bufferpool_size / xdbcEnv->deser_parallelism);
            //int maxBId = (thr + 1) * (xdbcEnv->bufferpool_size / xdbcEnv->deser_parallelism);
            //int minBId = 0;
            //int maxBId = xdbcEnv->bufferpool_size;

            /*spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} assigned ({1},{2}), first tuple: {3}", thr, minBId,
                                             maxBId, src[0]);*/


            int curBid = xdbcEnv->deserBufferPtr[thr]->pop();
            auto bpPtr = bp[curBid].data();

            for (int i = 0; i < src.size(); i++) {
                //spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} processing {1}, tuple: {2}", thr, bufferTupleId, tuple);

                startPtr = src[i].data();

                int bytesInTuple = 0;

                for (int attPos = 0; attPos < schemaSize; attPos++) {

                    //spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} processing schema", thr);

                    auto &attribute = xdbcEnv->schema[attPos];

                    if (attPos < schemaSize - 1)
                        endPtr = strchr(startPtr, '|');
                    else
                        endPtr = strchr(startPtr, '\0');

                    len = endPtr - startPtr;
                    //char tmp[len + 1];
                    //memcpy(tmp, startPtr, len);
                    //tmp[len] = '\0';
                    std::string_view tmp(startPtr, len);
                    tmpPtr = tmp.data();
                    tmpEnd = tmpPtr + len;
                    startPtr = endPtr + 1;

                    void *writePtr;
                    if (xdbcEnv->iformat == 1) {
                        writePtr = bpPtr + bufferTupleId * xdbcEnv->tuple_size + bytesInTuple;
                    } else if (xdbcEnv->iformat == 2) {
                        writePtr = bpPtr + bytesInTuple * xdbcEnv->buffer_size +
                                   bufferTupleId * attribute.size;
                    }

                    if (attribute.tpe == "INT") {
                        std::from_chars(tmpPtr, tmpEnd, celli);
                        memcpy(writePtr, &celli, 4);
                    } else if (attribute.tpe == "DOUBLE") {
                        std::from_chars(tmpPtr, tmpEnd, celld);
                        memcpy(writePtr, &celld, 8);
                    }

                    //TODO: add more types
                    bytesInTuple += attribute.size;
                }
                //spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} processed schema", thr);

                totalThreadWrittenTuples++;
                bufferTupleId++;

                if (bufferTupleId == xdbcEnv->buffer_size) {

                    bufferTupleId = 0;
                    //flagArr[curBid].store(0);
                    //totalReadBuffers.fetch_add(1);
                    totalThreadWrittenBuffers++;
                    xdbcEnv->compBufferPtr[compQueueId]->push(curBid);
                    compQueueId++;
                    if (compQueueId == xdbcEnv->compression_parallelism)
                        compQueueId = 0;

                    curBid = xdbcEnv->deserBufferPtr[thr]->pop();
                    bpPtr = bp[curBid].data();
                }

            }

            //remaining tuples
            if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->buffer_size) {
                spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} has {1} remaining tuples",
                                                 thr, xdbcEnv->buffer_size - bufferTupleId);

                //TODO: remove dirty fix, potentially with buffer header or resizable buffers
                int mone = -1;

                for (int i = bufferTupleId; i < xdbcEnv->buffer_size; i++) {

                    void *writePtr;
                    if (xdbcEnv->iformat == 1) {
                        writePtr = bpPtr + bufferTupleId * xdbcEnv->tuple_size;
                    } else if (xdbcEnv->iformat == 2) {
                        writePtr = bpPtr + bufferTupleId * xdbcEnv->schema[0].size;
                    }

                    memcpy(writePtr, &mone, 4);
                }

                //spdlog::get("XDBC.SERVER")->info("thr {0} finished remaining", thr);
                //totalReadBuffers.fetch_add(1);
                xdbcEnv->compBufferPtr[compQueueId]->push(curBid);
                totalThreadWrittenBuffers++;
            }

            /*else
                spdlog::get("XDBC.SERVER")->info("PG thread {0} has no remaining tuples", thr);*/

            /*spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} wrote buffers: {1}, tuples {2}",
                                             thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);*/

        }
    }

    for (int i = 0; i < xdbcEnv->compression_parallelism; i++)
        xdbcEnv->compBufferPtr[i]->push(-1);

    return 1;
}

int PGReader::pqWriteToBp(int thr) {

    const char *conninfo;
    PGconn *connection = NULL;
    // TODO: attention! `hostAddr` is for IPs while `host` is for hostnames, handle correctly
    conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
    connection = PQconnectdb(conninfo);
    int vectorSize = xdbcEnv->buffer_size * 100;
    vector<string> tuples(vectorSize);
    std::unique_lock<std::mutex> lock(partStackMutex);
    while (!partStack.empty()) {
        tuples.resize(vectorSize);

        Part part = partStack.top();
        partStack.pop();
        if (lock.owns_lock())
            lock.unlock();

        char *receiveBuffer = NULL;
        int receiveLength = 0;
        const int asynchronous = 0;
        PGresult *res;
        string toStr = std::to_string(part.endOff);

        std::string qStr =
                "COPY (SELECT " + getAttributesAsStr(xdbcEnv->schema) + " FROM " + tableName +
                " WHERE ctid BETWEEN '(" +
                std::to_string(part.startOff) + ",0)'::tid AND '(" +
                std::to_string(part.endOff) + ",0)'::tid) TO STDOUT WITH (FORMAT text, DELIMITER '|')";

        spdlog::get("XDBC.SERVER")->info("PG thread {0} runs query: {1}", thr, qStr);

        res = PQexec(connection, qStr.c_str());
        ExecStatusType resType = PQresultStatus(res);

        if (resType != PGRES_COPY_OUT)
            spdlog::get("XDBC.SERVER")->error("PG thread {0}: RESULT of COPY is {1}", thr, resType);

        receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);

        int tupleId = 0;
        int lTupleId = 0;
        int deserQ = 0;
        spdlog::get("XDBC.SERVER")->error("PG thread {0}: Entering PQgetCopyData loop with rcvlen: {1}", thr,
                                          receiveLength);
        while (receiveLength > 0) {
            //auto tmpTpl = new char[receiveLength];
            //memcpy(tmpTpl, receiveBuffer, receiveLength);

            tuples[lTupleId] = receiveBuffer;
            tupleId++;
            lTupleId++;
            if (lTupleId == vectorSize) {
                //vector<string> tpls(tuples);

                qs[deserQ]->push(tuples);

                deserQ++;
                if (deserQ == xdbcEnv->deser_parallelism)
                    deserQ = 0;


                lTupleId = 0;
            }


            PQfreemem(receiveBuffer);
            receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);
        }
        tuples.resize(lTupleId);
        //handle last tuples
        qs[deserQ]->push(tuples);

        /*      std::unique_lock<std::mutex> lock2(tupleStackMutex);
              tupleStack.push(tuples);
              lock2.unlock();
  */

        spdlog::get("XDBC.SERVER")->error("PG thread {0}: Exiting PQgetCopyData loop, tupleNo: {1}", thr, tupleId);
        /* we now check the last received length returned by copy data */
        if (receiveLength == 0) {
            /* we cannot read more data without blocking */
            spdlog::get("XDBC.SERVER")->warn("PG Reader received 0");
        } else if (receiveLength == -1) {
            /* received copy done message */
            PGresult *result = PQgetResult(connection);
            ExecStatusType resultStatus = PQresultStatus(result);

            if (resultStatus != PGRES_COMMAND_OK) {
                spdlog::get("XDBC.SERVER")->warn("PG thread {0} Copy failed", thr);

            }

            PQclear(result);
        } else if (receiveLength == -2) {
            /* received an error */
            spdlog::get("XDBC.SERVER")->warn("PG thread {0} Copy failed bc -2", thr);
        } else if (receiveLength < 0) {
            /* if copy out completed, make sure we drain all results from libpq */
            PGresult *result = PQgetResult(connection);
            while (result != NULL) {
                PQclear(result);
                result = PQgetResult(connection);
            }
        }


    }
    if (lock.owns_lock())
        lock.unlock();

    spdlog::get("XDBC.SERVER")->info("PG thread {0} exiting", thr);

    PQfinish(connection);

    vector<string> v;
    for (const auto &q: qs) {
        q->push(v);
    }

    return 1;
}
