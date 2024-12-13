#include "PGReader.h"
#include "/usr/include/postgresql/libpq-fe.h"
#include <pqxx/pqxx>
#include <boost/asio.hpp>
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include <stack>
#include <charconv>
#include <queue>
#include "spdlog/spdlog.h"
#include "../fast_float.h"
#include "../deserializers.h"
#include "../../xdbcserver.h"

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
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false),
        xdbcEnv(&xdbcEnv) {

    spdlog::get("XDBC.SERVER")->info("PG Constructor called with table {0}", tableName);
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

        if (bufferTupleId == xdbcEnv->tuples_per_buffer) {
            //cout << "wrote buffer " << bufferId << endl;
            bufferTupleId = 0;
            //flagArr[bufferId] = 0;

            bufferId++;

            if (bufferId == xdbcEnv->buffers_in_bufferpool)
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

        if (bufferTupleId == xdbcEnv->tuples_per_buffer) {
            //cout << "wrote buffer " << bufferId << endl;
            bufferTupleId = 0;
            //flagArr[bufferId] = 0;

            bufferId++;

            if (bufferId == xdbcEnv->buffers_in_bufferpool)
                bufferId = 0;

        }
    }

    PQfinish(conn);
    return totalCnt;
}

int PGReader::read_pq_copy() {

    auto start_read = std::chrono::steady_clock::now();

    spdlog::get("XDBC.SERVER")->info("Using pglib with COPY, parallelism: {0}", xdbcEnv->read_parallelism);

    int threadWrittenTuples[xdbcEnv->deser_parallelism];
    int threadWrittenBuffers[xdbcEnv->deser_parallelism];
    thread readThreads[xdbcEnv->read_parallelism];
    thread deSerThreads[xdbcEnv->deser_parallelism];

    // TODO: throw something when table does not exist

    int maxRowNum = getMaxCtId(tableName);

    int partNum = xdbcEnv->read_parallelism;
    div_t partSizeDiv = div(maxRowNum, partNum);

    int partSize = partSizeDiv.quot;

    if (partSizeDiv.rem > 0)
        partSize++;

    int readQ = 0;
    for (int i = partNum - 1; i >= 0; i--) {
        Part p{};
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = UINT32_MAX;

        xdbcEnv->partPtr->push(p);

        spdlog::get("XDBC.SERVER")->info("Partition {0} [{1},{2}] assigned to read thread {3} ",
                                         p.id, p.startOff, p.endOff, readQ);

        readQ++;
        if (readQ == xdbcEnv->read_parallelism)
            readQ = 0;


    }

    //final partition
    Part fP{};
    fP.id = -1;

    xdbcEnv->activeReadThreads.resize(xdbcEnv->read_parallelism);
    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        xdbcEnv->partPtr->push(fP);
        readThreads[i] = std::thread(&PGReader::readPG, this, i);
        xdbcEnv->activeReadThreads[i] = true;

    }


    auto start_deser = std::chrono::steady_clock::now();
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        threadWrittenTuples[i] = 0;
        threadWrittenBuffers[i] = 0;

        deSerThreads[i] = std::thread(&PGReader::deserializePG,
                                      this, i,
                                      std::ref(threadWrittenTuples[i]), std::ref(threadWrittenBuffers[i])
        );

    }

    int totalTuples = 0;
    int totalBuffers = 0;
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        deSerThreads[i].join();
        totalTuples += threadWrittenTuples[i];
        totalBuffers += threadWrittenBuffers[i];
    }


    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        readThreads[i].join();
    }

    finishedReading.store(true);

    auto end = std::chrono::steady_clock::now();
    auto total_read_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start_read).count();
    auto total_deser_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start_deser).count();


    spdlog::get("XDBC.SERVER")->info("Read+Deser | Elapsed time: {0} ms for #tuples: {1}, #buffers: {2}",
                                     total_deser_time / 1000,
                                     totalTuples, totalBuffers);

    return totalTuples;
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
PGReader::deserializePG(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    spdlog::get("XDBC.SERVER")->info("PG Deser thr {0} started", thr);
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "start"});

    int emptyCtr = 0;
    std::queue<int> writeBuffers;
    std::queue<std::vector<std::byte>> tmpBuffers;
    int curWriteBuffer;
    size_t readOffset = 0;
    const char *endPtr;
    size_t len;
    size_t bufferTupleId = 0;
    int bytesInTuple = 0;
    std::byte *startWritePtr;
    const char *startReadPtr;
    void *write;
    int readMoreQ = 0;
    size_t schemaSize = xdbcEnv->schema.size();
    std::vector<size_t> sizes(schemaSize);
    std::vector<size_t> schemaChars(schemaSize);
    using DeserializeFunc = void (*)(const char *src, const char *end, void *dest, int attSize, size_t len);
    std::vector<DeserializeFunc> deserializers(schemaSize);

    for (size_t i = 0; i < schemaSize; ++i) {
        if (xdbcEnv->schema[i].tpe[0] == 'I') {
            sizes[i] = 4; // sizeof(int)
            schemaChars[i] = 'I';
            deserializers[i] = deserialize<int>;
        } else if (xdbcEnv->schema[i].tpe[0] == 'D') {
            sizes[i] = 8; // sizeof(double)
            schemaChars[i] = 'D';
            deserializers[i] = deserialize<double>;
        } else if (xdbcEnv->schema[i].tpe[0] == 'C') {
            sizes[i] = 1; // sizeof(char)
            schemaChars[i] = 'C';
            deserializers[i] = deserialize<char>;
        } else if (xdbcEnv->schema[i].tpe[0] == 'S') {
            sizes[i] = xdbcEnv->schema[i].size;
            schemaChars[i] = 'S';
            deserializers[i] = deserialize<const char *>;
        }
    }

    while (emptyCtr < xdbcEnv->read_parallelism || !tmpBuffers.empty()) {

        if (emptyCtr < xdbcEnv->read_parallelism && (tmpBuffers.empty() || writeBuffers.empty())) {
            auto start_wait = std::chrono::high_resolution_clock::now();

            //spdlog::get("XDBC.SERVER")->info("Deser thr {0} waiting, emptyCtr {1}", thr, emptyCtr);
            int curBid = xdbcEnv->deserBufferPtr->pop();
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

            //spdlog::get("XDBC.SERVER")->info("Deser thr {0} got buff {1}", thr, curBid);


            if (curBid == -1) {
                emptyCtr++;
                continue;
            }

            //allocate new tmp buffer, copy contents into it
            //size_t bytesToRead = 0;
            auto *header = reinterpret_cast<Header *>(bp[curBid].data());
            std::vector<std::byte> tmpBuffer(header->totalSize);
            std::memcpy(tmpBuffer.data(), bp[curBid].data() + sizeof(Header), header->totalSize);

            //push current tmp and write buffers into our respective queues
            tmpBuffers.push(tmpBuffer);
            writeBuffers.push(curBid);
        }


        //spdlog::get("XDBC.SERVER")->info("tmpBuffers {0}, writeBuffers {1}", bbbb, cccc);
        //signal to reader that we need one more buffer
        if (emptyCtr == xdbcEnv->read_parallelism && !tmpBuffers.empty() && writeBuffers.empty()) {


            //spdlog::get("XDBC.SERVER")->info("Deser thr {0} requesting buff from {1}", thr, readMoreQ);
            //use read thread 0 to request buffers
            //TODO: check if we need to refactor moreBuffersQ since only 1 thread is used for forwarding
            xdbcEnv->moreBuffersQ->push(thr);

            auto start_wait = std::chrono::high_resolution_clock::now();

            int curBid = xdbcEnv->deserBufferPtr->pop();
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

            //spdlog::get("XDBC.SERVER")->info("Deser thr {0} got buff {1}", thr, curBid);

            writeBuffers.push(curBid);
        }

        //define current read buffer & write buffer
        curWriteBuffer = writeBuffers.front();
        const std::vector<std::byte> &curReadBufferRef = tmpBuffers.front();

        while (readOffset < curReadBufferRef.size()) {

            startReadPtr = reinterpret_cast<const char *>(curReadBufferRef.data() + readOffset);
            //+sizeof(size_t) for temp header (totalTuples)
            startWritePtr = bp[curWriteBuffer].data() + sizeof(Header);

            bytesInTuple = 0;

            for (int attPos = 0; attPos < schemaSize; attPos++) {

                //spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} processing schema", thr);

                auto &attribute = xdbcEnv->schema[attPos];

                endPtr = (attPos < schemaSize - 1) ? strchr(startReadPtr, '|') : strchr(startReadPtr, '\n');

                len = endPtr - startReadPtr;

                std::string_view tmp(startReadPtr, len);
                const char *tmpPtr = tmp.data();
                const char *tmpEnd = tmpPtr + len;
                startReadPtr = endPtr + 1;

                if (xdbcEnv->iformat == 1) {
                    write = startWritePtr + bufferTupleId * xdbcEnv->tuple_size + bytesInTuple;
                } else if (xdbcEnv->iformat == 2) {
                    write = startWritePtr + bytesInTuple * xdbcEnv->tuples_per_buffer + bufferTupleId * attribute.size;
                }

                deserializers[attPos](tmpPtr, tmpEnd, write, attribute.size, len);

                bytesInTuple += attribute.size;
                readOffset += len + 1;

            }
            bufferTupleId++;
            totalThreadWrittenTuples++;

            if (bufferTupleId == xdbcEnv->tuples_per_buffer) {
                Header head{};
                head.totalTuples = bufferTupleId;
                memcpy(bp[curWriteBuffer].data(), &head, sizeof(Header));
                bufferTupleId = 0;

                totalThreadWrittenBuffers++;


                xdbcEnv->compBufferPtr->push(curWriteBuffer);
                xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

                writeBuffers.pop();
                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

                break;
            }
        }
        if (readOffset >= curReadBufferRef.size()) {
            tmpBuffers.pop();
            readOffset = 0;
        }
    }
    //remaining tuples
    if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->tuples_per_buffer) {
        spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} has {1} remaining tuples",
                                         thr, xdbcEnv->tuples_per_buffer - bufferTupleId);

        //write tuple count to tmp header
        Header head{};
        head.totalTuples = bufferTupleId;
        memcpy(bp[curWriteBuffer].data(), &head, sizeof(Header));

        xdbcEnv->compBufferPtr->push(curWriteBuffer);
        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

        totalThreadWrittenBuffers++;
    }


    //notify that we will not request other buffers
    xdbcEnv->moreBuffersQ->push(-1);


    /*else
        spdlog::get("XDBC.SERVER")->info("PG thread {0} has no remaining tuples", thr);*/

    spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} finished. buffers: {1}, tuples {2}",
                                     thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);

    //notify that we finished
    xdbcEnv->compBufferPtr->push(-1);


    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "end"});

    return 1;
}

int PGReader::readPG(int thr) {

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "start"});

    int curBid = xdbcEnv->readBufferPtr->pop();
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

    Part curPart = xdbcEnv->partPtr->pop();

    std::byte *writePtr = bp[curBid].data() + sizeof(Header);
    size_t sizeWritten = 0;
    size_t buffersRead = 0;
    size_t tuplesRead = 0;
    size_t tuplesPerBuffer = 0;
    bool keep = true;


    const char *conninfo;
    PGconn *connection = NULL;
    // TODO: attention! `hostAddr` is for IPs while `host` is for hostnames, handle correctly
    conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
    connection = PQconnectdb(conninfo);

    while (curPart.id != -1) {

        char *receiveBuffer = NULL;
        int receiveLength = 0;
        const int asynchronous = 0;
        PGresult *res;

        std::string qStr =
                "COPY (SELECT " + getAttributesAsStr(xdbcEnv->schema) + " FROM " + tableName +
                " WHERE ctid BETWEEN '(" +
                std::to_string(curPart.startOff) + ",0)'::tid AND '(" +
                std::to_string(curPart.endOff) + ",0)'::tid) TO STDOUT WITH (FORMAT text, DELIMITER '|')";

        spdlog::get("XDBC.SERVER")->info("PG thread {0} runs query: {1}", thr, qStr);

        res = PQexec(connection, qStr.c_str());
        ExecStatusType resType = PQresultStatus(res);

        if (resType != PGRES_COPY_OUT)
            spdlog::get("XDBC.SERVER")->error("PG thread {0}: RESULT of COPY is {1}", thr, resType);

        receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);


        spdlog::get("XDBC.SERVER")->info("PG Read thread {0}: Entering PQgetCopyData loop with rcvlen: {1}", thr,
                                         receiveLength);


        while (receiveLength > 0) {

            tuplesRead++;
            tuplesPerBuffer++;

            if (((writePtr - bp[curBid].data() + receiveLength) > (bp[curBid].size() - sizeof(Header)))) {

                tuplesPerBuffer = 0;
                // Buffer is full, send it and fetch a new buffer
                Header head{};
                head.totalSize = sizeWritten;
                std::memcpy(bp[curBid].data(), &head, sizeof(Header));
                sizeWritten = 0;
                xdbcEnv->deserBufferPtr->push(curBid);

                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});

                //spdlog::get("XDBC.SERVER")->info("CSV thread {0}: sent buff {1} to deserQ {2}", thr, curBid, deserQ);

                curBid = xdbcEnv->readBufferPtr->pop();
                /*keep = !keep;
                if (thr == 0 && keep)
                    spdlog::get("XDBC.SERVER")->info("CSV thread {0}: size {1}", thr,
                                                     xdbcEnv->moreBuffersQ[thr]->size());
                //test forwarding
                if (thr == 0 && keep && xdbcEnv->moreBuffersQ[thr]->size() > 0) {

                    int requestThrId = xdbcEnv->moreBuffersQ[thr]->pop();
                    spdlog::get("XDBC.SERVER")->info("CSV thread {0}: forwarding to {1}", thr, requestThrId);
                    xdbcEnv->deserBufferPtr[requestThrId]->push(curBid);
                    curBid = xdbcEnv->readBufferPtr[thr]->pop();

                }*/
                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});
                //spdlog::get("XDBC.SERVER")->info("CSV thread {0}: got buff {1} ", thr, curBid);

                writePtr = bp[curBid].data() + sizeof(Header);
                buffersRead++;
            }
            std::memcpy(writePtr, receiveBuffer, receiveLength);
            writePtr += receiveLength;
            sizeWritten += receiveLength;
            PQfreemem(receiveBuffer);
            receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);

        }

        curPart = xdbcEnv->partPtr->pop();
        spdlog::get("XDBC.SERVER")->error("PG thread {0}: Exiting PQgetCopyData loop, tupleNo: {1}", thr, tuplesRead);

        // we now check the last received length returned by copy data
        if (receiveLength == 0) {
            // we cannot read more data without blocking
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

    spdlog::get("XDBC.SERVER")->info("PG read thread {0} finished reading", thr);

    PQfinish(connection);

    Header head{};
    head.totalSize = sizeWritten;
    //send the last buffer & notify the end
    std::memcpy(bp[curBid].data(), &head, sizeof(Header));
    xdbcEnv->deserBufferPtr->push(curBid);
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});

    xdbcEnv->deserBufferPtr->push(-1);

    int deserFinishedCounter = 0;

    //TODO: this can be refactored to forward all empty buffers unti deser is finished
    while (thr == 0 && deserFinishedCounter < xdbcEnv->deser_parallelism) {
        int requestThrId = xdbcEnv->moreBuffersQ->pop();

        if (requestThrId == -1)
            deserFinishedCounter += 1;
        else {
            //spdlog::get("XDBC.SERVER")->info("Read thr {0} waiting for free buff", thr);
            curBid = xdbcEnv->readBufferPtr->pop();
            //spdlog::get("XDBC.SERVER")->info("Read thr {0} sending buff {1} to deser thr {2}",
            //thr, curBid, requestThrId);

            xdbcEnv->deserBufferPtr->push(curBid);
        }

    }
    xdbcEnv->activeReadThreads[thr] = false;

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "end"});
    spdlog::get("XDBC.SERVER")->info("PG read thread {0} finished. #tuples: {1}, #buffers {2}",
                                     thr, tuplesRead, buffersRead);

    return 1;
}
