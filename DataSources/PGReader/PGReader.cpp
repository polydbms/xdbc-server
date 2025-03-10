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

// TODO: refactor for new buffer_size -> tuples_per_buffer and deserialization method

std::vector<std::string> splitStr(std::string const &original, char separator)
{
    std::vector<std::string> results;
    std::string::const_iterator start = original.begin();
    std::string::const_iterator end = original.end();
    std::string::const_iterator next = std::find(start, end, separator);
    while (next != end)
    {
        results.emplace_back(start, next);
        start = next + 1;
        next = std::find(start, end, separator);
    }
    results.emplace_back(start, next);
    return results;
}

int fast_atoi(const char *str)
{
    int val = 0;
    while (*str)
    {
        val = val * 10 + (*str++ - '0');
    }
    return val;
}

unsigned int naive(const char *p)
{
    unsigned int x = 0;
    while (*p != '\0')
    {
        x = (x * 10) + (*p - '0');
        ++p;
    }
    return x;
}

enum STR2INT_ERROR
{
    SUCCESS,
    OVERFLOW,
    UNDERFLOW,
    INCONVERTIBLE
};

STR2INT_ERROR str2int(int &i, char const *s, int base = 0)
{
    char *end;
    long l;
    errno = 0;
    l = strtol(s, &end, base);
    if ((errno == ERANGE && l == LONG_MAX) || l > INT_MAX)
    {
        return OVERFLOW;
    }
    if ((errno == ERANGE && l == LONG_MIN) || l < INT_MIN)
    {
        return UNDERFLOW;
    }
    if (*s == '\0' || *end != '\0')
    {
        return INCONVERTIBLE;
    }
    i = l;
    return SUCCESS;
}

PGReader::PGReader(RuntimeEnv &xdbcEnv, const std::string &tableName) : DataSource(xdbcEnv, tableName),
                                                                        bp(*xdbcEnv.bpPtr),
                                                                        totalReadBuffers(0),
                                                                        finishedReading(false),
                                                                        xdbcEnv(&xdbcEnv)
{

    spdlog::get("XDBC.SERVER")->info("PG Constructor called with table {0}", tableName);
}

int PGReader::getTotalReadBuffers() const
{
    return totalReadBuffers;
}

bool PGReader::getFinishedReading() const
{
    return finishedReading;
}

int PGReader::getMaxCtId(const std::string &tableName)
{

    const char *conninfo;
    PGconn *connection = NULL;

    conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
    connection = PQconnectdb(conninfo);

    PGresult *res;
    std::string qStr = "SELECT (MAX(ctid)::text::point)[0]::bigint AS maxctid FROM " + tableName;
    // spdlog::get("XDBC.SERVER")->info("Getting max(ctid) with: {}", qStr);
    res = PQexec(connection, qStr.c_str());

    int fnum = PQfnumber(res, "maxctid");

    char *maxPtr = PQgetvalue(res, 0, fnum);

    int maxCtId = stoi(maxPtr);

    PQfinish(connection);
    return maxCtId;
}

void PGReader::readData()
{

    auto start = std::chrono::steady_clock::now();
    int totalCnt = 0;

    totalCnt = read_pq_copy();

    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("PGReader | Elapsed time: {0} ms for #tuples: {1}", std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), totalCnt);
}

int PGReader::read_pq_copy()
{
    xdbcEnv->env_manager_DS.start();
    auto start_read = std::chrono::steady_clock::now();

    spdlog::get("XDBC.SERVER")->info("Using pglib with COPY, parallelism: {0}", xdbcEnv->read_parallelism);

    std::vector<int> threadWrittenTuples(xdbcEnv->max_threads, 0);  // Initialize all elements to 0
    std::vector<int> threadWrittenBuffers(xdbcEnv->max_threads, 0); // Initialize all elements to 0
    thread readThreads[xdbcEnv->read_parallelism];
    thread deSerThreads[xdbcEnv->deser_parallelism];

    // TODO: throw something when table does not exist

    int maxRowNum = getMaxCtId(tableName);

    int partNum = xdbcEnv->read_partitions;
    div_t partSizeDiv = div(maxRowNum, partNum);

    int partSize = partSizeDiv.quot;

    if (partSizeDiv.rem > 0)
        partSize++;

    xdbcEnv->readPart_info.resize(partNum);
    for (int i = partNum - 1; i >= 0; i--)
    {
        Part p{};
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = UINT32_MAX;

        xdbcEnv->readPartPtr->push(i);
        xdbcEnv->readPart_info[i] = p;

        spdlog::get("XDBC.SERVER")->info("Partition {0} [{1},{2}] pushed into queue ", p.id, p.startOff, p.endOff);
    }

    //*** Create threads for read operation
    xdbcEnv->env_manager_DS.registerOperation("read", [&](int thr)
                                              { try {
    if (thr >= xdbcEnv->max_threads) {
    spdlog::get("XDBC.SERVER")->error("No of threads exceed limit");
    return;
    }
    xdbcEnv->readPartPtr->push(-1);
    readPG(thr);
    } catch (const std::exception& e) {
    spdlog::get("XDBC.SERVER")->error("Exception in thread {}: {}", thr, e.what());
    } catch (...) {
    spdlog::get("XDBC.SERVER")->error("Unknown exception in thread {}", thr);
    } }, xdbcEnv->readPartPtr);
    xdbcEnv->env_manager_DS.configureThreads("read", xdbcEnv->read_parallelism); // start read component threads
    //*** Finish creating threads for read operation

    auto start_deser = std::chrono::steady_clock::now();

    //*** Create threads for deserialize operation
    xdbcEnv->env_manager_DS.registerOperation("deserialize", [&](int thr)
                                              { try {
    if (thr >= xdbcEnv->max_threads) {
    spdlog::get("XDBC.SERVER")->error("No of threads exceed limit");
    return;
    }
    deserializePG(thr, threadWrittenTuples[thr], threadWrittenBuffers[thr]);
    } catch (const std::exception& e) {
    spdlog::get("XDBC.SERVER")->error("Exception in thread {}: {}", thr, e.what());
    } catch (...) {
    spdlog::get("XDBC.SERVER")->error("Unknown exception in thread {}", thr);
    } }, xdbcEnv->deserBufferPtr);
    xdbcEnv->env_manager_DS.configureThreads("deserialize", xdbcEnv->deser_parallelism); // start deserialize component threads
    //*** Finish creating threads for deserialize operation

    if (xdbcEnv->spawn_source == 1)
    {
        xdbcEnv->enable_updation_DS = 1;
    }
    while (xdbcEnv->enable_updation_DS == 1) // Reconfigure threads as long as it is allowed
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30000));
    xdbcEnv->read_parallelism = 4;
    xdbcEnv->env_manager_DS.configureThreads("read", xdbcEnv->read_parallelism); // start read component threads
    // Wait for read to finish and then kill deserialize
    xdbcEnv->env_manager_DS.joinThreads("read");
    xdbcEnv->env_manager_DS.configureThreads("deserialize", 0);
    xdbcEnv->env_manager_DS.joinThreads("deserialize");

    int totalTuples = 0;
    int totalBuffers = 0;
    for (int i = 0; i < xdbcEnv->max_threads; i++)
    {
        totalTuples += threadWrittenTuples[i];
        totalBuffers += threadWrittenBuffers[i];
    }

    finishedReading.store(true);

    auto end = std::chrono::steady_clock::now();
    auto total_read_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start_read).count();
    auto total_deser_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start_deser).count();

    xdbcEnv->env_manager_DS.stop(); // *** Stop Reconfigurration handler
    spdlog::get("XDBC.SERVER")->info("PG Read+Deser | Elapsed time: {0} ms for #tuples: {1}, #buffers: {2}", total_deser_time / 1000, totalTuples, totalBuffers);

    return totalTuples;
}

int PGReader::deserializePG(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers)
{
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "start"});

    int outBid;

    size_t readOffset = 0;
    const char *endPtr;
    size_t len;
    size_t bufferTupleId = 0;
    int bytesInTuple = 0;

    std::byte *startWritePtr;
    const char *startReadPtr;
    void *write;

    size_t schemaSize = xdbcEnv->schema.size();

    std::vector<size_t> sizes(schemaSize);
    std::vector<size_t> schemaChars(schemaSize);
    using DeserializeFunc = void (*)(const char *src, const char *end, void *dest, int attSize, size_t len);
    std::vector<DeserializeFunc> deserializers(schemaSize);

    for (size_t i = 0; i < schemaSize; ++i)
    {
        if (xdbcEnv->schema[i].tpe[0] == 'I')
        {
            sizes[i] = 4; // sizeof(int)
            schemaChars[i] = 'I';
            deserializers[i] = deserialize<int>;
        }
        else if (xdbcEnv->schema[i].tpe[0] == 'D')
        {
            sizes[i] = 8; // sizeof(double)
            schemaChars[i] = 'D';
            deserializers[i] = deserialize<double>;
        }
        else if (xdbcEnv->schema[i].tpe[0] == 'C')
        {
            sizes[i] = 1; // sizeof(char)
            schemaChars[i] = 'C';
            deserializers[i] = deserialize<char>;
        }
        else if (xdbcEnv->schema[i].tpe[0] == 'S')
        {
            sizes[i] = xdbcEnv->schema[i].size;
            schemaChars[i] = 'S';
            deserializers[i] = deserialize<const char *>;
        }
    }

    outBid = xdbcEnv->freeBufferPtr->pop();
    int inBid = xdbcEnv->deserBufferPtr->pop();
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});
    while (true)
    {

        const std::vector<std::byte> &curReadBufferRef = bp[inBid];
        const auto *header = reinterpret_cast<const Header *>(curReadBufferRef.data());
        const std::byte *dataAfterHeader = curReadBufferRef.data() + sizeof(Header);

        while (readOffset < header->totalSize)
        {

            startReadPtr = reinterpret_cast<const char *>(dataAfterHeader + readOffset);
            //+sizeof(Header) for temp header (totalTuples)
            startWritePtr = bp[outBid].data() + sizeof(Header);

            bytesInTuple = 0;

            for (int attPos = 0; attPos < schemaSize; attPos++)
            {

                // spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} processing schema", thr);

                auto &attribute = xdbcEnv->schema[attPos];

                endPtr = (attPos < schemaSize - 1) ? strchr(startReadPtr, '|') : strchr(startReadPtr, '\n');

                len = endPtr - startReadPtr;

                std::string_view tmp(startReadPtr, len);
                const char *tmpPtr = tmp.data();
                const char *tmpEnd = tmpPtr + len;
                startReadPtr = endPtr + 1;

                if (xdbcEnv->iformat == 1)
                {
                    write = startWritePtr + bufferTupleId * xdbcEnv->tuple_size + bytesInTuple;
                }
                else if (xdbcEnv->iformat == 2)
                {
                    write = startWritePtr + bytesInTuple * xdbcEnv->tuples_per_buffer + bufferTupleId * attribute.size;
                }

                deserializers[attPos](tmpPtr, tmpEnd, write, attribute.size, len);

                bytesInTuple += attribute.size;
                readOffset += len + 1;
            }

            bufferTupleId++;
            totalThreadWrittenTuples++;

            if (bufferTupleId == xdbcEnv->tuples_per_buffer)
            {
                Header head{};
                head.totalTuples = bufferTupleId;
                head.totalSize = head.totalTuples * xdbcEnv->tuple_size;
                head.intermediateFormat = xdbcEnv->iformat;
                memcpy(bp[outBid].data(), &head, sizeof(Header));
                bufferTupleId = 0;

                totalThreadWrittenBuffers++;

                xdbcEnv->pts->push(
                    ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

                xdbcEnv->compBufferPtr->push(outBid);
                outBid = xdbcEnv->freeBufferPtr->pop();
            }
        }

        // we are done with reading the incoming buffer contents, return it and get a new one
        xdbcEnv->freeBufferPtr->push(inBid);
        inBid = xdbcEnv->deserBufferPtr->pop();
        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});
        readOffset = 0;
        if (inBid == -1)
            break;
    }

    // remaining tuples
    if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->tuples_per_buffer)
    {
        spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} has {1} remaining tuples", thr, xdbcEnv->tuples_per_buffer - bufferTupleId);

        // write tuple count to tmp header
        Header head{};
        head.totalTuples = bufferTupleId;
        head.totalSize = head.totalTuples * xdbcEnv->tuple_size;
        if (xdbcEnv->iformat == 2)
            head.totalSize = xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size;
        head.intermediateFormat = xdbcEnv->iformat;
        memcpy(bp[outBid].data(), &head, sizeof(Header));

        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});
        xdbcEnv->compBufferPtr->push(outBid);

        totalThreadWrittenBuffers++;
    }

    spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} finished. buffers: {1}, tuples {2}", thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);

    xdbcEnv->finishedDeserThreads.fetch_add(1);

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "end"});

    return 1;
}

int PGReader::readPG(int thr)
{
    xdbcEnv->activeReadThreads.fetch_add(1);
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "start"});

    int curBid = xdbcEnv->freeBufferPtr->pop();
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

    int part_id = xdbcEnv->readPartPtr->pop();
    spdlog::get("XDBC.SERVER")->info("Read Thread {0} partition {1}", thr, part_id);
    Part curPart;

    std::byte *writePtr = bp[curBid].data() + sizeof(Header);
    size_t sizeWritten = 0;
    size_t buffersRead = 0;
    size_t tuplesRead = 0;
    size_t tuplesPerBuffer = 0;
    bool keep = true;

    const char *conninfo;
    PGconn *connection = NULL;
    // TODO: attention! `hostAddr` is for IPs while `host` is for hostnames, handle correctly
    // TODO: remove hardcoded info
    conninfo = "dbname = db1 user = postgres password = 123456 host = pg1 port = 5432";
    connection = PQconnectdb(conninfo);

    while (part_id != -1)
    {
        spdlog::get("XDBC.SERVER")->info("Read Thread {0} partition {1} time ", thr, part_id);
        curPart = xdbcEnv->readPart_info[part_id];
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

        spdlog::get("XDBC.SERVER")->info("PG Read thread {0}: Entering PQgetCopyData loop with rcvlen: {1}", thr, receiveLength);

        while (receiveLength > 0)
        {

            // Buffer is full, send it and fetch a new buffer
            if (((writePtr - bp[curBid].data() + receiveLength) > xdbcEnv->buffer_size * 1024))
            {
                Header head{};
                head.totalSize = sizeWritten;
                head.totalTuples = tuplesPerBuffer;
                std::memcpy(bp[curBid].data(), &head, sizeof(Header));
                sizeWritten = 0;

                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});
                xdbcEnv->deserBufferPtr->push(curBid);

                curBid = xdbcEnv->freeBufferPtr->pop();
                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

                writePtr = bp[curBid].data() + sizeof(Header);
                buffersRead++;
                tuplesPerBuffer = 0;
            }

            std::memcpy(writePtr, receiveBuffer, receiveLength);
            writePtr += receiveLength;
            sizeWritten += receiveLength;
            PQfreemem(receiveBuffer);
            receiveBuffer = NULL;
            receiveLength = PQgetCopyData(connection, &receiveBuffer, asynchronous);
            tuplesRead++;
            tuplesPerBuffer++;
        }

        part_id = xdbcEnv->readPartPtr->pop();
        spdlog::get("XDBC.SERVER")->info("PG thread {0}: Exiting PQgetCopyData loop, tupleNo: {1}", thr, tuplesRead);

        // we now check the last received length returned by copy data
        if (receiveLength == 0)
        {
            // we cannot read more data without blocking
            spdlog::get("XDBC.SERVER")->warn("PG Reader received 0");
        }
        else if (receiveLength == -1)
        {
            /* received copy done message */
            PGresult *result = PQgetResult(connection);
            ExecStatusType resultStatus = PQresultStatus(result);

            if (resultStatus != PGRES_COMMAND_OK)
            {
                spdlog::get("XDBC.SERVER")->warn("PG thread {0} Copy failed", thr);
            }

            PQclear(result);
        }
        else if (receiveLength == -2)
        {
            /* received an error */
            spdlog::get("XDBC.SERVER")->warn("PG thread {0} Copy failed bc -2", thr);
        }
        else if (receiveLength < 0)
        {
            /* if copy out completed, make sure we drain all results from libpq */
            PGresult *result = PQgetResult(connection);
            while (result != NULL)
            {
                PQclear(result);
                result = PQgetResult(connection);
            }
        }
    }

    spdlog::get("XDBC.SERVER")->info("PG read thread {0} finished reading", thr);

    PQfinish(connection);

    // send the last buffer & notify the end
    Header head{};
    head.totalSize = sizeWritten;
    head.totalTuples = tuplesPerBuffer;

    std::memcpy(bp[curBid].data(), &head, sizeof(Header));

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});
    xdbcEnv->deserBufferPtr->push(curBid);

    xdbcEnv->finishedReadThreads.fetch_add(1);
    xdbcEnv->activeReadThreads.fetch_add(-1);
    if (xdbcEnv->activeReadThreads == 0)
    {
        xdbcEnv->enable_updation_DS = 0;
        xdbcEnv->enable_updation_xServe = 0;
    }

    int deserFinishedCounter = 0;

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "end"});
    spdlog::get("XDBC.SERVER")->info("PG read thread {0} finished. #tuples: {1}, #buffers {2}", thr, tuplesRead, buffersRead);

    return 1;
}
