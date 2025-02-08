#include "postgres.h"  // Must be first
#include <cstdint>    // For standard integer types
#include "executor/spi.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "utils/memutils.h"
#include "spdlog/spdlog.h"

#include "PGInternalReader.h"
#include "/usr/include/postgresql/libpq-fe.h"
#include "../fast_float.h"
#include "../deserializers.h"
#include "../../xdbcserver.h"
#include <pqxx/pqxx>
#include <boost/asio.hpp>
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include <stack>
#include <charconv>
#include <queue>

using namespace std;
using namespace pqxx;
using namespace boost::asio;
undefined reference to `SPI_gettypeid(TupleDescData*, int)'

PG_MODULE_MAGIC;

PGInternalReader::PGInternalReader(RuntimeEnv &xdbcEnv, const std::string &tableName) :
        DataSource(xdbcEnv, tableName),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false),
        xdbcEnv(&xdbcEnv) {

    spdlog::get("XDBC.SERVER")->info("pg constructor called with table {0}", tableName);
}

int PGInternalReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool PGInternalReader::getFinishedReading() const {
    return finishedReading;
}

int PGInternalReader::getMaxCtId(const std::string &tableName) {

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
void PGInternalReader::readData() {
    auto start = std::chrono::steady_clock::now();
    int totalcnt = readDbData();
        
    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("read  | elapsed time: {0} ms for #tuples: {1}",
                                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalcnt);
}

int PGInternalReader::readDbData() {
    auto start_read = std::chrono::steady_clock::now();

    spdlog::get("XDBC.SERVER")->info("using spi connect to read pg data, parallelism: {0}", xdbcEnv->read_parallelism);



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

        xdbcEnv->partPtr[readQ]->push(p);

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
        xdbcEnv->partPtr[i]->push(fP);
        readThreads[i] = std::thread(&PGInternalReader::readPG, this, i);
        xdbcEnv->activeReadThreads[i] = true;

    }


    auto start_deser = std::chrono::steady_clock::now();
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        threadWrittenTuples[i] = 0;
        threadWrittenBuffers[i] = 0;

        deSerThreads[i] = std::thread(&PGInternalReader::deserializePG,
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

int 
PGInternalReader::readPG(int thr) {
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "start"});

    int curBid = xdbcEnv->readBufferPtr[thr]->pop();
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

    Part curpart = xdbcEnv->partPtr[thr]->pop();

    std::byte *writePtr = bp[curBid].data() + sizeof(Header);
    int deserQ = 0;
    size_t sizeWritten = 0;
    size_t buffersRead = 0;
    size_t tuplesRead = 0;
    size_t tuplesPerBuffer = 0;
    bool keep = true;
    int ret; 


    if ((ret = SPI_connect()) != SPI_OK_CONNECT) {
        spdlog::get("XDBC.SERVER")->error("spi connection failed: error code %d", ret);
    }
    
    while (curpart.id != -1) {

        char *receiveBuffer = nullptr; 

        int receiveLength = 0;
        const int asynchronous = 0;
        int rows;
        int i = 0;
        TupleDesc tupdesc;
        SPITupleTable *tuptable;
        HeapTuple tuple; 
        // modify to support more complex requests ?
        std::string qstr = "select " + getAttributesAsStr(xdbcEnv->schema) + " from " + tableName;
        
        // execute query
        ret = SPI_execute(qstr.c_str(), true, 0);

        if (ret != SPI_OK_SELECT) {
            SPI_finish();
            spdlog::get("XDBC.SERVER")->error("query execution failed or non-select query.");
        }

        rows = SPI_processed;
            
        tupdesc = SPI_tuptable->tupdesc;
        tuptable = SPI_tuptable;
        curpart = xdbcEnv->partPtr[thr]->pop();

        while (rows - i > 0) {
            tuplesRead++;
            tuplesPerBuffer++;
            
            // Calculate required space for all attributes
            size_t rowSize = 0;
            for (int attPos = 0; attPos < tupdesc->natts; attPos++) {
                rowSize += SPI_gettypeid(tupdesc, attPos + 1);
            }

            // Check if buffer has enough space
            if (((writePtr - bp[curBid].data() + rowSize) > (bp[curBid].size() - sizeof(Header)))) {
                tuplesPerBuffer = 0;
                // buffer is full, send it and fetch a new buffer
                Header head{};
                head.totalSize = sizeWritten;
                std::memcpy(bp[curBid].data(), &head, sizeof(Header));
                sizeWritten = 0;
                xdbcEnv->deserBufferPtr[deserQ]->push(curBid);

                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});

                deserQ = (deserQ + 1) % xdbcEnv->deser_parallelism;

                curBid = xdbcEnv->readBufferPtr[thr]->pop();

                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

                writePtr = bp[curBid].data() + sizeof(Header);
                buffersRead++;
            }

            // Write each attribute value using SPI_getbinval
            for (int attPos = 0; attPos < tupdesc->natts; attPos++) {
                bool isNull;
                Datum value = SPI_getbinval(tuptable->vals[i], tupdesc, attPos + 1, &isNull);
                
                if (!isNull) {
                    // Get the type and size of the attribute
                    Oid typeId = SPI_gettypeid(tupdesc, attPos + 1);
                    size_t attSize = 0;
                    
                    // Handle different PostgreSQL types
                    switch(typeId) {
                        case INT4OID:
                            attSize = sizeof(int32);
                            *(int32*)writePtr = DatumGetInt32(value);
                            break;
                        case INT8OID:
                            attSize = sizeof(int64);
                            *(int64*)writePtr = DatumGetInt64(value);
                            break;
                        case FLOAT4OID:
                            attSize = sizeof(float4);
                            *(float4*)writePtr = DatumGetFloat4(value);
                            break;
                        case FLOAT8OID:
                            attSize = sizeof(float8);
                            *(float8*)writePtr = DatumGetFloat8(value);
                            break;
                        // Add other types as needed
                    }
                    
                    writePtr += attSize;
                    sizeWritten += attSize;
                }
            }
            i += 1;
        }

        spdlog::get("XDBC.SERVER")->error("pg thread {0}: exiting pqgetcopydata loop, tupleno: {1}", thr, tuplesRead);

        // we now check the last received length returned by copy data
        if (rows == 0) {
            // we cannot read more data without blocking
            spdlog::get("XDBC.SERVER")->warn("pg reader received 0");
        } else if (rows < 0) {
            /* received an error */
            spdlog::get("XDBC.SERVER")->warn("PG thread {0} Copy failed bc -2", thr);
        }     

        spdlog::get("XDBC.SERVER")->info("PG read thread {0} finished reading", thr);

        SPI_freetuptable(SPI_tuptable);
    }
    SPI_finish();

    Header head{};
    head.totalSize = sizeWritten;
    //send the last buffer & notify the end
    std::memcpy(bp[curBid].data(), &head, sizeof(Header));
    xdbcEnv->deserBufferPtr[deserQ]->push(curBid);
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});


    for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
        xdbcEnv->deserBufferPtr[i]->push(-1);

    int deserFinishedCounter = 0;
    while (thr == 0 && deserFinishedCounter < xdbcEnv->deser_parallelism) {
        int requestThrId = xdbcEnv->moreBuffersQ[thr]->pop();

        if (requestThrId == -1)
            deserFinishedCounter += 1;
        else {

            //spdlog::get("XDBC.SERVER")->info("Read thr {0} waiting for free buff", thr);
            curBid = xdbcEnv->readBufferPtr[thr]->pop();
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});
            /*spdlog::get("XDBC.SERVER")->info("Read thr {0} sending buff {1} to deser thr {2}",
                                             thr, curBid, requestThrId);*/

            xdbcEnv->deserBufferPtr[requestThrId]->push(curBid);
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});

        }


    }
    xdbcEnv->activeReadThreads[thr] = false;

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "end"});
    spdlog::get("XDBC.SERVER")->info("PG read thread {0} finished. #tuples: {1}, #buffers {2}",
                                     thr, tuplesRead, buffersRead);

    return 1;
}


int
PGInternalReader::deserializePG(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

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
    int compQ = 0;
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
            int curBid = xdbcEnv->deserBufferPtr[thr]->pop();
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
            xdbcEnv->moreBuffersQ[0]->push(thr);

            auto start_wait = std::chrono::high_resolution_clock::now();

            int curBid = xdbcEnv->deserBufferPtr[thr]->pop();
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


                xdbcEnv->compBufferPtr[compQ]->push(curWriteBuffer);
                xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});
                compQ = (compQ + 1) % xdbcEnv->compression_parallelism;

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

        xdbcEnv->compBufferPtr[compQ]->push(curWriteBuffer);
        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

        totalThreadWrittenBuffers++;
    }


    //notify that we will not request other buffers
    for (int i = 0; i < xdbcEnv->read_parallelism; i++)
        xdbcEnv->moreBuffersQ[i]->push(-1);

    /*else
        spdlog::get("XDBC.SERVER")->info("PG thread {0} has no remaining tuples", thr);*/

    spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} finished. buffers: {1}, tuples {2}",
                                     thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);

    for (int i = 0; i < xdbcEnv->compression_parallelism; i++)
        xdbcEnv->compBufferPtr[i]->push(-1);

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "end"});

    return 1;
}


