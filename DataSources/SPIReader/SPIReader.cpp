#include "SPIReader.h"
#include <iostream>
#include <thread>
#include <cstring>
#include <charconv>
#include "spdlog/spdlog.h"
#include "../../xdbcserver.h"

// Postgres headers
extern "C" {
#include "postgres.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"
}

using namespace std;

SPIReader::SPIReader(RuntimeEnv &xdbcEnv, const std::string &tableName) :
        DataSource(xdbcEnv, tableName),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false),
        xdbcEnv(&xdbcEnv) {
    
    // Initialize Blocking Queues with capacity
    batchQueue = std::make_shared<customQueue<RawBatch*>>(10);       // Max 10 filled batches waiting
    freeBatchQueue = std::make_shared<customQueue<RawBatch*>>(10);   // Max 10 free batches recycling
    
    // Pre-allocate pools
    for(int i = 0; i < 10; ++i) {
        RawBatch* b = new RawBatch();
        batchPool.push_back(b);
        freeBatchQueue->push(b);
    }
}

int SPIReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool SPIReader::getFinishedReading() const {
    return finishedReading;
}

void SPIReader::readData() {
    auto start = std::chrono::steady_clock::now();

    // Launch deserializer threads (Consumers of formatted buffers)
    std::vector<int> threadWrittenTuples(xdbcEnv->deser_parallelism, 0);
    std::vector<int> threadWrittenBuffers(xdbcEnv->deser_parallelism, 0);
    std::vector<std::thread> deSerThreads(xdbcEnv->deser_parallelism);

    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        deSerThreads[i] = std::thread(&SPIReader::deserializeSPI,
                                      this, i,
                                      std::ref(threadWrittenTuples[i]), std::ref(threadWrittenBuffers[i])
        );
    }
    
    // Launch Batch Processor Thread (Consumer of RawBatches)
    // This thread performs formatting/serialization decoupled from SPI
    std::thread consumerThread(&SPIReader::processBatches, this);
    
    // Perform SPI reading on THIS thread (Producer)
    int totalCnt = readSPI();

    // Signal Consumer to finish
    batchQueue->push(nullptr); // Sentinel
    consumerThread.join();

    // Join deserializers
    int totalTuples = 0;
    int totalBuffers = 0;
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        deSerThreads[i].join();
        totalTuples += threadWrittenTuples[i];
        totalBuffers += threadWrittenBuffers[i];
    }
    
    // Cleanup Pool
    for(auto b : batchPool) {
        delete b;
    }
    batchPool.clear();

    finishedReading.store(true);

    auto end = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::cerr << "SPIReader | Elapsed time: " << total_time << " ms for #tuples: " << totalTuples << std::endl;
}

int SPIReader::readSPI() {
    std::cerr << "[SPIReader] Starting SPI Read on main thread" << std::endl;
    
    MemoryContext oldcontext = CurrentMemoryContext;
    if (SPI_connect() != SPI_OK_CONNECT) return -1;

    std::string cols = getAttributesAsStr(xdbcEnv->schema);
    std::string qStr = "SELECT " + cols + " FROM " + tableName;
    SPIPlanPtr plan = SPI_prepare(qStr.c_str(), 0, NULL);
    if (!plan) { SPI_finish(); return -1; }
    
    Portal portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
    if (!portal) { SPI_finish(); return -1; }
    
    std::cerr << "[SPIReader] Cursor opened successfully" << std::endl;

    long fetch_size = 2000; // adjustable batch size
    bool more_data = true;
    long total_fetched = 0;
    
    // Reusable arrays for Datums
    int capacity_natts = 0;
    Datum *values = nullptr;
    bool *nulls = nullptr;

    while (more_data) {
        // Get a free batch buffer
        RawBatch* batch = freeBatchQueue->pop();
        if(!batch) break; // Should not happen unless error
        batch->reset();

        SPI_cursor_fetch(portal, true, fetch_size);
        if (SPI_processed == 0) {
            freeBatchQueue->push(batch); // Return unused
            more_data = false;
            break;
        }

        SPITupleTable *tuptable = SPI_tuptable;
        uint64 processed = SPI_processed;
        TupleDesc tupdesc = tuptable->tupdesc;

        if (tupdesc->natts > capacity_natts) {
             if (values) pfree(values);
             if (nulls) pfree(nulls);
             capacity_natts = tupdesc->natts;
             values = (Datum *) palloc(capacity_natts * sizeof(Datum));
             nulls = (bool *) palloc(capacity_natts * sizeof(bool));
        }

        for (uint64_t i = 0; i < processed; i++) {
            heap_deform_tuple(tuptable->vals[i], tupdesc, values, nulls);
            
            // Serialize row to RawBatch
            for (int j = 0; j < xdbcEnv->schema.size(); j++) {
                char tpe = xdbcEnv->schema[j].tpe[0];
                bool isNull = (j < tupdesc->natts) ? nulls[j] : true;
                
                // Write Null Flag (1=NotNull, 0=Null)
                char notNullFlag = isNull ? 0 : 1;
                batch->data.push_back(notNullFlag);
                
                if (!isNull) {
                    if (tpe == 'I') {
                        int32 val = DatumGetInt32(values[j]);
                        char* ptr = (char*)&val;
                        batch->data.insert(batch->data.end(), ptr, ptr + 4);
                    } else if (tpe == 'D') {
                        float8 val = DatumGetFloat8(values[j]);
                        char* ptr = (char*)&val;
                        batch->data.insert(batch->data.end(), ptr, ptr + 8);
                    } else if (tpe == 'S' || tpe == 'C') {
                        struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(values[j]);
                        char *str_data = VARDATA_ANY(detoasted);
                        int str_len = VARSIZE_ANY_EXHDR(detoasted);
                        
                        // Write Length
                        char* lenPtr = (char*)&str_len;
                        batch->data.insert(batch->data.end(), lenPtr, lenPtr + 4);
                        // Write Data
                        batch->data.insert(batch->data.end(), str_data, str_data + str_len);
                        
                        if ((void *)detoasted != (void *)DatumGetPointer(values[j])) {
                            pfree(detoasted);
                        }
                    } else {
                         // Fallback for unknown types (ignore or empty)
                         char t = 0;
                         batch->data.push_back(t); // Dummy length 0
                    }
                }
            }
            batch->rowCount++;
        }
        
        SPI_freetuptable(tuptable);
        total_fetched += processed;
        
        // Push full batch to consumer
        batchQueue->push(batch);
    }
    
    if (values) pfree(values);
    if (nulls) pfree(nulls);

    SPI_cursor_close(portal);
    SPI_finish();
    MemoryContextSwitchTo(oldcontext);

    std::cerr << "[SPIReader] Producer finished. Total rows: " << total_fetched << std::endl;
    return total_fetched;
}


void SPIReader::processBatches() {
    std::cerr << "[SPIReader] processBatches: skip_deserializer = " 
              << (xdbcEnv->skip_deserializer ? "TRUE" : "FALSE") << std::endl;
    
    if (xdbcEnv->skip_deserializer) {
        std::cerr << "[SPIReader] -> Taking BINARY path (processBatchesBinary)" << std::endl;
        processBatchesBinary();
    } else {
        std::cerr << "[SPIReader] -> Taking TEXT path (processBatchesText)" << std::endl;
        processBatchesText();
    }
}

void SPIReader::processBatchesText() {
    // EXISTING TEXT MODE - kept unchanged
    int curBid = xdbcEnv->freeBufferPtr->pop();
    std::memset(bp[curBid].data(), 0, bp[curBid].size());
    
    Header *head = reinterpret_cast<Header *>(bp[curBid].data());
    char *writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
    size_t currentBufferUsed = 0;
    
    std::string row_str;
    row_str.reserve(1024);
    
    while(true) {
        RawBatch* batch = batchQueue->pop();
        if (!batch) break; // Sentinel
        
        const char* readPtr = batch->data.data();
        const char* endPtr = readPtr + batch->data.size();
        
        for(int i=0; i<batch->rowCount; ++i) {
            row_str.clear();
            
            for (int j = 0; j < xdbcEnv->schema.size(); j++) {
                char tpe = xdbcEnv->schema[j].tpe[0];
                char notNullFlag = *readPtr++;
                
                if (notNullFlag) {
                    if (tpe == 'I') {
                        int32 val;
                        memcpy(&val, readPtr, 4); readPtr += 4;
                        char buf[32];
                        auto res = std::to_chars(buf, buf + 32, val);
                        row_str.append(buf, res.ptr - buf);
                    } else if (tpe == 'D') {
                        float8 val;
                        memcpy(&val, readPtr, 8); readPtr += 8;
                        char buf[64];
                        auto res = std::to_chars(buf, buf + 64, val);
                        row_str.append(buf, res.ptr - buf);
                    } else if (tpe == 'S' || tpe == 'C') {
                        int len;
                        memcpy(&len, readPtr, 4); readPtr += 4;
                        row_str.append(readPtr, len);
                        readPtr += len;
                    }
                }
                
                if (j < xdbcEnv->schema.size() - 1) row_str += "|";
            }
            row_str += "\n";
            
            // Check overflow
            if (currentBufferUsed + row_str.length() > (xdbcEnv->buffer_size * 1024)) {
                 head->totalSize = currentBufferUsed;
                 head->totalTuples = 0; 
                 xdbcEnv->deserBufferPtr->push(curBid);
                 
                 curBid = xdbcEnv->freeBufferPtr->pop();
                 std::memset(bp[curBid].data(), 0, bp[curBid].size()); // Safety
                 
                 head = reinterpret_cast<Header *>(bp[curBid].data());
                 writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
                 currentBufferUsed = 0;
            }

            memcpy(writePtr, row_str.c_str(), row_str.length());
            writePtr += row_str.length();
            currentBufferUsed += row_str.length();
        }
        
        freeBatchQueue->push(batch);
    }
    
    // Push last buffer
    if (currentBufferUsed > 0) {
         head->totalSize = currentBufferUsed;
         head->totalTuples = 0;
         xdbcEnv->deserBufferPtr->push(curBid);
    } else {
         xdbcEnv->freeBufferPtr->push(curBid);
    }

    // Terminate downstream
    xdbcEnv->finishedReadThreads.store(1);
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
        xdbcEnv->deserBufferPtr->push(-1);
}

void SPIReader::processBatchesBinary() {
    // NEW BINARY MODE - writes directly to compressor, bypassing deserializer
    int curBid = xdbcEnv->freeBufferPtr->pop();
    std::memset(bp[curBid].data(), 0, bp[curBid].size());
    
    Header *head = reinterpret_cast<Header *>(bp[curBid].data());
    char *writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
    int tupleCount = 0;
    
    // Pre-compute column offsets in schema
    std::vector<size_t> offsetsInTuple(xdbcEnv->schema.size());
    size_t offset = 0;
    for (int j = 0; j < xdbcEnv->schema.size(); j++) {
        offsetsInTuple[j] = offset;
        offset += xdbcEnv->schema[j].size;
    }
    
    while(true) {
        RawBatch* batch = batchQueue->pop();
        if (!batch) break; // Sentinel
        
        const char* readPtr = batch->data.data();
        
        for(int i=0; i<batch->rowCount; ++i) {
            // For each row, convert RawBatch binary format to intermediate binary format
            for (int j = 0; j < xdbcEnv->schema.size(); j++) {
                char tpe = xdbcEnv->schema[j].tpe[0];
                char notNullFlag = *readPtr++;
                
                void *dest;
                if (xdbcEnv->iformat == 1) { // Row-major
                    dest = writePtr + tupleCount * xdbcEnv->tuple_size + offsetsInTuple[j];
                } else if (xdbcEnv->iformat == 2) { // Column-major
                    dest = writePtr + offsetsInTuple[j] * xdbcEnv->tuples_per_buffer + 
                           tupleCount * xdbcEnv->schema[j].size;
                } else {
                    // Unsupported format, fallback to row-major
                    dest = writePtr + tupleCount * xdbcEnv->tuple_size + offsetsInTuple[j];
                }
                
                if (notNullFlag) {
                    if (tpe == 'I') {
                        memcpy(dest, readPtr, 4);
                        readPtr += 4;
                    } else if (tpe == 'D') {
                        memcpy(dest, readPtr, 8);
                        readPtr += 8;
                    } else if (tpe == 'S' || tpe == 'C') {
                        int len;
                        memcpy(&len, readPtr, 4); readPtr += 4;
                        // Pad/truncate to schema size
                        int copyLen = std::min(len, xdbcEnv->schema[j].size);
                        memcpy(dest, readPtr, copyLen);
                        if (copyLen < xdbcEnv->schema[j].size) {
                            memset((char*)dest + copyLen, 0, xdbcEnv->schema[j].size - copyLen);
                        }
                        readPtr += len;
                    }
                } else {
                    // Write zeros for NULL values
                    memset(dest, 0, xdbcEnv->schema[j].size);
                }
            }
            
            tupleCount++;
            
            // Check if buffer is full
            if (tupleCount == xdbcEnv->tuples_per_buffer) {
                head->totalTuples = tupleCount;
                head->totalSize = tupleCount * xdbcEnv->tuple_size;
                head->intermediateFormat = xdbcEnv->iformat;
                
                // Push to DESER queue for pass through (matching PQReader pattern)
                xdbcEnv->deserBufferPtr->push(curBid);
                
                curBid = xdbcEnv->freeBufferPtr->pop();
                std::memset(bp[curBid].data(), 0, bp[curBid].size());
                head = reinterpret_cast<Header *>(bp[curBid].data());
                writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
                tupleCount = 0;
            }
        }
        
        freeBatchQueue->push(batch);
    }
    
    // Push last partial buffer
    if (tupleCount > 0) {
        head->totalTuples = tupleCount;
        head->totalSize = tupleCount * xdbcEnv->tuple_size;
        if (xdbcEnv->iformat == 2) {
            head->totalSize = xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size;
        }
        head->intermediateFormat = xdbcEnv->iformat;
        // CRITICAL: Push to DESER queue, not COMP queue!
        // Deserializer threads will passthrough when skip=on
        xdbcEnv->deserBufferPtr->push(curBid);
    } else {
        xdbcEnv->freeBufferPtr->push(curBid);
    }
    
    std::cerr << "[SPIReader] processBatchesBinary FINISHED - pushed " << tupleCount 
              << " final tuples to deserializer queue" << std::endl;
    
    // Signal deserializer threads to finish (they will passthrough and signal compressor)
    xdbcEnv->finishedReadThreads.store(1);
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
        xdbcEnv->deserBufferPtr->push(-1);
}

// Ensure we have the deserialize templates available
#include "../deserializers.h"
#include "../fast_float.h"

// Copy of deserializePG logic, adapted if needed. 
// Actually it's identical since the input format (pipe separated) is the same.
// We just need to implement it here because PGReader's method is private/member.

int SPIReader::deserializeSPI(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {
    
    // Passthrough mode when skip_deserializer is enabled
    if (xdbcEnv->skip_deserializer) {
        while (true) {
            int inBid = xdbcEnv->deserBufferPtr->pop();
            if (inBid == -1) break;
            
            xdbcEnv->compBufferPtr->push(inBid);
            totalThreadWrittenBuffers++;
        }
        
        // Signal completion
        xdbcEnv->finishedDeserThreads.fetch_add(1);
        if (xdbcEnv->finishedDeserThreads == xdbcEnv->deser_parallelism) {
            for (int i = 0; i < xdbcEnv->compression_parallelism; i++)
                xdbcEnv->compBufferPtr->push(-1);
        }
        
        return 1;
    }
    
    // TEXT DESERIALIZATION MODE (existing logic)
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
    
    outBid = xdbcEnv->freeBufferPtr->pop();
    int inBid = xdbcEnv->deserBufferPtr->pop();
    
    while (true) {
         if (inBid == -1) break;

        const std::vector<std::byte> &curReadBufferRef = bp[inBid];
        const auto *header = reinterpret_cast<const Header *>(curReadBufferRef.data());
        const std::byte *dataAfterHeader = curReadBufferRef.data() + sizeof(Header);

        while (readOffset < header->totalSize) {

            startReadPtr = reinterpret_cast<const char *>(dataAfterHeader + readOffset);
            startWritePtr = bp[outBid].data() + sizeof(Header);

            bytesInTuple = 0;

            for (int attPos = 0; attPos < schemaSize; attPos++) {
                auto &attribute = xdbcEnv->schema[attPos];
                
                // Find delimiter
                endPtr = (attPos < schemaSize - 1) ? strchr(startReadPtr, '|') : strchr(startReadPtr, '\n');
                
                // Safety check if malformed
                if (!endPtr) break; 

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
                head.totalSize = head.totalTuples * xdbcEnv->tuple_size;
                head.intermediateFormat = xdbcEnv->iformat;
                memcpy(bp[outBid].data(), &head, sizeof(Header));
                bufferTupleId = 0;

                totalThreadWrittenBuffers++;
                xdbcEnv->compBufferPtr->push(outBid);
                outBid = xdbcEnv->freeBufferPtr->pop();
            }
        }

        xdbcEnv->freeBufferPtr->push(inBid);
        inBid = xdbcEnv->deserBufferPtr->pop();
        readOffset = 0;
    }

    // Remaining tuples
    if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->tuples_per_buffer) {
        Header head{};
        head.totalTuples = bufferTupleId;
        head.totalSize = head.totalTuples * xdbcEnv->tuple_size;
        if (xdbcEnv->iformat == 2)
            head.totalSize = xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size;
        head.intermediateFormat = xdbcEnv->iformat;
        memcpy(bp[outBid].data(), &head, sizeof(Header));

        xdbcEnv->compBufferPtr->push(outBid);
        totalThreadWrittenBuffers++;
    }

    xdbcEnv->finishedDeserThreads.fetch_add(1);
    if (xdbcEnv->finishedDeserThreads == xdbcEnv->deser_parallelism) {
        for (int i = 0; i < xdbcEnv->compression_parallelism; i++)
            xdbcEnv->compBufferPtr->push(-1);
    }

    return 1;
}
