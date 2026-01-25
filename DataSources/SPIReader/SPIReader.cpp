#include "SPIReader.h"
#include <iostream>
#include <thread>
#include <cstring>
#include "spdlog/spdlog.h"
#include "../../xdbcserver.h"

// Postgres headers
extern "C" {
#include "postgres.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
}

using namespace std;

SPIReader::SPIReader(RuntimeEnv &xdbcEnv, const std::string &tableName) :
        DataSource(xdbcEnv, tableName),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false),
        xdbcEnv(&xdbcEnv) {
    std::cerr << "[SPIReader] SPI Constructor called with table " << tableName << std::endl;
}

int SPIReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool SPIReader::getFinishedReading() const {
    return finishedReading;
}

void SPIReader::readData() {
    auto start = std::chrono::steady_clock::now();

    // In SPI mode, readData() is called directly on the main thread.
    // However, deserialization can still happen in background threads.
    
    // Launch deserializer threads explicitly since we are "reading"
    std::vector<int> threadWrittenTuples(xdbcEnv->deser_parallelism, 0);
    std::vector<int> threadWrittenBuffers(xdbcEnv->deser_parallelism, 0);
    std::vector<std::thread> deSerThreads(xdbcEnv->deser_parallelism);

    auto start_deser = std::chrono::steady_clock::now();
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        deSerThreads[i] = std::thread(&SPIReader::deserializeSPI,
                                      this, i,
                                      std::ref(threadWrittenTuples[i]), std::ref(threadWrittenBuffers[i])
        );
    }
    
    // Perform SPI reading on THIS thread (main thread)
    int totalCnt = readSPI();

    // Join deserializers
    int totalTuples = 0;
    int totalBuffers = 0;
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        deSerThreads[i].join();
        totalTuples += threadWrittenTuples[i];
        totalBuffers += threadWrittenBuffers[i];
    }

    finishedReading.store(true);

    auto end = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::cerr << "SPIReader | Elapsed time: " << total_time << " ms for #tuples: " << totalTuples << std::endl;
}

int SPIReader::readSPI() {
    std::cerr << "[SPIReader] Starting SPI Read on main thread" << std::endl;

    // Connect to SPI
    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        std::cerr << "[SPIReader] SPI_connect failed: " << ret << std::endl;
        return -1;
    }

    // Prepare query: SELECT * FROM table
    // Construct columns string
    std::string cols = getAttributesAsStr(xdbcEnv->schema);
    std::string qStr = "SELECT " + cols + " FROM " + tableName;
    
    std::cerr << "[SPIReader] Executing SPI Query: " << qStr << std::endl;

    // Executing simple SPI_execute instead of cursor to verify stability
    int res = SPI_execute(qStr.c_str(), true, 0); // read_only=true, limit=0 (all)
    
    if (res < 0) {
        std::cerr << "[SPIReader] SPI_execute failed: " << res << std::endl;
        SPI_finish();
        return -1;
    }
    
    long total_fetched = SPI_processed;
    std::cerr << "[SPIReader] SPI_execute success. Processed: " << total_fetched << std::endl;

    int curBid = xdbcEnv->freeBufferPtr->pop();
    Header *head = reinterpret_cast<Header *>(bp[curBid].data());
    char *writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
    size_t currentBufferUsed = 0;
    
    if (total_fetched > 0 && SPI_tuptable != NULL) {
        for (uint64_t i = 0; i < total_fetched; i++) {
            std::string row_str = "";
            for (int j = 1; j <= SPI_tuptable->tupdesc->natts; j++) {
                char *val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, j);
                if (val) {
                    row_str += val;
                }
                if (j < SPI_tuptable->tupdesc->natts) {
                    row_str += "|";
                }
            }
            row_str += "\n";

            // Check overflow
            if (currentBufferUsed + row_str.length() > (xdbcEnv->buffer_size * 1024)) {
                 head->totalSize = currentBufferUsed;
                 head->totalTuples = 0; 
                 xdbcEnv->deserBufferPtr->push(curBid);
                 
                 curBid = xdbcEnv->freeBufferPtr->pop();
                 head = reinterpret_cast<Header *>(bp[curBid].data());
                 writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
                 currentBufferUsed = 0;
            }

            memcpy(writePtr, row_str.c_str(), row_str.length());
            writePtr += row_str.length();
            currentBufferUsed += row_str.length();
        }
        
        // SPI_freetuptable(SPI_tuptable); // Not needed if we finish? usually good practice
    }

    SPI_finish();


    // Push last buffer
    if (currentBufferUsed > 0) {
         head->totalSize = currentBufferUsed;
         head->totalTuples = 0;
         xdbcEnv->deserBufferPtr->push(curBid);
    } else {
         // Return unused buffer
         xdbcEnv->freeBufferPtr->push(curBid);
    }

    // Signal end to deserializers (Since read_parallelism is 1 for SPI)
    xdbcEnv->finishedReadThreads.store(1);
    
    // Push poison pills for deserializers
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
        xdbcEnv->deserBufferPtr->push(-1);
    
    std::cerr << "[SPIReader] SPI Read finished. Total rows: " << total_fetched << std::endl;
    return total_fetched;
    
    /*
    // Connect to SPI
    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        spdlog::get("XDBC.SERVER")->error("SPI_connect failed: {0}", ret);
        return -1;
    }

    // Prepare query: SELECT * FROM table
    // Construct columns string
    std::string cols = getAttributesAsStr(xdbcEnv->schema);
    std::string qStr = "SELECT " + cols + " FROM " + tableName;
    
    spdlog::get("XDBC.SERVER")->info("Executing SPI Query: {0}", qStr);

    // Open cursor to fetch incrementally
    Portal portal = SPI_cursor_open_with_args(NULL, qStr.c_str(), 0, NULL, NULL, NULL, true, 0);
    if (portal == NULL) {
        spdlog::get("XDBC.SERVER")->error("SPI_cursor_open failed");
        SPI_finish();
        return -1;
    }

    long fetch_size = 10000; // adjustable batch size
    bool more_data = true;
    long total_fetched = 0;
    
    // We need to write data into buffers for deserializers to pick up.
    // The format expected by deserializePG/SPI is pipe-delimited text.
    // e.g. "1|name_1|1.5\n"
    
    int curBid = xdbcEnv->freeBufferPtr->pop();
    Header *head = reinterpret_cast<Header *>(bp[curBid].data());
    // std::byte *writePtr = bp[curBid].data() + sizeof(Header); // header size
    char *writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
    size_t currentBufferUsed = 0;
    size_t tuplesInPacket = 0;

    // We can reuse PGReader logic but adapted to SPI_getvalue
    while (more_data) {
        SPI_cursor_fetch(portal, true, fetch_size);
        
        if (SPI_processed == 0) {
            more_data = false;
            break;
        }

        for (uint64_t i = 0; i < SPI_processed; i++) {
            std::string row_str = "";
            for (int j = 1; j <= SPI_tuptable->tupdesc->natts; j++) {
                char *val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, j);
                if (val) {
                    row_str += val;
                }
                if (j < SPI_tuptable->tupdesc->natts) {
                    row_str += "|";
                }
            }
            row_str += "\n";
            
            // Check overflow
            if (currentBufferUsed + row_str.length() > (xdbcEnv->buffer_size * 1024)) {
                 // Push buffer
                 head->totalSize = currentBufferUsed;
                 head->totalTuples = 0; // The deserializer counts real tuples
                 
                 xdbcEnv->deserBufferPtr->push(curBid);
                 
                 // Get new buffer
                 curBid = xdbcEnv->freeBufferPtr->pop();
                 head = reinterpret_cast<Header *>(bp[curBid].data());
                 writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
                 currentBufferUsed = 0;
            }

            // Copy to buffer
            memcpy(writePtr, row_str.c_str(), row_str.length());
            writePtr += row_str.length();
            currentBufferUsed += row_str.length();
            
            // Important: free SPI memory if needed, though SPI_getvalue returns palloc'd memory 
            // created in the current memory context. Since we are in a loop, we might accumulate.
            // But we are using a cursor so it might be per-fetch context.
            // Standard SPI loop usually manages contexts.
        }
        
        SPI_freetuptable(SPI_tuptable);
        total_fetched += SPI_processed;
    }

    SPI_cursor_close(portal);
    SPI_finish();

    // Push last buffer
    if (currentBufferUsed > 0) {
         head->totalSize = currentBufferUsed;
         head->totalTuples = 0;
         xdbcEnv->deserBufferPtr->push(curBid);
    } else {
         // Return unused buffer
         xdbcEnv->freeBufferPtr->push(curBid);
    }

    // Signal end to deserializers
    xdbcEnv->finishedReadThreads.fetch_add(1); // 1 read thread (this one)
    // We treat this as 1 "read thread" completing.
    // If read_parallelism > 1, the other threads won't exist in SPI mode, so we rely on this count.
    // Actually, xdbcEnv logic expects `finishedReadThreads == read_parallelism`.
    // We should set read_parallelism to 1 in xdbcserver.cpp for SPI mode.
    
    // If read_parallelism is set to 1, this works.
    if (xdbcEnv->read_parallelism <= 1) {
        for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
            xdbcEnv->deserBufferPtr->push(-1);
    } else {
        // Just force it?
         for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
            xdbcEnv->deserBufferPtr->push(-1);
    }
    
    spdlog::get("XDBC.SERVER")->info("SPI Read finished. Total rows: {0}", total_fetched);
    return total_fetched;
    */
}

// Ensure we have the deserialize templates available
#include "../deserializers.h"
#include "../fast_float.h"

// Copy of deserializePG logic, adapted if needed. 
// Actually it's identical since the input format (pipe separated) is the same.
// We just need to implement it here because PGReader's method is private/member.

int SPIReader::deserializeSPI(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {
    
    // Logic identical to PGReader::deserializePG
    // We need to copy-paste it because we can't easily inherit it without making it protected in PGReader
    // or moving it to a common base/helper.
    // Given the task, copy-paste is safer than refactoring PGReader right now.
    
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
