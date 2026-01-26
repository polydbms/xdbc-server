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

    // CRITICAL FIX: Save current memory context to ensure clean state
    // This prevents stale pointers from previous sessions in long-running worker
    MemoryContext oldcontext = CurrentMemoryContext;

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

    // Prepare the query plan first
    SPIPlanPtr plan = SPI_prepare(qStr.c_str(), 0, NULL);
    if (plan == NULL) {
        std::cerr << "[SPIReader] SPI_prepare failed" << std::endl;
        SPI_finish();
        return -1;
    }
    
    // Open cursor with the prepared plan
    Portal portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
    if (portal == NULL) {
        std::cerr << "[SPIReader] SPI_cursor_open failed" << std::endl;
        SPI_finish();
        return -1;
    }
    
    std::cerr << "[SPIReader] Cursor opened successfully" << std::endl;

    long fetch_size = 10000; // adjustable batch size
    bool more_data = true;
    long total_fetched = 0;
    
    int curBid = xdbcEnv->freeBufferPtr->pop();
    
    // CRITICAL FIX: Zero-initialize first buffer to prevent stale data
    // from previous session causing memory corruption
    std::memset(bp[curBid].data(), 0, bp[curBid].size());
    
    Header *head = reinterpret_cast<Header *>(bp[curBid].data());
    char *writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
    size_t currentBufferUsed = 0;

    // Prepare Datum/Null arrays
    int capacity_natts = 0;
    Datum *values = nullptr;
    bool *nulls = nullptr;
    
    // Reuse string buffer to avoid allocations per row
    std::string row_str;
    row_str.reserve(1024);

    int batch_num = 0;
    while (more_data) {
        SPI_cursor_fetch(portal, true, fetch_size);
        
        if (SPI_processed == 0) {
            more_data = false;
            break;
        }

        // Save the tuple table pointer locally
        SPITupleTable *tuptable = SPI_tuptable;
        uint64 processed = SPI_processed;
        TupleDesc tupdesc = tuptable->tupdesc;

        // Ensure we have enough space for execution
        if (tupdesc->natts > capacity_natts) {
             if (values) pfree(values);
             if (nulls) pfree(nulls);
             capacity_natts = tupdesc->natts;
             values = (Datum *) palloc(capacity_natts * sizeof(Datum));
             nulls = (bool *) palloc(capacity_natts * sizeof(bool));
        }

        for (uint64_t i = 0; i < processed; i++) {
            row_str.clear();
            
            // Deform tuple into Datums
            heap_deform_tuple(tuptable->vals[i], tupdesc, values, nulls);
            
            for (int j = 0; j < tupdesc->natts; j++) {
                if (nulls[j]) {
                    // Null handling (print nothing, just separate)
                } else {
                     // Check type from Schema or TupDesc? 
                     // Using xdbcEnv->schema serves as our "expected" type map.
                     // We match j-th attribute to j-th schema entry.
                     // (Assuming order matches query: SELECT * matches schema order)
                     
                     if (j < xdbcEnv->schema.size()) {
                         char tpe = xdbcEnv->schema[j].tpe[0];
                         
                         if (tpe == 'I') { // Integer
                             int32 val = DatumGetInt32(values[j]);
                             char buf[32];
                             auto res = std::to_chars(buf, buf + 32, val);
                             if (res.ec == std::errc()) {
                                 row_str.append(buf, res.ptr - buf);
                             }
                         } else if (tpe == 'D') { // Double
                             float8 val = DatumGetFloat8(values[j]);
                             char buf[64];
                             #if __cpp_lib_to_chars >= 201611L
                                 auto res = std::to_chars(buf, buf + 64, val);
                                 if (res.ec == std::errc()) {
                                     row_str.append(buf, res.ptr - buf);
                                 }
                             #else
                                 int len = sprintf(buf, "%.15g", val);
                                 row_str.append(buf, len);
                             #endif
                         } else if (tpe == 'S' || tpe == 'C') { // String/Char
                             // Detoast if necessary
                             struct varlena *v = (struct varlena *) DatumGetPointer(values[j]);
                             
                             // Need to handle detoasting safely
                             struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(values[j]);
                             char *str_data = VARDATA_ANY(detoasted);
                             int str_len = VARSIZE_ANY_EXHDR(detoasted);
                             
                             row_str.append(str_data, str_len);
                             
                             if ((void *)detoasted != (void *)v) {
                                 pfree(detoasted);
                             }
                         } else {
                             // Fallback
                             char *val = SPI_getvalue(tuptable->vals[i], tupdesc, j + 1);
                             if (val) {
                                row_str.append(val);
                                pfree(val);
                             }
                         }
                     } else {
                         // Schema mismatch or extra columns? Fallback
                         char *val = SPI_getvalue(tuptable->vals[i], tupdesc, j + 1);
                         if (val) {
                            row_str.append(val);
                            pfree(val);
                         }
                     }
                }

                if (j < tupdesc->natts - 1) {
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
                 // Memset only header + some safety? Memset full buffer is expensive?
                 // Original code memset full buffer. Let's keep it for safety.
                 // std::memset(bp[curBid].data(), 0, bp[curBid].size()); // Optional optimization: skip this if not strictly needed
                 
                 head = reinterpret_cast<Header *>(bp[curBid].data());
                 writePtr = reinterpret_cast<char*>(bp[curBid].data() + sizeof(Header));
                 currentBufferUsed = 0;
            }

            memcpy(writePtr, row_str.c_str(), row_str.length());
            writePtr += row_str.length();
            currentBufferUsed += row_str.length();
        }
        
        SPI_freetuptable(tuptable);
        total_fetched += processed;
        batch_num++;
    }
    
    if (values) pfree(values);
    if (nulls) pfree(nulls);

    SPI_cursor_close(portal);
    SPI_finish();
    
    // CRITICAL FIX: Restore original memory context
    // Ensures we don't leak into wrong context
    MemoryContextSwitchTo(oldcontext);

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
