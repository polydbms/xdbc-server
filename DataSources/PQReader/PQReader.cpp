#include "PQReader.h"
#include "spdlog/spdlog.h"
#include "arrow/io/api.h"
#include <arrow/io/memory.h>
#include "parquet/stream_reader.h"
#include <parquet/metadata.h>
#include "../../xdbcserver.h"
#include "../deserializers_parquet.h"
#include <fstream>
#include <filesystem>

PQReader::PQReader(RuntimeEnv &xdbcEnv, const std::string &tableName) :
        DataSource(xdbcEnv, tableName),
        bp(*xdbcEnv.bpPtr),
        finishedReading(false),
        totalReadBuffers(0),
        xdbcEnv(&xdbcEnv) {
    spdlog::get("XDBC.SERVER")->info("Parquet Constructor called with table: {0}", tableName);

}


void PQReader::readData() {
    auto start_read = std::chrono::steady_clock::now();

    int threadWrittenTuples[xdbcEnv->deser_parallelism];
    int threadWrittenBuffers[xdbcEnv->deser_parallelism];
    std::thread readThreads[xdbcEnv->read_parallelism];
    std::thread deSerThreads[xdbcEnv->deser_parallelism];


    size_t numFiles = std::distance(std::filesystem::directory_iterator("/dev/shm/" + tableName),
                                    std::filesystem::directory_iterator{});


    spdlog::get("XDBC.SERVER")->info("Parquet files: {0}", numFiles);

    int partNum = xdbcEnv->read_parallelism;
    div_t partSizeDiv = div(numFiles, partNum);

    int partSize = partSizeDiv.quot;

    if (partSizeDiv.rem > 0)
        partSize++;

    for (int i = partNum - 1; i >= 0; i--) {
        Part p{};
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = numFiles;

        xdbcEnv->partPtr->push(p);

        spdlog::get("XDBC.SERVER")->info("Partition {0} [{1},{2}] pushed into queue",
                                         p.id, p.startOff, p.endOff);

    }

    //final partition
    Part fP{};
    fP.id = -1;

    xdbcEnv->activeReadThreads.resize(xdbcEnv->read_parallelism);
    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        xdbcEnv->partPtr->push(fP);
        readThreads[i] = std::thread(&PQReader::readPQ, this, i);
        xdbcEnv->activeReadThreads[i] = true;

    }


    auto start_deser = std::chrono::steady_clock::now();
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        threadWrittenTuples[i] = 0;
        threadWrittenBuffers[i] = 0;

        deSerThreads[i] = std::thread(&PQReader::deserializePQ,
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

    auto total_deser_time = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_deser).count();

    spdlog::get("XDBC.SERVER")->info("Read+Deser | Elapsed time: {0} ms for #tuples: {1}, #buffers: {2}",
                                     total_deser_time / 1000,
                                     totalTuples, totalBuffers);

}

int PQReader::deserializePQ(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "start"});

    // Pop a buffer from deserBufferPtr
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

    auto deserBuff = xdbcEnv->deserBufferPtr->pop();
    auto writeBuff = xdbcEnv->freeBufferPtr->pop();

    auto writeBuffPtr = bp[writeBuff].data() + sizeof(Header);

    size_t schemaSize = xdbcEnv->schema.size();
    int numRows = 0;
    size_t parquetFileSize;

    // Precompute column offsets, sizes, and deserializers
    std::vector<size_t> columnOffsets(schemaSize);
    std::vector<size_t> columnSizes(schemaSize);
    std::vector<std::function<void(parquet::StreamReader & , void * , int)>> deserializers(schemaSize);

    size_t rowSize = 0;
    for (size_t i = 0; i < schemaSize; ++i) {
        const auto &attr = xdbcEnv->schema[i];
        columnOffsets[i] = rowSize;
        if (attr.tpe[0] == 'I') {
            columnSizes[i] = 4;  // sizeof(int)
            deserializers[i] = deserialize<int>;
        } else if (attr.tpe[0] == 'D') {
            columnSizes[i] = 8;
            deserializers[i] = deserialize<double>;
        } else if (attr.tpe[0] == 'C') {
            columnSizes[i] = 1;  // sizeof(char)
            deserializers[i] = deserialize<char>;
        } else if (attr.tpe[0] == 'S') {
            columnSizes[i] = attr.size;
            deserializers[i] = deserialize<std::string>;
        } else {
            throw std::runtime_error("Unsupported column type: " + attr.tpe);
        }
        rowSize += columnSizes[i];
    }

    // Preallocate fixed-size buffers for string attributes
    std::vector<std::string> stringBuffers(schemaSize);
    for (int colIdx = 0; colIdx < schemaSize; ++colIdx) {
        const auto &attr = xdbcEnv->schema[colIdx];
        if (attr.tpe[0] == 'S') { // STRING or CHAR
            stringBuffers[colIdx].resize(attr.size, '\0'); // Fixed size with null padding
        }
    }

    while (true) {
        // Get buffer data from deserBuff
        const auto *bufferPtr = bp[deserBuff].data() + sizeof(size_t);

        // Reinterpret the start of the buffer as the Header
        std::memcpy(&parquetFileSize, bp[deserBuff].data(), sizeof(size_t));

        // Wrap the buffer with an Arrow BufferReader
        auto arrow_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(bufferPtr),
                                                            parquetFileSize);
        auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);

        // Initialize the StreamReader
        parquet::StreamReader stream{parquet::ParquetFileReader::Open(buffer_reader)};

        // Deserialize data using StreamReader
        while (!stream.eof()) {
            for (int colIdx = 0; colIdx < schemaSize; ++colIdx) {
                const auto &attr = xdbcEnv->schema[colIdx];

                void *dest = nullptr;

                if (xdbcEnv->iformat == 1) {
                    dest = writeBuffPtr + numRows * xdbcEnv->tuple_size + columnOffsets[colIdx];
                } else if (xdbcEnv->iformat == 2) {
                    dest = writeBuffPtr + columnOffsets[colIdx] * xdbcEnv->tuples_per_buffer +
                           numRows * attr.size;
                }

                //TODO: check if we can pass the preallocated strings to our deserializers
                if (attr.tpe[0] == 'S') {
                    auto &buffer = stringBuffers[colIdx];
                    stream >> buffer;
                    std::memcpy(dest, buffer.data(), attr.size);
                } else {
                    // Use deserializer for other types
                    deserializers[colIdx](stream, dest, attr.size);
                }
            }

            // End of row processing
            stream >> parquet::EndRow;
            ++numRows;
            ++totalThreadWrittenTuples;

            if (numRows == xdbcEnv->tuples_per_buffer) {
                // Write header and push buffer
                Header head{};
                head.totalTuples = numRows;
                std::memcpy(bp[writeBuff].data(), &head, sizeof(Header));

                xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

                xdbcEnv->compBufferPtr->push(writeBuff);

                xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

                writeBuff = xdbcEnv->freeBufferPtr->pop();

                writeBuffPtr = bp[writeBuff].data() + sizeof(Header);
                totalThreadWrittenBuffers++;
                numRows = 0;
            }
        }

        // Return the used buffer to freeBufferPtr
        xdbcEnv->freeBufferPtr->push(deserBuff);
        deserBuff = xdbcEnv->deserBufferPtr->pop();
        if (deserBuff == -1)
            break;
    }

    // Handle remaining rows
    if (numRows > 0) {
        Header head{};
        head.totalTuples = numRows;
        std::memcpy(bp[writeBuff].data(), &head, sizeof(Header));
        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

        xdbcEnv->compBufferPtr->push(writeBuff);

        ++totalThreadWrittenBuffers;
    }
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "end"});

    // Notify completion
    xdbcEnv->finishedDeserThreads.fetch_add(1);
    if (xdbcEnv->finishedDeserThreads == xdbcEnv->deser_parallelism) {
        for (int i = 0; i < xdbcEnv->compression_parallelism; ++i) {
            xdbcEnv->compBufferPtr->push(-1);
        }
    }

    return 0;
}


int PQReader::readPQ(int thr) {

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "start"});

    // Base directory containing the split Parquet files
    auto baseDir = "/dev/shm/" + tableName + "/";

    // Fetch the next partition to process
    Part curPart = xdbcEnv->partPtr->pop();
    while (curPart.id != -1) {
        // Iterate over the range of partitions
        for (int partitionId = curPart.startOff; partitionId < curPart.endOff; ++partitionId) {
            // Construct the file name for the current partition
            std::string fileName = baseDir + "lineitem_split_part" + std::to_string(partitionId) + ".parquet";

            // Open the Parquet file
            std::ifstream parquetFile(fileName, std::ios::binary | std::ios::in);
            if (!parquetFile.is_open()) {
                throw std::runtime_error("Failed to open Parquet file: " + fileName);
            }

            // Fetch a free buffer
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});
            auto writeBuff = xdbcEnv->freeBufferPtr->pop();

            auto writeBuffPtr = bp[writeBuff].data();

            // Read the entire file into the buffer
            parquetFile.seekg(0, std::ios::end);
            size_t fileSize = parquetFile.tellg();
            parquetFile.seekg(0, std::ios::beg);


            std::memcpy(writeBuffPtr, &fileSize, sizeof(size_t));

            // Ensure the buffer is large enough
            if (fileSize > xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size) {
                throw std::runtime_error("Parquet file is larger than the buffer size.");
            }

            parquetFile.read(reinterpret_cast<char *>(writeBuffPtr + sizeof(size_t)), fileSize);
            if (parquetFile.gcount() != fileSize) {
                throw std::runtime_error("Failed to read the entire Parquet file.");
            }

            //spdlog::get("XDBC.SERVER")->info("Reader thr {} writing buffer {} with size {}", thr, writeBuff, fileSize);

            // Push the buffer to the next stage
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});
            xdbcEnv->deserBufferPtr->push(writeBuff);
            // Close the file
            parquetFile.close();
        }

        // Fetch the next partition
        curPart = xdbcEnv->partPtr->pop();

    }
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "end"});

    xdbcEnv->finishedReadThreads.fetch_add(1);
    if (xdbcEnv->finishedReadThreads == xdbcEnv->read_parallelism) {
        for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
            xdbcEnv->deserBufferPtr->push(-1);
    }

    return 0;
}


int PQReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool PQReader::getFinishedReading() const {
    return finishedReading;
}
