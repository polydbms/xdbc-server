#include "PQReader.h"
#include "spdlog/spdlog.h"
#include "arrow/io/api.h"
#include <arrow/io/memory.h>
#include "parquet/stream_reader.h"
#include <parquet/metadata.h>
#include "../../xdbcserver.h"
#include "deserializers_parquet.h"
#include <fstream>
#include <filesystem>

PQReader::PQReader(RuntimeEnv &xdbcEnv, const std::string &tableName) : DataSource(xdbcEnv, tableName),
                                                                        bp(*xdbcEnv.bpPtr),
                                                                        finishedReading(false),
                                                                        totalReadBuffers(0),
                                                                        xdbcEnv(&xdbcEnv)
{
    spdlog::get("XDBC.SERVER")->info("Parquet Constructor called with table: {0}", tableName);
}

void PQReader::readData()
{
    xdbcEnv->env_manager_DS.start();
    auto start_read = std::chrono::steady_clock::now();

    std::vector<int> threadWrittenTuples(xdbcEnv->max_threads, 0);  // Initialize all elements to 0
    std::vector<int> threadWrittenBuffers(xdbcEnv->max_threads, 0); // Initialize all elements to 0
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

    for (int i = partNum - 1; i >= 0; i--)
    {
        Part p{};
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = numFiles;

        xdbcEnv->partPtr->push(p);

        spdlog::get("XDBC.SERVER")->info("Partition {0} [{1},{2}] pushed into queue", p.id, p.startOff, p.endOff);
    }

    // final partition
    Part fP{};
    fP.id = -1;

    //*** Create threads for read operation
    xdbcEnv->env_manager_DS.registerOperation("read", [&](int thr)
                                              { try {
    if (thr >= xdbcEnv->max_threads) {
    spdlog::get("XDBC.SERVER")->error("No of threads exceed limit");
    return;
    }
    xdbcEnv->partPtr->push(fP);
    readPQ(thr);
    } catch (const std::exception& e) {
    spdlog::get("XDBC.SERVER")->error("Exception in thread {}: {}", thr, e.what());
    } catch (...) {
    spdlog::get("XDBC.SERVER")->error("Unknown exception in thread {}", thr);
    } }, xdbcEnv->freeBufferPtr);
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
    deserializePQ(thr, threadWrittenTuples[thr], threadWrittenBuffers[thr]);
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
        xdbcEnv->env_manager_DS.configureThreads("deserialize", xdbcEnv->deser_parallelism);
    }

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

    auto total_deser_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() - start_deser)
                                .count();
    xdbcEnv->env_manager_DS.stop(); // *** Stop Reconfigurration handler
    spdlog::get("XDBC.SERVER")->info("Read+Deser | Elapsed time: {0} ms for #tuples: {1}, #buffers: {2}", total_deser_time / 1000, totalTuples, totalBuffers);
}

int PQReader::deserializePQ(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers)
{

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "start"});

    if (xdbcEnv->skip_deserializer)
    {
        while (true)
        {
            int inBid = xdbcEnv->deserBufferPtr->pop();
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

            if (inBid == -1)
                break;

            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});
            xdbcEnv->compBufferPtr->push(inBid);
            totalThreadWrittenBuffers++;
        }
    }
    else
    {
        // Pop a buffer from deserBufferPtr
        auto deserBuff = xdbcEnv->deserBufferPtr->pop();
        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

        auto writeBuff = xdbcEnv->freeBufferPtr->pop();

        auto writeBuffPtr = bp[writeBuff].data() + sizeof(Header);

        size_t schemaSize = xdbcEnv->schema.size();
        int numRows = 0;
        size_t parquetFileSize;

        // Precompute column offsets, sizes, and deserializers
        std::vector<size_t> columnOffsets(schemaSize);
        std::vector<size_t> columnSizes(schemaSize);
        std::vector<std::function<void(parquet::StreamReader &, void *, int)>> deserializers(schemaSize);

        size_t rowSize = 0;
        for (size_t i = 0; i < schemaSize; ++i)
        {
            const auto &attr = xdbcEnv->schema[i];
            columnOffsets[i] = rowSize;
            if (attr.tpe[0] == 'I')
            {
                columnSizes[i] = 4; // sizeof(int)
                deserializers[i] = deserialize<int>;
            }
            else if (attr.tpe[0] == 'D')
            {
                columnSizes[i] = 8;
                deserializers[i] = deserialize<double>;
            }
            else if (attr.tpe[0] == 'C')
            {
                columnSizes[i] = 1; // sizeof(char)
                deserializers[i] = deserialize<char>;
            }
            else if (attr.tpe[0] == 'S')
            {
                columnSizes[i] = attr.size;
                deserializers[i] = deserialize<std::string>;
            }
            else
            {
                throw std::runtime_error("Unsupported column type: " + attr.tpe);
            }
            rowSize += columnSizes[i];
        }

        // Preallocate fixed-size buffers for string attributes
        std::vector<std::string> stringBuffers(schemaSize);
        for (int colIdx = 0; colIdx < schemaSize; ++colIdx)
        {
            const auto &attr = xdbcEnv->schema[colIdx];
            if (attr.tpe[0] == 'S')
            {                                                  // STRING or CHAR
                stringBuffers[colIdx].resize(attr.size, '\0'); // Fixed size with null padding
            }
        }

        while (true)
        {
            // Get buffer data from deserBuff
            const auto *bufferPtr = bp[deserBuff].data() + sizeof(Header);

            // Reinterpret the start of the buffer as the Header
            auto header = reinterpret_cast<Header *>(bp[deserBuff].data());
            parquetFileSize = header->totalSize;

            // Wrap the buffer with an Arrow BufferReader
            auto arrow_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(bufferPtr),
                                                                parquetFileSize);
            auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);

            // Initialize the StreamReader
            parquet::StreamReader stream{parquet::ParquetFileReader::Open(buffer_reader)};

            // Deserialize data using StreamReader

            while (!stream.eof())
            {

                for (int colIdx = 0; colIdx < schemaSize; ++colIdx)
                {
                    const auto &attr = xdbcEnv->schema[colIdx];

                    void *dest = nullptr;

                    if (xdbcEnv->iformat == 1)
                    {
                        dest = writeBuffPtr + numRows * xdbcEnv->tuple_size + columnOffsets[colIdx];
                    }
                    else if (xdbcEnv->iformat == 2)
                    {
                        dest = writeBuffPtr + columnOffsets[colIdx] * xdbcEnv->tuples_per_buffer +
                               numRows * attr.size;
                    }

                    // TODO: check if we can pass the preallocated strings to our deserializers
                    if (attr.tpe[0] == 'S')
                    {
                        // std::string buffer;
                        auto &buffer = stringBuffers[colIdx];
                        stream >> buffer;
                        std::memset(dest, 0, attr.size);
                        std::memcpy(dest, buffer.data(), buffer.size());
                    }
                    else
                    {
                        // Use deserializer for other types
                        deserializers[colIdx](stream, dest, attr.size);
                    }
                }

                // End of row processing
                stream >> parquet::EndRow;
                ++numRows;
                ++totalThreadWrittenTuples;

                if (numRows == xdbcEnv->tuples_per_buffer)
                {
                    // Write header and push buffer
                    Header head{};
                    head.totalTuples = numRows;
                    head.totalSize = xdbcEnv->tuple_size * xdbcEnv->tuples_per_buffer;
                    head.intermediateFormat = xdbcEnv->iformat;
                    std::memcpy(bp[writeBuff].data(), &head, sizeof(Header));

                    /// test
                    /*const char *dataPtr = reinterpret_cast<const char *>(bp[writeBuff].data() + sizeof(Header));

                    spdlog::get("XDBC.SERVER")->info("First row values:");

                    size_t offset = 0;  // Offset within the row
                    for (const auto &att: xdbcEnv->schema) {
                        std::ostringstream oss;
                        oss << att.name << ": ";

                        if (att.tpe == "INT") {
                            int value = *reinterpret_cast<const int *>(dataPtr + offset);
                            oss << value;
                            offset += 4;
                        } else if (att.tpe == "DOUBLE") {
                            double value = *reinterpret_cast<const double *>(dataPtr + offset);
                            oss << value;
                            offset += 8;
                        } else if (att.tpe == "CHAR") {
                            char value = *reinterpret_cast<const char *>(dataPtr + offset);
                            oss << value;
                            offset += 1;
                        } else if (att.tpe == "STRING") {
                            std::string value(dataPtr + offset, dataPtr + offset + att.size);
                            oss << value;
                            offset += att.size;  // Use the size from the schema
                        } else {
                            oss << "Unknown type";
                        }

                        spdlog::get("XDBC.SERVER")->info(oss.str());
                    }*/
                    // test

                    xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

                    xdbcEnv->compBufferPtr->push(writeBuff);

                    writeBuff = xdbcEnv->freeBufferPtr->pop();
                    xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

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
        if (numRows > 0)
        {
            spdlog::get("XDBC.SERVER")->info("PQ Deser thread {0} has {1} remaining tuples", thr, numRows);
            Header head{};
            head.totalTuples = numRows;
            head.totalSize = head.totalTuples * xdbcEnv->tuple_size;
            head.intermediateFormat = xdbcEnv->iformat;
            if (xdbcEnv->iformat == 2)
                head.totalSize = xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size;

            std::memcpy(bp[writeBuff].data(), &head, sizeof(Header));
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

            xdbcEnv->compBufferPtr->push(writeBuff);

            ++totalThreadWrittenBuffers;
        }
    }
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "end"});

    // Notify completion
    xdbcEnv->finishedDeserThreads.fetch_add(1);

    return 0;
}

int PQReader::readPQ(int thr)
{

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "start"});

    // Base directory containing the split Parquet files
    auto baseDir = "/dev/shm/" + tableName + "/";

    // Fetch the next partition to process
    Part curPart = xdbcEnv->partPtr->pop();
    while (curPart.id != -1)
    {
        // Iterate over the range of partitions
        for (int partitionId = curPart.startOff; partitionId < curPart.endOff; ++partitionId)
        {
            // Construct the file name for the current partition
            std::string fileName = baseDir + tableName + "_part" + std::to_string(partitionId) + ".parquet";

            // Open the Parquet file
            std::ifstream parquetFile(fileName, std::ios::binary | std::ios::in);
            if (!parquetFile.is_open())
            {
                throw std::runtime_error("Failed to open Parquet file: " + fileName);
            }

            // Fetch a free buffer
            auto writeBuff = xdbcEnv->freeBufferPtr->pop();
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

            auto writeBuffPtr = bp[writeBuff].data();

            // Read the entire file into the buffer
            parquetFile.seekg(0, std::ios::end);
            size_t fileSize = parquetFile.tellg();
            parquetFile.seekg(0, std::ios::beg);

            Header head{};
            head.intermediateFormat = 4;
            head.totalSize = fileSize;
            head.totalTuples = 0;

            std::memcpy(writeBuffPtr, &head, sizeof(Header));

            // Ensure the buffer is large enough
            if (fileSize > xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size)
            {
                throw std::runtime_error("Parquet file is larger than the buffer size.");
            }

            parquetFile.read(reinterpret_cast<char *>(writeBuffPtr + sizeof(Header)), fileSize);
            if (parquetFile.gcount() != fileSize)
            {
                throw std::runtime_error("Failed to read the entire Parquet file.");
            }

            // spdlog::get("XDBC.SERVER")->info("Reader thr {} writing buffer {} with size {}", thr, writeBuff, fileSize);

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
    if (xdbcEnv->finishedReadThreads == xdbcEnv->read_parallelism)
    {
        xdbcEnv->enable_updation_DS = 0;
        xdbcEnv->enable_updation_xServe = 0;
    }

    return 0;
}

int PQReader::getTotalReadBuffers() const
{
    return totalReadBuffers;
}

bool PQReader::getFinishedReading() const
{
    return finishedReading;
}
