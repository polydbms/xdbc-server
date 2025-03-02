#include <charconv>
#include "CSVReader.h"
#include "spdlog/spdlog.h"
#include "../csv.hpp"
#include "../../xdbcserver.h"
#include <queue>
#include "../deserializers.h"
#include "deserializers_arrow.h"
#include <arrow/builder.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <stdexcept>

size_t finalizeAndWriteRecordBatchToMemory(
    const std::vector<std::shared_ptr<arrow::ArrayBuilder>> &builders,
    const std::shared_ptr<arrow::Schema> &schema,
    void *memoryPtr,
    size_t availableBufferSize,
    size_t numRows)
{

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (const auto &builder : builders)
    {
        std::shared_ptr<arrow::Array> array;
        auto status = builder->Finish(&array);
        if (!status.ok())
        {
            throw std::runtime_error("Failed to finalize Arrow builder: " + status.message());
        }
        arrays.push_back(array);
    }

    // Create a RecordBatch
    auto recordBatch = arrow::RecordBatch::Make(schema, numRows, arrays);

    // Create a MutableBuffer
    auto buffer = std::make_shared<arrow::MutableBuffer>(
        static_cast<uint8_t *>(memoryPtr), availableBufferSize);
    auto bufferWriter = std::make_shared<arrow::io::FixedSizeBufferWriter>(buffer);

    // Serialize the RecordBatch using MakeFileWriter
    auto fileWriterResult = arrow::ipc::MakeFileWriter(bufferWriter, schema);
    if (!fileWriterResult.ok())
    {
        throw std::runtime_error("Failed to create FileWriter: " + fileWriterResult.status().message());
    }
    auto fileWriter = fileWriterResult.ValueOrDie();

    auto status = fileWriter->WriteRecordBatch(*recordBatch);
    if (!status.ok())
    {
        throw std::runtime_error("Failed to write RecordBatch: " + status.message());
    }

    status = fileWriter->Close();
    if (!status.ok())
    {
        throw std::runtime_error("Failed to close FileWriter: " + status.message());
    }

    // Calculate the serialized size and add padding for 8-byte alignment
    size_t serializedSize = bufferWriter->Tell().ValueOrDie();

    if (serializedSize > availableBufferSize)
    {
        throw std::runtime_error("Serialized data exceeds available buffer size");
    }

    return serializedSize;
}

void handle_error(const char *msg)
{
    perror(msg);
    exit(255);
}

static uintmax_t wc(char const *fname)
{
    static const auto BUFFER_SIZE = 16 * 1024;
    int fd = open(fname, O_RDONLY);
    if (fd == -1)
    {
        handle_error("open");
        spdlog::get("XDBC.SERVER")->error("File does not exist: {0}", fname);
    }

    posix_fadvise(fd, 0, 0, 1); // FDADVICE_SEQUENTIAL

    char buf[BUFFER_SIZE + 1];
    uintmax_t lines = 0;

    while (size_t bytes_read = read(fd, buf, BUFFER_SIZE))
    {
        if (bytes_read == (size_t)-1)
            handle_error("read failed");
        if (!bytes_read)
            break;

        for (char *p = buf; (p = (char *)memchr(p, '\n', (buf + bytes_read) - p)); ++p)
            ++lines;
    }

    return lines;
}

CSVReader::CSVReader(RuntimeEnv &xdbcEnv, const std::string &tableName) : DataSource(xdbcEnv, tableName),
                                                                          bp(*xdbcEnv.bpPtr),
                                                                          finishedReading(false),
                                                                          totalReadBuffers(0),
                                                                          xdbcEnv(&xdbcEnv)
{
    spdlog::get("XDBC.SERVER")->info("CSV Constructor called with table: {0}", tableName);
}

void CSVReader::readData()
{
    xdbcEnv->env_manager_DS.start();
    auto start_read = std::chrono::steady_clock::now();

    std::vector<int> threadWrittenTuples(xdbcEnv->max_threads, 0);  // Initialize all elements to 0
    std::vector<int> threadWrittenBuffers(xdbcEnv->max_threads, 0); // Initialize all elements to 0
    std::thread readThreads[xdbcEnv->read_parallelism];
    std::thread deSerThreads[xdbcEnv->deser_parallelism];

    auto fileName = "/dev/shm/" + tableName + ".csv";

    int maxRowNum = wc(fileName.c_str());
    spdlog::get("XDBC.SERVER")->info("CSV line number: {0}", maxRowNum);

    int partNum = xdbcEnv->read_parallelism;
    div_t partSizeDiv = div(maxRowNum, partNum);

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
            p.endOff = maxRowNum;

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
    readCSV(thr);
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
    deserializeCSV(thr, threadWrittenTuples[thr], threadWrittenBuffers[thr]);
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

int CSVReader::readCSV(int thr)
{

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "start"});
    // auto fileName = "/dev/shm/" + tableName + "_" + thrStrNum + ".csv";
    auto fileName = "/dev/shm/" + tableName + ".csv";

    std::ifstream file(fileName);
    if (!file.is_open())
    {
        spdlog::get("XDBC.SERVER")->error("CSV thread {0} error opening file", thr);
        return 0;
    }
    // spdlog::get("XDBC.SERVER")->info("CSV thread {0}: Entered to read file {1}", thr, fileName);

    int curBid = xdbcEnv->freeBufferPtr->pop();
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

    Part curPart = xdbcEnv->partPtr->pop();

    std::string line;
    int currentLine = 0;
    std::byte *writePtr = bp[curBid].data() + sizeof(Header);
    size_t sizeWritten = 0;
    size_t buffersRead = 0;
    size_t tuplesRead = 0;
    size_t tuplesWritten = 0;

    while (curPart.id != -1)
    {
        // skip to our starting offset
        while (currentLine < curPart.startOff && std::getline(file, line))
        {
            currentLine++;
        }

        while (currentLine < curPart.endOff && std::getline(file, line))
        {
            line += "\n";
            tuplesRead++;
            size_t lineSize = line.size();

            if ((writePtr - bp[curBid].data() + lineSize) > (bp[curBid].size() - sizeof(Header)))
            {

                // Buffer is full, send it and fetch a new buffer
                Header head{};
                head.totalSize = sizeWritten;
                head.totalTuples = tuplesWritten;
                std::memcpy(bp[curBid].data(), &head, sizeof(Header));
                sizeWritten = 0;
                tuplesWritten = 0;

                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});

                xdbcEnv->deserBufferPtr->push(curBid);

                curBid = xdbcEnv->freeBufferPtr->pop();
                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

                // spdlog::get("XDBC.SERVER")->info("CSV thread {0}: got buff {1} ", thr, curBid);

                writePtr = bp[curBid].data() + sizeof(Header);
                buffersRead++;
            }

            std::memcpy(writePtr, line.c_str(), lineSize);
            writePtr += lineSize;
            sizeWritten += lineSize;
            tuplesWritten++;

            ++currentLine;
        }
        currentLine = 0;

        curPart = xdbcEnv->partPtr->pop();
    }

    Header head{};
    head.totalSize = sizeWritten;
    head.totalTuples = tuplesWritten;
    // send the last buffer & notify the end
    std::memcpy(bp[curBid].data(), &head, sizeof(Header));

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});

    xdbcEnv->deserBufferPtr->push(curBid);

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "end"});

    xdbcEnv->finishedReadThreads.fetch_add(1);
    if (xdbcEnv->finishedReadThreads == xdbcEnv->read_parallelism)
    {
        xdbcEnv->enable_updation_DS = 0;
        xdbcEnv->enable_updation_xServe = 0;
        //     for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
        //         xdbcEnv->deserBufferPtr->push(-1);
    }

    file.close();
    spdlog::get("XDBC.SERVER")->info("Read thr {0} finished reading", thr);

    spdlog::get("XDBC.SERVER")->info("Read thread {0} finished. #tuples: {1}, #buffers {2}", thr, tuplesRead, buffersRead);
    return 1;
}

int CSVReader::deserializeCSV(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers)
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

        // TODO: add deserializer based on requirements
        std::vector<size_t> sizes(schemaSize);
        std::vector<size_t> schemaChars(schemaSize);
        using DeserializeFunc = void (*)(const char *src, const char *end, void *dest, int attSize, size_t len);
        std::vector<DeserializeFunc> deserializers(schemaSize);

        std::vector<std::function<void(const char *, const char *, std::shared_ptr<arrow::ArrayBuilder>, int,
                                       size_t)>>
            arrowDeserializers(schemaSize);

        std::vector<std::shared_ptr<arrow::ArrayBuilder>> arrowBuilders(schemaSize);
        std::vector<std::shared_ptr<arrow::Field>> fields;

        for (size_t i = 0; i < schemaSize; ++i)
        {
            std::shared_ptr<arrow::DataType> dataType;

            if (xdbcEnv->schema[i].tpe[0] == 'I')
            {
                sizes[i] = 4; // sizeof(int)
                schemaChars[i] = 'I';
                deserializers[i] = deserialize<int>;
                arrowDeserializers[i] = deserialize_arrow<int>;
                arrowBuilders[i] = std::make_shared<arrow::Int32Builder>();
                dataType = arrow::int32();
            }
            else if (xdbcEnv->schema[i].tpe[0] == 'D')
            {
                sizes[i] = 8; // sizeof(double)
                schemaChars[i] = 'D';
                deserializers[i] = deserialize<double>;
                arrowDeserializers[i] = deserialize_arrow<double>;
                arrowBuilders[i] = std::make_shared<arrow::DoubleBuilder>();
                dataType = arrow::float64();
            }
            else if (xdbcEnv->schema[i].tpe[0] == 'C')
            {
                sizes[i] = 1; // sizeof(char)
                schemaChars[i] = 'C';
                deserializers[i] = deserialize<char>;
                arrowDeserializers[i] = deserialize_arrow<char>;
                arrowBuilders[i] = std::make_shared<arrow::FixedSizeBinaryBuilder>(
                    arrow::fixed_size_binary(1));
                dataType = arrow::fixed_size_binary(1);
            }
            else if (xdbcEnv->schema[i].tpe[0] == 'S')
            {
                sizes[i] = xdbcEnv->schema[i].size;
                schemaChars[i] = 'S';
                deserializers[i] = deserialize<const char *>;
                arrowDeserializers[i] = deserialize_arrow<const char *>;
                arrowBuilders[i] = std::make_shared<arrow::FixedSizeBinaryBuilder>(
                    arrow::fixed_size_binary(sizes[i]));
                dataType = arrow::fixed_size_binary(sizes[i]);
            }
            fields.push_back(arrow::field(xdbcEnv->schema[i].name, dataType));
        }

        auto arrowSchema = std::make_shared<arrow::Schema>(fields);
        // spdlog::info("Arrow Schema: {}", arrowSchema->ToString());

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
                startWritePtr = bp[outBid].data() + sizeof(Header);

                bytesInTuple = 0;

                for (int attPos = 0; attPos < schemaSize; attPos++)
                {

                    // spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} processing schema", thr);

                    auto &attribute = xdbcEnv->schema[attPos];

                    endPtr = (attPos < schemaSize - 1) ? strchr(startReadPtr, ',') : strchr(startReadPtr, '\n');

                    len = endPtr - startReadPtr;

                    std::string_view tmp(startReadPtr, len);
                    const char *tmpPtr = tmp.data();
                    const char *tmpEnd = tmpPtr + len;
                    startReadPtr = endPtr + 1;

                    if (xdbcEnv->iformat == 1 || xdbcEnv->iformat == 2)
                    {
                        // Determine the write pointer based on row or column format
                        void *write = (xdbcEnv->iformat == 1)
                                          ? startWritePtr + bufferTupleId * xdbcEnv->tuple_size + bytesInTuple
                                          : startWritePtr + bytesInTuple * xdbcEnv->tuples_per_buffer +
                                                bufferTupleId * sizes[attPos];

                        // Use CSV deserializers
                        deserializers[attPos](tmpPtr, tmpEnd, write, attribute.size, len);
                    }
                    else if (xdbcEnv->iformat == 3)
                    {
                        // Format 3: Arrow
                        arrowDeserializers[attPos](tmpPtr, tmpEnd, arrowBuilders[attPos],
                                                   sizes[attPos], len);
                    }

                    bytesInTuple += attribute.size;
                    readOffset += len + 1;
                }

                bufferTupleId++;
                totalThreadWrittenTuples++;

                if (bufferTupleId == xdbcEnv->tuples_per_buffer && (xdbcEnv->iformat == 1 || xdbcEnv->iformat == 2))
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
                else if (bufferTupleId == xdbcEnv->tuples_per_buffer - 1000 && xdbcEnv->iformat == 3)
                {
                    size_t serializedSize = finalizeAndWriteRecordBatchToMemory(
                        arrowBuilders,                                // Pass the existing builders
                        arrowSchema,                                  // Pass the schema
                        startWritePtr,                                // Pointer to the memory region
                        xdbcEnv->buffer_size * 1024 - sizeof(Header), // Available buffer space after the Header
                        bufferTupleId                                 // Number of rows in the current batch
                    );

                    Header head{};
                    head.totalTuples = bufferTupleId;
                    head.totalSize = serializedSize;
                    head.intermediateFormat = xdbcEnv->iformat;
                    memcpy(bp[outBid].data(), &head, sizeof(Header));

                    bufferTupleId = 0;
                    totalThreadWrittenBuffers++;

                    xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

                    xdbcEnv->compBufferPtr->push(outBid);

                    for (auto &builder : arrowBuilders)
                    {
                        builder->Reset();
                    }
                    outBid = xdbcEnv->freeBufferPtr->pop();
                }
            }

            // we are done with reading the incoming buffer contents, return it and get a new one
            readOffset = 0;
            xdbcEnv->freeBufferPtr->push(inBid);
            inBid = xdbcEnv->deserBufferPtr->pop();
            if (inBid == -1)
                break;
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});
        }

        // remaining tuples
        if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->tuples_per_buffer &&
            (xdbcEnv->iformat == 1 || xdbcEnv->iformat == 2))
        {
            spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} has {1} remaining tuples", thr, xdbcEnv->tuples_per_buffer - bufferTupleId);

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
        else if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->tuples_per_buffer && xdbcEnv->iformat == 3)
        {
            size_t serializedSize = finalizeAndWriteRecordBatchToMemory(
                arrowBuilders,                                // Pass the existing builders
                arrowSchema,                                  // Pass the schema
                startWritePtr,                                // Pointer to the memory region
                xdbcEnv->buffer_size * 1024 - sizeof(Header), // Available buffer space after the Header
                bufferTupleId                                 // Number of rows in the current batch
            );

            Header head{};
            head.totalTuples = bufferTupleId;
            head.totalSize = serializedSize;
            head.intermediateFormat = xdbcEnv->iformat;
            memcpy(bp[outBid].data(), &head, sizeof(Header));

            totalThreadWrittenBuffers++;
            xdbcEnv->compBufferPtr->push(outBid);

            for (auto &builder : arrowBuilders)
            {
                builder->Reset();
            }
        }
    }

    /*else
        spdlog::get("XDBC.SERVER")->info("CSV thread {0} has no remaining tuples", thr);*/

    spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} finished. buffers: {1}, tuples {2}", thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);

    xdbcEnv->finishedDeserThreads.fetch_add(1);
    // if (xdbcEnv->finishedDeserThreads == xdbcEnv->deser_parallelism)
    // {
    //     for (int i = 0; i < xdbcEnv->compression_parallelism; i++)
    //         xdbcEnv->compBufferPtr->push(-1);
    // }

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "end"});

    return 1;
}

int CSVReader::getTotalReadBuffers() const
{
    return totalReadBuffers;
}

bool CSVReader::getFinishedReading() const
{
    return finishedReading;
}
