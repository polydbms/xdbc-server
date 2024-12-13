#include <charconv>
#include "CSVReader.h"
#include "spdlog/spdlog.h"
#include "../csv.hpp"
#include "../../xdbcserver.h"
#include <queue>
#include "../deserializers.h"


void handle_error(const char *msg) {
    perror(msg);
    exit(255);
}

static uintmax_t wc(char const *fname) {
    static const auto BUFFER_SIZE = 16 * 1024;
    int fd = open(fname, O_RDONLY);
    if (fd == -1) {
        handle_error("open");
        spdlog::get("XDBC.SERVER")->error("File does not exist: {0}", fname);
    }

    posix_fadvise(fd, 0, 0, 1);  // FDADVICE_SEQUENTIAL

    char buf[BUFFER_SIZE + 1];
    uintmax_t lines = 0;

    while (size_t bytes_read = read(fd, buf, BUFFER_SIZE)) {
        if (bytes_read == (size_t) -1)
            handle_error("read failed");
        if (!bytes_read)
            break;

        for (char *p = buf; (p = (char *) memchr(p, '\n', (buf + bytes_read) - p)); ++p)
            ++lines;
    }

    return lines;
}


CSVReader::CSVReader(RuntimeEnv &xdbcEnv, const std::string &tableName) :
        DataSource(xdbcEnv, tableName),
        bp(*xdbcEnv.bpPtr),
        finishedReading(false),
        totalReadBuffers(0),
        xdbcEnv(&xdbcEnv) {
    spdlog::get("XDBC.SERVER")->info("CSV Constructor called with table: {0}", tableName);
}

void CSVReader::readData() {
    auto start_read = std::chrono::steady_clock::now();

    int threadWrittenTuples[xdbcEnv->deser_parallelism];
    int threadWrittenBuffers[xdbcEnv->deser_parallelism];
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

    for (int i = partNum - 1; i >= 0; i--) {
        Part p{};
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = maxRowNum;

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
        readThreads[i] = std::thread(&CSVReader::readCSV, this, i);
        xdbcEnv->activeReadThreads[i] = true;

    }


    auto start_deser = std::chrono::steady_clock::now();
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        threadWrittenTuples[i] = 0;
        threadWrittenBuffers[i] = 0;

        deSerThreads[i] = std::thread(&CSVReader::deserializeCSV,
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

int CSVReader::readCSV(int thr) {

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "start"});
    //auto fileName = "/dev/shm/" + tableName + "_" + thrStrNum + ".csv";
    auto fileName = "/dev/shm/" + tableName + ".csv";

    std::ifstream file(fileName);
    if (!file.is_open()) {
        spdlog::get("XDBC.SERVER")->error("CSV thread {0} error opening file", thr);
        return 0;
    }
    //spdlog::get("XDBC.SERVER")->info("CSV thread {0}: Entered to read file {1}", thr, fileName);

    int curBid = xdbcEnv->freeBufferPtr->pop();
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

    Part curPart = xdbcEnv->partPtr->pop();

    std::string line;
    int currentLine = 0;
    std::byte *writePtr = bp[curBid].data() + sizeof(Header);
    size_t sizeWritten = 0;
    size_t buffersRead = 0;
    size_t tuplesRead = 0;

    while (curPart.id != -1) {
        //skip to our starting offset
        while (currentLine < curPart.startOff && std::getline(file, line)) {
            currentLine++;
        }

        while (currentLine < curPart.endOff && std::getline(file, line)) {
            line += "\n";
            tuplesRead++;
            size_t lineSize = line.size();

            if ((writePtr - bp[curBid].data() + lineSize) > (bp[curBid].size() - sizeof(Header))) {

                // Buffer is full, send it and fetch a new buffer
                Header head{};
                head.totalSize = sizeWritten;
                std::memcpy(bp[curBid].data(), &head, sizeof(Header));
                sizeWritten = 0;
                xdbcEnv->deserBufferPtr->push(curBid);

                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});


                curBid = xdbcEnv->freeBufferPtr->pop();
                xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "pop"});

                //spdlog::get("XDBC.SERVER")->info("CSV thread {0}: got buff {1} ", thr, curBid);

                writePtr = bp[curBid].data() + sizeof(Header);
                buffersRead++;
            }

            std::memcpy(writePtr, line.c_str(), lineSize);
            writePtr += lineSize;
            sizeWritten += lineSize;

            ++currentLine;
        }
        currentLine = 0;

        curPart = xdbcEnv->partPtr->pop();


    }

    Header head{};
    head.totalSize = sizeWritten;
    //send the last buffer & notify the end
    std::memcpy(bp[curBid].data(), &head, sizeof(Header));
    xdbcEnv->deserBufferPtr->push(curBid);

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "push"});
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "read", "end"});

    xdbcEnv->finishedReadThreads.fetch_add(1);
    if (xdbcEnv->finishedReadThreads == xdbcEnv->read_parallelism) {
        for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
            xdbcEnv->deserBufferPtr->push(-1);
    }

    file.close();
    spdlog::get("XDBC.SERVER")->info("Read thr {0} finished reading", thr);

    xdbcEnv->activeReadThreads[thr] = false;

    spdlog::get("XDBC.SERVER")->info("Read thread {0} finished. #tuples: {1}, #buffers {2}",
                                     thr, tuplesRead, buffersRead);
    return 1;
}

int CSVReader::deserializeCSV(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

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

    while (emptyCtr < 1 || !tmpBuffers.empty()) {

        if (emptyCtr < 1 && (tmpBuffers.empty() || writeBuffers.empty())) {

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

            //std::memcpy(&bytesToRead, bp[curBid].data(), sizeof(size_t));
            std::vector<std::byte> tmpBuffer(header->totalSize);
            std::memcpy(tmpBuffer.data(), bp[curBid].data() + sizeof(Header), header->totalSize);

            //push current tmp and write buffers into our respective queues
            tmpBuffers.push(tmpBuffer);
            writeBuffers.push(curBid);
        }


        //spdlog::get("XDBC.SERVER")->info("tmpBuffers {0}, writeBuffers {1}", bbbb, cccc);
        if (!tmpBuffers.empty() && writeBuffers.empty()) {

            int curBid = xdbcEnv->freeBufferPtr->pop();
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "pop"});

            writeBuffers.push(curBid);
        }

        //define current read buffer & write buffer
        curWriteBuffer = writeBuffers.front();
        const std::vector<std::byte> &curReadBufferRef = tmpBuffers.front();

        while (readOffset < curReadBufferRef.size()) {

            startReadPtr = reinterpret_cast<const char *>(curReadBufferRef.data() + readOffset);
            //+sizeof(Header) for temp header (totalTuples)
            startWritePtr = bp[curWriteBuffer].data() + sizeof(Header);

            bytesInTuple = 0;

            for (int attPos = 0; attPos < schemaSize; attPos++) {

                //spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} processing schema", thr);

                auto &attribute = xdbcEnv->schema[attPos];

                endPtr = (attPos < schemaSize - 1) ? strchr(startReadPtr, ',') : strchr(startReadPtr, '\n');

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
        spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} has {1} remaining tuples",
                                         thr, xdbcEnv->tuples_per_buffer - bufferTupleId);

        //write tuple count to tmp header
        Header head{};
        head.totalTuples = bufferTupleId;
        memcpy(bp[curWriteBuffer].data(), &head, sizeof(Header));

        xdbcEnv->compBufferPtr->push(curWriteBuffer);
        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "push"});

        totalThreadWrittenBuffers++;
    }


    /*else
        spdlog::get("XDBC.SERVER")->info("CSV thread {0} has no remaining tuples", thr);*/

    spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} finished. buffers: {1}, tuples {2}",
                                     thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);

    xdbcEnv->finishedDeserThreads.fetch_add(1);
    if (xdbcEnv->finishedDeserThreads == xdbcEnv->deser_parallelism) {
        for (int i = 0; i < xdbcEnv->compression_parallelism; i++)
            xdbcEnv->compBufferPtr->push(-1);
    }

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "deser", "end"});

    return 1;
}


int CSVReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool CSVReader::getFinishedReading() const {
    return finishedReading;
}
