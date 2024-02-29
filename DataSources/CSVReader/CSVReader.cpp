#include <charconv>
#include "CSVReader.h"
#include "../csv.hpp"
#include "spdlog/spdlog.h"
#include "../../xdbcserver.h"
#include <boost/iostreams/device/mapped_file.hpp>
#include <queue>

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
        xdbcEnv(&xdbcEnv),
        tableName(tableName) {

}

void CSVReader::readData() {
    auto start = std::chrono::steady_clock::now();


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

    int readQ = 0;
    for (int i = partNum - 1; i >= 0; i--) {
        Part p{};
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = maxRowNum;

        xdbcEnv->partPtr[readQ]->push(p);
        readQ++;
        if (readQ == xdbcEnv->read_parallelism)
            readQ = 0;
    }

    //final partition
    Part fP{};
    fP.id = -1;

    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        xdbcEnv->partPtr[i]->push(fP);
        readThreads[i] = std::thread(&CSVReader::readCSV, this, i);

    }


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

    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("Deser  | Elapsed time: {0} ms for #tuples: {1}, #buffers: {2}",
                                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalTuples, totalBuffers);
}

int CSVReader::readCSV(int thr) {

    //auto fileName = "/dev/shm/" + tableName + "_" + thrStrNum + ".csv";
    auto fileName = "/dev/shm/" + tableName + ".csv";

    std::ifstream file(fileName);
    if (!file.is_open()) {
        spdlog::get("XDBC.SERVER")->error("CSV thread {0} error opening file", thr);
        return 0;
    }
    //spdlog::get("XDBC.SERVER")->info("CSV thread {0}: Entered to read file {1}", thr, fileName);

    int curBid = xdbcEnv->readBufferPtr[thr]->pop();
    Part curPart = xdbcEnv->partPtr[thr]->pop();

    std::string line;
    int currentLine = 0;
    std::byte *writePtr = bp[curBid].data() + sizeof(size_t);
    int deserQ = 0;
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

            if ((writePtr - bp[curBid].data() + lineSize) > (bp[curBid].size() - sizeof(size_t))) {

                // Buffer is full, send it and fetch a new buffer
                std::memcpy(bp[curBid].data(), &sizeWritten, sizeof(size_t));
                sizeWritten = 0;
                xdbcEnv->deserBufferPtr[deserQ]->push(curBid);
                deserQ++;
                if (deserQ == xdbcEnv->deser_parallelism)
                    deserQ = 0;
                auto start_wait = std::chrono::high_resolution_clock::now();

                curBid = xdbcEnv->readBufferPtr[thr]->pop();

                auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now() - start_wait).count();
                xdbcEnv->read_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

                writePtr = bp[curBid].data() + sizeof(size_t);
                buffersRead++;
            }

            std::memcpy(writePtr, line.c_str(), lineSize);
            writePtr += lineSize;
            sizeWritten += lineSize;

            ++currentLine;
        }
        currentLine = 0;

        curPart = xdbcEnv->partPtr[thr]->pop();

    }

    //send the last buffer & notify the end
    std::memcpy(bp[curBid].data(), &sizeWritten, sizeof(size_t));
    xdbcEnv->deserBufferPtr[deserQ]->push(curBid);

    for (int i = 0; i < xdbcEnv->deser_parallelism; i++)
        xdbcEnv->deserBufferPtr[i]->push(-1);

    file.close();


    spdlog::get("XDBC.SERVER")->info("Read thread {0} #tuples: {1}, #buffers {2}", thr, tuplesRead, buffersRead);
    return 1;
}

int CSVReader::deserializeCSV(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    int emptyCtr = 0;
    std::queue<int> writeBuffers;
    std::queue<std::vector<std::byte>> tmpBuffers;
    int curWriteBuffer;
    size_t readOffset = 0;
    const char *endPtr;
    size_t len;
    int bufferTupleId = 0;
    int bytesInTuple = 0;
    int celli = -1;
    double celld = -1;
    int compQ = 0;
    std::byte *startWritePtr;
    const char *startReadPtr;
    void *write;

    while (emptyCtr < xdbcEnv->read_parallelism) {
        auto start_wait = std::chrono::high_resolution_clock::now();

        if ((tmpBuffers.empty() || writeBuffers.empty())) {
            int curBid = xdbcEnv->deserBufferPtr[thr]->pop();

            auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_wait).count();
            xdbcEnv->deser_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

            if (curBid == -1) {
                emptyCtr++;
                continue;
            }

            //allocate new tmp buffer, copy contents into it
            size_t bytesToRead = 0;
            std::memcpy(&bytesToRead, bp[curBid].data(), sizeof(size_t));
            std::vector<std::byte> tmpBuffer(bytesToRead);
            std::memcpy(tmpBuffer.data(), bp[curBid].data() + sizeof(size_t), bytesToRead);

            //push current tmp and write buffers into our respective queues
            tmpBuffers.push(tmpBuffer);
            writeBuffers.push(curBid);
        }


        //define current read buffer & write buffer
        curWriteBuffer = writeBuffers.front();
        const std::vector<std::byte> &curReadBufferRef = tmpBuffers.front();

        while (readOffset < curReadBufferRef.size()) {

            startReadPtr = reinterpret_cast<const char *>(curReadBufferRef.data() + readOffset);
            startWritePtr = bp[curWriteBuffer].data();

            bytesInTuple = 0;

            for (int attPos = 0; attPos < xdbcEnv->schema.size(); attPos++) {

                //spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} processing schema", thr);

                auto &attribute = xdbcEnv->schema[attPos];

                if (attPos < xdbcEnv->schema.size() - 1)
                    endPtr = strchr(startReadPtr, ',');
                else
                    endPtr = strchr(startReadPtr, '\n');

                len = endPtr - startReadPtr;

                std::string_view tmp(startReadPtr, len);
                const char *tmpPtr = tmp.data();
                const char *tmpEnd = tmpPtr + len;
                startReadPtr = endPtr + 1;

                if (xdbcEnv->iformat == 1) {
                    write = startWritePtr + bufferTupleId * xdbcEnv->tuple_size + bytesInTuple;
                } else if (xdbcEnv->iformat == 2) {
                    write = startWritePtr + bytesInTuple * xdbcEnv->buffer_size + bufferTupleId * attribute.size;
                }

                if (attribute.tpe == "INT") {
                    std::from_chars(tmpPtr, tmpEnd, celli);
                    memcpy(write, &celli, 4);
                } else if (attribute.tpe == "DOUBLE") {
                    std::from_chars(tmpPtr, tmpEnd, celld);
                    memcpy(write, &celld, 8);
                }

                //TODO: add more types
                bytesInTuple += attribute.size;
                readOffset += len + 1;

            }
            bufferTupleId++;
            totalThreadWrittenTuples++;

            if (bufferTupleId == xdbcEnv->buffer_size) {
                bufferTupleId = 0;

                totalThreadWrittenBuffers++;
                xdbcEnv->compBufferPtr[compQ]->push(curWriteBuffer);
                compQ++;
                if (compQ == xdbcEnv->compression_parallelism)
                    compQ = 0;

                writeBuffers.pop();
                break;

            }

        }
        if (readOffset >= curReadBufferRef.size()) {
            tmpBuffers.pop();
            readOffset = 0;
        }
    }

    int k = tmpBuffers.size();
    int bb = writeBuffers.size();


    //remaining tuples
    if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->buffer_size) {
        spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} has {1} remaining tuples",
                                         thr, xdbcEnv->buffer_size - bufferTupleId);

        //TODO: remove dirty fix, potentially with buffer header or resizable buffers
        int mone = -1;

        for (int i = bufferTupleId; i < xdbcEnv->buffer_size; i++) {

            void *writePtr;
            if (xdbcEnv->iformat == 1) {
                writePtr = startWritePtr + bufferTupleId * xdbcEnv->tuple_size;
            } else if (xdbcEnv->iformat == 2) {
                writePtr = startWritePtr + bufferTupleId * xdbcEnv->schema[0].size;
            }

            memcpy(writePtr, &mone, 4);
        }

        //spdlog::get("XDBC.SERVER")->info("thr {0} finished remaining", thr);
        xdbcEnv->compBufferPtr[compQ]->push(curWriteBuffer);
        totalThreadWrittenBuffers++;
    }



    /*else
        spdlog::get("XDBC.SERVER")->info("PG thread {0} has no remaining tuples", thr);*/

    /*spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} wrote buffers: {1}, tuples {2}",
                                     thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);*/

    for (int i = 0; i < xdbcEnv->compression_parallelism; i++)
        xdbcEnv->compBufferPtr[i]->push(-1);

    return 1;
}


int CSVReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool CSVReader::getFinishedReading() const {
    return finishedReading;
}
