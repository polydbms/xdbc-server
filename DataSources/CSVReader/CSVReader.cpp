#include <charconv>
#include "CSVReader.h"
#include "../csv.hpp"
#include "spdlog/spdlog.h"
#include <boost/iostreams/device/mapped_file.hpp>

void handle_error(const char *msg) {
    perror(msg);
    exit(255);
}

static uintmax_t wc(char const *fname) {
    static const auto BUFFER_SIZE = 16 * 1024;
    int fd = open(fname, O_RDONLY);
    if (fd == -1)
        handle_error("open");

    /* Advise the kernel of our access pattern.  */
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
        tableName(tableName),
        partStack(),
        partStackMutex(),
        qs() {

}

void CSVReader::readData() {
    auto start = std::chrono::steady_clock::now();
    int totalCnt = 0;

    int threadWrittenTuples[xdbcEnv->deser_parallelism];
    int threadWrittenBuffers[xdbcEnv->deser_parallelism];
    std::thread readThreads[xdbcEnv->read_parallelism];
    std::thread deSerThreads[xdbcEnv->deser_parallelism];

    auto fileName = "/dev/shm/" + tableName + ".csv";

    int maxRowNum = wc(fileName.c_str());
    spdlog::get("XDBC.SERVER")->info("CSV line number: {0}", maxRowNum);

    int partNum = xdbcEnv->read_partitions;
    div_t partSizeDiv = div(maxRowNum, partNum);

    int partSize = partSizeDiv.quot;

    if (partSizeDiv.rem > 0)
        partSize++;

    for (int i = partNum - 1; i >= 0; i--) {
        Part p;
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = maxRowNum;

        partStack.push(p);

    }

    //initialize deser queues
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        Q_ptr q(new queue<std::vector<std::string>>);
        qs.push_back(q);
    }

    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        readThreads[i] = std::thread(&CSVReader::csvWriteToBp, this, i);
    }


    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        threadWrittenTuples[i] = 0;
        threadWrittenBuffers[i] = 0;

        deSerThreads[i] = std::thread(&CSVReader::writeTuplesToBp,
                                      this, i,
                                      std::ref(threadWrittenTuples[i]), std::ref(threadWrittenBuffers[i])
        );

    }

    int total = 0;
    for (int i = 0; i < xdbcEnv->deser_parallelism; i++) {
        deSerThreads[i].join();
        total += threadWrittenTuples[i];
    }

    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        readThreads[i].join();
    }
    finishedReading.store(true);
    totalCnt += total;

    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("Read  | Elapsed time: {0} ms for #tuples: {1}",
                                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalCnt);
}

int CSVReader::csvWriteToBp(int thr) {

    //auto fileName = "/dev/shm/" + tableName + "_" + thrStrNum + ".csv";
    auto fileName = "/dev/shm/" + tableName + ".csv";

    std::ifstream file(fileName);
    if (!file.is_open()) {
        spdlog::get("XDBC.SERVER")->error("CSV thread {0} error opening file", thr);
        return 0;
    }
    //spdlog::get("XDBC.SERVER")->info("CSV thread {0}: Entered to read file {1}", thr, fileName);

    int vectorSize = xdbcEnv->buffer_size * 100;
    std::vector<std::string> tuples(vectorSize);

    std::unique_lock<std::mutex> lock(partStackMutex);
    while (!partStack.empty()) {
        tuples.resize(vectorSize);

        Part part = partStack.top();
        partStack.pop();
        if (lock.owns_lock())
            lock.unlock();


        spdlog::get("XDBC.SERVER")->info("CSV thread {0} processing part {1}", thr, part.id);

        // Calculate the byte offset to the start of the desired line
        long lineSize = 0;
        long byteOffset = 0;
        file.seekg(0, std::ios::beg);

        std::string tempLine;
        for (size_t line = 0; line < part.startOff; ++line) {
            std::getline(file, tempLine);  // Read the line
            lineSize = tempLine.size() + 1; // Add 1 for the newline character
            byteOffset += lineSize;
        }

        // Move the file pointer to the calculated offset
        file.seekg(byteOffset, std::ios::beg);

        /*spdlog::get("XDBC.SERVER")->info(
                "CSV Thread {0} starting at {1}, ending at {2}, reading {3} tuples, offset {4}",
                thr, part.startOff, part.endOff, (part.endOff - part.startOff), byteOffset);*/

        int lTupleId = 0;
        int tupleId = 0;
        std::string lineData;
        int deserQ = 0;


        for (size_t line = 0; line < (part.endOff - part.startOff); ++line) {

            if (std::getline(file, lineData)) {
                tuples[lTupleId] = lineData;
                lTupleId++;
                tupleId++;
                if (lTupleId == vectorSize) {

                    qs[deserQ]->push(tuples);

                    deserQ++;
                    if (deserQ == xdbcEnv->deser_parallelism)
                        deserQ = 0;

                    lTupleId = 0;
                }

            } else {
                spdlog::get("XDBC.SERVER")->warn("CSV Thread {0} breaking at {1} for part {2}", thr, line, part.id);
                break;
            }
        }

        //handle last tuples
        if (lTupleId != 0) {
            tuples.resize(lTupleId);
            qs[deserQ]->push(tuples);
        }

        //spdlog::get("XDBC.SERVER")->info("CSV thread {0} exiting copy, tupleNo: {1}", thr, tupleId);

    }
    if (lock.owns_lock())
        lock.unlock();

    file.close();

    std::vector<std::string> v;
    for (const auto &q: qs) {
        q->push(v);
    }
    return 1;
}

int CSVReader::writeTuplesToBp(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    //spdlog::get("XDBC.SERVER")->info("CSV Deser thread {0} entered", thr);
    int emptyCtr = 0;
    int schemaSize = xdbcEnv->schema.size();
    int sendQueueId = 0;
    char *endPtr;
    size_t len;
    int celli = -1;
    double celld = -1;
    int bufferTupleId = 0;
    const char *tmpPtr;
    const char *tmpEnd;
    char *startPtr;

    while (emptyCtr < xdbcEnv->read_parallelism) {

        auto src = qs[thr]->pop();
        if (src.empty())
            emptyCtr++;
        else {

            bufferTupleId = 0;
            //int minBId = thr * (xdbcEnv->bufferpool_size / xdbcEnv->deser_parallelism);
            //int maxBId = (thr + 1) * (xdbcEnv->bufferpool_size / xdbcEnv->deser_parallelism);
            //int minBId = 0;
            //int maxBId = xdbcEnv->bufferpool_size;

            /*spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} assigned ({1},{2}), first tuple: {3}", thr, minBId,
                                             maxBId, src[0]);*/


            int curBid = xdbcEnv->writeBufferPtr[thr]->pop();
            auto bpPtr = bp[curBid].data();

            for (int i = 0; i < src.size(); i++) {
                //spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} processing {1}, tuple: {2}", thr, bufferTupleId, tuple);

                startPtr = src[i].data();

                int bytesInTuple = 0;

                for (int attPos = 0; attPos < schemaSize; attPos++) {

                    //spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} processing schema", thr);

                    auto &attribute = xdbcEnv->schema[attPos];

                    if (attPos < schemaSize - 1)
                        endPtr = strchr(startPtr, '|');
                    else
                        endPtr = strchr(startPtr, '\0');

                    len = endPtr - startPtr;
                    //char tmp[len + 1];
                    //memcpy(tmp, startPtr, len);
                    //tmp[len] = '\0';
                    std::string_view tmp(startPtr, len);
                    tmpPtr = tmp.data();
                    tmpEnd = tmpPtr + len;
                    startPtr = endPtr + 1;

                    void *writePtr;
                    if (xdbcEnv->iformat == 1) {
                        writePtr = bpPtr + bufferTupleId * xdbcEnv->tuple_size + bytesInTuple;
                    } else if (xdbcEnv->iformat == 2) {
                        writePtr = bpPtr + bytesInTuple * xdbcEnv->buffer_size +
                                   bufferTupleId * attribute.size;
                    }

                    if (attribute.tpe == "INT") {
                        std::from_chars(tmpPtr, tmpEnd, celli);
                        memcpy(writePtr, &celli, 4);
                    } else if (attribute.tpe == "DOUBLE") {
                        std::from_chars(tmpPtr, tmpEnd, celld);
                        memcpy(writePtr, &celld, 8);
                    }

                    //TODO: add more types
                    bytesInTuple += attribute.size;
                }
                //spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} processed schema", thr);

                totalThreadWrittenTuples++;
                bufferTupleId++;

                if (bufferTupleId == xdbcEnv->buffer_size) {

                    bufferTupleId = 0;
                    //flagArr[curBid].store(0);
                    //totalReadBuffers.fetch_add(1);
                    totalThreadWrittenBuffers++;
                    xdbcEnv->sendBufferPtr[sendQueueId]->push(curBid);
                    sendQueueId++;
                    if (sendQueueId == xdbcEnv->network_parallelism)
                        sendQueueId = 0;

                    curBid = xdbcEnv->writeBufferPtr[thr]->pop();
                    bpPtr = bp[curBid].data();
                }

            }

            //remaining tuples
            if (bufferTupleId > 0 && bufferTupleId != xdbcEnv->buffer_size) {
                spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} has {1} remaining tuples",
                                                 thr, xdbcEnv->buffer_size - bufferTupleId);

                //TODO: remove dirty fix, potentially with buffer header or resizable buffers
                int mone = -1;

                for (int i = bufferTupleId; i < xdbcEnv->buffer_size; i++) {

                    void *writePtr;
                    if (xdbcEnv->iformat == 1) {
                        writePtr = bpPtr + bufferTupleId * xdbcEnv->tuple_size;
                    } else if (xdbcEnv->iformat == 2) {
                        writePtr = bpPtr + bufferTupleId * xdbcEnv->schema[0].size;
                    }

                    memcpy(writePtr, &mone, 4);
                }

                //spdlog::get("XDBC.SERVER")->info("thr {0} finished remaining", thr);
                //totalReadBuffers.fetch_add(1);
                xdbcEnv->sendBufferPtr[sendQueueId]->push(curBid);
                totalThreadWrittenBuffers++;
            }

            /*else
                spdlog::get("XDBC.SERVER")->info("PG thread {0} has no remaining tuples", thr);*/

            /*spdlog::get("XDBC.SERVER")->info("PG Deser thread {0} wrote buffers: {1}, tuples {2}",
                                             thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);*/

        }
    }

    for (int i = 0; i < xdbcEnv->network_parallelism; i++)
        xdbcEnv->sendBufferPtr[i]->push(-1);

    return 1;
}

int CSVReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool CSVReader::getFinishedReading() const {
    return finishedReading;
}
