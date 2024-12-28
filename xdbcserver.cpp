#include <chrono>
#include "xdbcserver.h"

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/crc.hpp>
#include <thread>
#include <numeric>

#include "Compression/Compressor.h"
#include "DataSources/PGReader/PGReader.h"
#include "DataSources/CHReader/CHReader.h"
#include "DataSources/CSVReader/CSVReader.h"
#include "DataSources/PQReader/PQReader.h"
#include "spdlog/spdlog.h"


using namespace std;
using namespace boost::asio;
using ip::tcp;

size_t compute_crc(const void *data, size_t size) {
    boost::crc_32_type crc;
    crc.process_bytes(data, size);
    return crc.checksum();
}

uint16_t compute_checksum(const uint8_t *data, std::size_t size) {
    uint16_t checksum = 0;
    for (std::size_t i = 0; i < size; ++i) {
        checksum ^= data[i];
    }
    return checksum;
}


string read_(tcp::socket &socket) {
    boost::asio::streambuf buf;
    try {
        size_t b = boost::asio::read_until(socket, buf, "\n");
        //spdlog::get("XDBC.SERVER")->info("Got bytes: {0} ", b);
    }
    catch (const boost::system::system_error &e) {
        spdlog::get("XDBC.SERVER")->warn("Boost error while reading: {0} ", e.what());
    }


    string data = boost::asio::buffer_cast<const char *>(buf.data());
    return data;
}


XDBCServer::XDBCServer(RuntimeEnv &xdbcEnv)
        : bp(),
          xdbcEnv(&xdbcEnv),
          totalSentBuffers(0),
          tableName() {

    PTQ_ptr pq(new customQueue<ProfilingTimestamps>);
    xdbcEnv.pts = pq;

    //initialize read thread status
    xdbcEnv.finishedReadThreads.store(0);

    //initialize free queue
    xdbcEnv.freeBufferPtr = std::make_shared<customQueue<int>>();

    //initially all buffers are put in the free buffer queue
    for (int i = 0; i < xdbcEnv.buffers_in_bufferpool; i++)
        xdbcEnv.freeBufferPtr->push(i);


    //initialize partitions queue
    xdbcEnv.partPtr = std::make_shared<customQueue<Part>>();

    //TODO: introduce checks for queue capacities, based on existing buffers

    //calculate buffers per queue
    int total_consumer_threads = xdbcEnv.deser_parallelism +
                                 xdbcEnv.compression_parallelism + xdbcEnv.network_parallelism;

    if (xdbcEnv.buffers_in_bufferpool < total_consumer_threads)
        spdlog::get("XDBC.SERVER")->error("not enough buffers in bufferpool");

    int queueCapacityPerComp = xdbcEnv.buffers_in_bufferpool / 3;
    int deserQueueCapacity = queueCapacityPerComp + xdbcEnv.buffers_in_bufferpool % 3;

    //initialize deser queue(s)
    xdbcEnv.deserBufferPtr = std::make_shared<customQueue<int>>();
    xdbcEnv.deserBufferPtr->setCapacity(deserQueueCapacity);
    xdbcEnv.finishedDeserThreads.store(0);

    //initialize compression queue
    xdbcEnv.compBufferPtr = std::make_shared<customQueue<int>>();
    xdbcEnv.compBufferPtr->setCapacity(queueCapacityPerComp);
    xdbcEnv.finishedCompThreads.store(0);

    //initialize send queue
    xdbcEnv.sendBufferPtr = std::make_shared<customQueue<int>>();
    xdbcEnv.sendBufferPtr->setCapacity(queueCapacityPerComp);
    xdbcEnv.finishedSendThreads.store(0);

    spdlog::get("XDBC.SERVER")->info("Initialized queues, "
                                     "freeBuffersQ: {0}, "
                                     "deserQ:{1}, "
                                     "compQ: {2}, "
                                     "sendQ: {2}",
                                     xdbcEnv.buffers_in_bufferpool, deserQueueCapacity, queueCapacityPerComp);


    //initialize send thread flags
    for (int i = 0; i < xdbcEnv.network_parallelism; i++) {
        FBQ_ptr q1(new customQueue<int>);
        xdbcEnv.sendThreadReady.push_back(q1);
    }

    xdbcEnv.bpPtr = &bp;

    spdlog::get("XDBC.SERVER")->info("Created XDBC Server with BPS: {0} KiB, buffers, BS: {1} KiB",
                                     xdbcEnv.buffer_size * xdbcEnv.buffers_in_bufferpool, xdbcEnv.buffer_size);

}

void XDBCServer::monitorQueues(int interval_ms) {

    long long curTimeInterval = interval_ms / 1000;

    while (xdbcEnv->monitor) {
        //auto now = std::chrono::high_resolution_clock::now();
        //auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        // Calculate the total size of all queues in each category
        size_t readBufferTotalSize = xdbcEnv->freeBufferPtr->size();

        size_t deserBufferTotalSize = xdbcEnv->deserBufferPtr->size();

        size_t compressedBufferTotalSize = xdbcEnv->compBufferPtr->size();
        size_t sendBufferTotalSize = xdbcEnv->sendBufferPtr->size();

        // Store the measurement as a tuple
        xdbcEnv->queueSizes.emplace_back(curTimeInterval, readBufferTotalSize, deserBufferTotalSize,
                                         compressedBufferTotalSize, sendBufferTotalSize);

        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        curTimeInterval += interval_ms / 1000;
    }
}

int XDBCServer::send(int thr, DataSource &dataReader) {

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "send", "start"});
    //spdlog::get("XDBC.SERVER")->info("Entered send thread: {0}", thr);
    int port = 1234 + thr + 1;
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor listenerAcceptor(ioContext,
                                                    boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(),
                                                                                   port));
    boost::asio::ip::tcp::socket socket(ioContext);

    //let main thread know socket is ready
    xdbcEnv->sendThreadReady[thr]->push(1);

    listenerAcceptor.accept(socket);

    spdlog::get("XDBC.SERVER")->info("Send thread {0} accepting on port: {1}", thr, port);
    //get client
    string readThreadId = read_(socket);
    readThreadId.erase(std::remove(readThreadId.begin(), readThreadId.end(), '\n'), readThreadId.cend());

    //decide partitioning
    /*int minBId = thr * (xdbcEnv->buffers_in_bufferpool / xdbcEnv->network_parallelism);
    int maxBId = (thr + 1) * (xdbcEnv->buffers_in_bufferpool / xdbcEnv->network_parallelism);*/

    //int minBId = 0;
    //int maxBId = xdbcEnv.bufferpool_size;

    spdlog::get("XDBC.SERVER")->info("Send thread {0} paired with Client rcv thread {1}", thr, readThreadId);

    int bufferId;
    size_t totalSentBytes = 0;
    int threadSentBuffers = 0;
    int sendToDeser = false;

    boost::asio::const_buffer sendBuffer;

    bool boostError = false;
    int emptyCtr = 0;

    while (emptyCtr < 1 && !boostError) {

        auto start_wait = std::chrono::high_resolution_clock::now();

        bufferId = xdbcEnv->sendBufferPtr->pop();
        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "send", "pop"});

        if (bufferId == -1)
            emptyCtr++;
        else {

            //spdlog::get("XDBC.SERVER")->warn("Send thread {0} exited compression with total size {1}/{2}", thr,
            //                                             totalSize, xdbcEnv.buffer_size * xdbcEnv.tuple_size);


/*            if (bufferId == 0)
                spdlog::get("XDBC.SERVER")->info("Send thread {0}, buffer: {1}, buffSize: {2}, ratio: {3}",
                                                 thr, bufferId, totalSize,
                                                 static_cast<double>(totalSize) /
                                                 (xdbcEnv.buffer_size * xdbcEnv.tuple_size));*/



            //std::array<size_t, 4> header{compId, compressed_size, compute_crc(bp[bufferId].data(), compressed_size),
            //                             static_cast<size_t>(xdbcEnv.iformat)};

            //tmpHeaderBuff = boost::asio::buffer(header);
            Header *headerPtr = reinterpret_cast<Header *>(bp[bufferId].data());
            /*spdlog::get("XDBC.SERVER")->warn("buffer {0} compression: {1}, totalSize: {2}", bufferId,
                                             headerPtr->compressionType, headerPtr->totalSize);*/
            sendBuffer = boost::asio::buffer(bp[bufferId], headerPtr->totalSize + sizeof(Header));

            try {

                totalSentBytes += boost::asio::write(socket, sendBuffer);
                threadSentBuffers++;


                totalSentBuffers.fetch_add(1);
                //spdlog::get("XDBC.SERVER")->info("total sent: {0}", totalSentBuffers);

                //reset & release buffer for reader
                //bp[bufferId].resize(xdbcEnv->buffer_size * xdbcEnv->tuple_size + sizeof(Header));


                xdbcEnv->freeBufferPtr->push(bufferId);


                xdbcEnv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "send", "push"});


            } catch (const boost::system::system_error &e) {
                spdlog::get("XDBC.SERVER")->error("Error writing to socket:  {0} ", e.what());
                boostError = true;
                // Handle the error...
            }
        }
    }
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "send", "end"});

    spdlog::get("XDBC.SERVER")->info("Send thread {0} finished. Bytes {1}, #buffers {2} ",
                                     thr, totalSentBytes, threadSentBuffers);

    boost::system::error_code ec;
    socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    if (ec) {
        spdlog::get("XDBC.SERVER")->error("Server send thread {0} shut down error: {1}", thr, ec.message());
    }

    socket.close(ec);
    if (ec) {
        spdlog::get("XDBC.SERVER")->error("Server send thread {0} close error: {1}", thr, ec.message());
    }

    return 1;
}


int XDBCServer::serve() {


    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor acceptor(ioContext,
                                            boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 1234));
    boost::asio::ip::tcp::socket baseSocket(ioContext);
    acceptor.accept(baseSocket);

    //read operation

    std::uint32_t dataSize = 0;
    size_t len = boost::asio::read(baseSocket, boost::asio::buffer(&dataSize, sizeof(dataSize)));
    std::vector<char> tableNameStr(dataSize);
    boost::asio::read(baseSocket, boost::asio::buffer(tableNameStr.data(), dataSize));
    tableName = std::string(tableNameStr.begin(), tableNameStr.end());
    //tableName = read_(baseSocket);

    //tableName.erase(std::remove(tableName.begin(), tableName.end(), '\n'), tableName.cend());

    spdlog::get("XDBC.SERVER")->info("Client wants to read table {0} ", tableName);

    dataSize = 0;
    len = boost::asio::read(baseSocket, boost::asio::buffer(&dataSize, sizeof(dataSize)));
    std::vector<char> schemaJSONstr(dataSize);
    len = boost::asio::read(baseSocket, boost::asio::buffer(schemaJSONstr.data(), dataSize));
    xdbcEnv->schemaJSON = std::string(schemaJSONstr.begin(), schemaJSONstr.end());

    //spdlog::get("XDBC.SERVER")->info("Got schema {0}", xdbcEnv->schemaJSON);


    std::vector<thread> net_threads(xdbcEnv->network_parallelism);
    std::vector<thread> comp_threads(xdbcEnv->compression_parallelism);
    std::thread t1;
    std::unique_ptr<DataSource> ds;

    if (xdbcEnv->system == "postgres") {
        ds = std::make_unique<PGReader>(*xdbcEnv, tableName);
    } else if (xdbcEnv->system == "clickhouse") {
        ds = std::make_unique<CHReader>(*xdbcEnv, tableName);
    } else if (xdbcEnv->system == "csv") {
        ds = std::make_unique<CSVReader>(*xdbcEnv, tableName);
    } else if (xdbcEnv->system == "parquet") {
        ds = std::make_unique<PQReader>(*xdbcEnv, tableName);
    }

    xdbcEnv->tuple_size = std::accumulate(xdbcEnv->schema.begin(), xdbcEnv->schema.end(), 0,
                                          [](int acc, const SchemaAttribute &attr) {
                                              return acc + attr.size;
                                          });
    xdbcEnv->tuples_per_buffer = (xdbcEnv->buffer_size * 1024 / xdbcEnv->tuple_size);

    bp.resize(xdbcEnv->buffers_in_bufferpool,
              std::vector<std::byte>(xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size + sizeof(Header)));
    spdlog::get("XDBC.SERVER")->info("Tuples per buffer: {0}", xdbcEnv->tuples_per_buffer);
    spdlog::get("XDBC.SERVER")->info("Input table tuple size: {0} with schema:\n{1}",
                                     xdbcEnv->tuple_size, ds->formatSchema(xdbcEnv->schema));

    xdbcEnv->monitor.store(true);

    _monitorThread = std::thread(&XDBCServer::monitorQueues, this, 1000);

    t1 = std::thread([&ds]() {
        ds->readData();
    });

    spdlog::get("XDBC.SERVER")->info("Created {0} read threads", xdbcEnv->system);

    std::unique_ptr<Compressor> compressorPtr;
    compressorPtr = std::make_unique<Compressor>(*xdbcEnv);

    for (int i = 0; i < xdbcEnv->compression_parallelism; i++) {
        comp_threads[i] = std::thread(&Compressor::compress, compressorPtr.get(), i, xdbcEnv->compression_algorithm);
    }

    spdlog::get("XDBC.SERVER")->info("Created compress threads: {0} ", xdbcEnv->compression_parallelism);

    for (int i = 0; i < xdbcEnv->network_parallelism; i++) {
        net_threads[i] = std::thread(&XDBCServer::send, this, i, std::ref(*ds));
    }
    //check that sockets are ready
    int acc = 0;
    int sendThreadReadyQ = 0;
    while (acc != xdbcEnv->network_parallelism) {
        acc += xdbcEnv->sendThreadReady[sendThreadReadyQ]->pop();
        spdlog::get("XDBC.SERVER")->info("Send threads ready: {0}/{1} ", acc, xdbcEnv->sendThreadReady.size());
        sendThreadReadyQ = (sendThreadReadyQ + 1) % xdbcEnv->network_parallelism;
    }

    spdlog::get("XDBC.SERVER")->info("Created send threads: {0} ", xdbcEnv->network_parallelism);


    const std::string msg = "Server ready\n";
    boost::system::error_code error;
    size_t bs = boost::asio::write(baseSocket, boost::asio::buffer(msg), error);
    if (error) {
        spdlog::get("XDBC.SERVER")->warn("Boost error while writing: ", error.message());
    }

    //spdlog::get("XDBC.SERVER")->info("Basesocket signaled with bytes: {0} ", bs);


    // Join all the threads
    for (auto &thread: comp_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (auto &thread: net_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    xdbcEnv->monitor.store(false);
    _monitorThread.join();

    t1.join();
    boost::system::error_code ec;
    baseSocket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    if (ec) {
        spdlog::get("XDBC.SERVER")->error("Base socket shut down error: {0}", ec.message());
    }

    baseSocket.close(ec);
    if (ec) {
        spdlog::get("XDBC.SERVER")->error("Base socket close error: {0}", ec.message());
    }

    return 1;
}






