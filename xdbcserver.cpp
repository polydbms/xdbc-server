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

    //initialize read queues
    for (int i = 0; i < xdbcEnv.read_parallelism; i++) {
        FBQ_ptr q(new customQueue<int>);
        FPQ_ptr q2(new customQueue<Part>);


        //initially all buffers are free to write into
        for (int j = i * (xdbcEnv.buffers_in_bufferpool / xdbcEnv.read_parallelism);
             j < (i + 1) * (xdbcEnv.buffers_in_bufferpool / xdbcEnv.read_parallelism);
             j++)
            q->push(j);


        xdbcEnv.readBufferPtr.push_back(q);
        xdbcEnv.partPtr.push_back(q2);
        //initialize more buffers queue
        FBQ_ptr mq(new customQueue<int>);
        xdbcEnv.moreBuffersQ.push_back(mq);
    }



    //initialize deser queues
    for (int i = 0; i < xdbcEnv.deser_parallelism; i++) {
        FBQ_ptr q(new customQueue<int>);
        xdbcEnv.deserBufferPtr.push_back(q);
    }

    //initialize compression queues
    for (int i = 0; i < xdbcEnv.compression_parallelism; i++) {
        FBQ_ptr q(new customQueue<int>);
        xdbcEnv.compBufferPtr.push_back(q);
    }

    //initialize send queues
    for (int i = 0; i < xdbcEnv.network_parallelism; i++) {
        FBQ_ptr q(new customQueue<int>);
        xdbcEnv.sendBufferPtr.push_back(q);
        //initialize send thread flags
        FBQ_ptr q1(new customQueue<int>);
        xdbcEnv.sendThreadReady.push_back(q1);
    }


    xdbcEnv.bpPtr = &bp;

    spdlog::get("XDBC.SERVER")->info("Created XDBC Server with BPS: {0} buffers, BS: {1} bytes",
                                     bp.size(), xdbcEnv.buffer_size);

}

int XDBCServer::send(int thr, DataSource &dataReader) {

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
    int minBId = thr * (xdbcEnv->buffers_in_bufferpool / xdbcEnv->network_parallelism);
    int maxBId = (thr + 1) * (xdbcEnv->buffers_in_bufferpool / xdbcEnv->network_parallelism);

    //int minBId = 0;
    //int maxBId = xdbcEnv.bufferpool_size;

    spdlog::get("XDBC.SERVER")->info(
            "Send thread {0} paired with Client read thread {1} assigned buffers [{2},{3}]",
            thr, readThreadId, minBId, maxBId);

    int bufferId;
    size_t totalSentBytes = 0;
    int threadSentBuffers = 0;

    boost::asio::const_buffer sendBuffer;

    bool boostError = false;
    int emptyCtr = 0;
    int readQ = 0;


    while (emptyCtr < xdbcEnv->compression_parallelism && !boostError) {

        auto start_wait = std::chrono::high_resolution_clock::now();

        bufferId = xdbcEnv->sendBufferPtr[thr]->pop();

        auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_wait).count();
        xdbcEnv->network_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

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
                int checkedThreads = 0;

                // Cycle through the read queues in a round-robin fashion
                while (checkedThreads < xdbcEnv->read_parallelism) {
                    // Check if the current thread pointed to by readQ is active
                    if (xdbcEnv->activeReadThreads[readQ]) {
                        // If the thread is active, send the buffer to this thread's queue
                        xdbcEnv->readBufferPtr[readQ]->push(bufferId);
                        //spdlog::get("XDBC.SERVER")->info("Send thread {0} sending buff {1} to readQ {2}",
                        // thr, bufferId, readQ);

                        // Move to the next queue
                        readQ = (readQ + 1) % xdbcEnv->read_parallelism;
                        break; // Exit after successfully sending the buffer
                    } else {
                        // If the thread is not active, just move to the next queue
                        readQ = (readQ + 1) % xdbcEnv->read_parallelism;
                        checkedThreads++; // Increment the number of checked threads
                    }
                }
                // Optionally handle the case where all threads are inactive
                if (checkedThreads == xdbcEnv->read_parallelism) {
                    spdlog::get("XDBC.SERVER")->info("All threads are inactive. Buffer {0} by thread {1} not sent",
                                                     bufferId, thr);
                }


            } catch (const boost::system::system_error &e) {
                spdlog::get("XDBC.SERVER")->error("Error writing to socket:  {0} ", e.what());
                boostError = true;
                // Handle the error...
            }
        }
    }

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
    }

    xdbcEnv->tuple_size = std::accumulate(xdbcEnv->schema.begin(), xdbcEnv->schema.end(), 0,
                                          [](int acc, const SchemaAttribute &attr) {
                                              return acc + attr.size;
                                          });
    xdbcEnv->tuples_per_buffer = xdbcEnv->buffer_size * 1024 / xdbcEnv->tuple_size;

    bp.resize(xdbcEnv->buffers_in_bufferpool,
              std::vector<std::byte>(xdbcEnv->tuples_per_buffer * xdbcEnv->tuple_size + sizeof(Header)));
    spdlog::get("XDBC.SERVER")->info("Tuples per buffer: {0}", xdbcEnv->tuples_per_buffer);
    spdlog::get("XDBC.SERVER")->info("Input table tuple size: {0} with schema:\n{1}",
                                     xdbcEnv->tuple_size, ds->formatSchema(xdbcEnv->schema));

    t1 = std::thread([&ds]() {
        ds->readData();
    });

    spdlog::get("XDBC.SERVER")->info("Created {0} read threads", xdbcEnv->system);

    std::unique_ptr<Compressor> compressorPtr;
    compressorPtr = std::make_unique<Compressor>(*xdbcEnv);

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < xdbcEnv->compression_parallelism; i++) {
        comp_threads[i] = std::thread(&Compressor::compress, compressorPtr.get(), i, xdbcEnv->compression_algorithm);
    }

    auto start_net = std::chrono::high_resolution_clock::now();

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

    auto duration_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start).count();
    xdbcEnv->compression_time.fetch_add(duration_microseconds, std::memory_order_relaxed);

    for (auto &thread: net_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    auto duration_microseconds_network = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start_net).count();
    xdbcEnv->network_time.fetch_add(duration_microseconds_network, std::memory_order_relaxed);


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






