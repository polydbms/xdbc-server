#include <chrono>
#include "xdbcserver.h"

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/crc.hpp>
#include <thread>
#include <atomic>

#include "Compression/Compressor.h"
#include "DataSources/PGReader/PGReader.h"
#include "DataSources/CHReader/CHReader.h"
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


XDBCServer::XDBCServer(const RuntimeEnv &env)
        : bp(),
          xdbcEnv(env),
          flagArr(env.bufferpool_size),
          totalSentBuffers(0),
          tableName() {

    for (auto &flag: flagArr) {
        flag = 1;
    }

    bp.resize(env.bufferpool_size, std::vector<std::byte>(env.buffer_size * env.tuple_size));

    xdbcEnv.flagArrPtr = &flagArr;
    xdbcEnv.bpPtr = &bp;

    spdlog::get("XDBC.SERVER")->info("Created XDBC Server with BPS: {0} buffers, BS: {1} bytes, TS: {2} bytes",
                                     bp.size(), env.buffer_size * env.tuple_size, env.tuple_size);

}

bool XDBCServer::hasUnsent(DataSource &dataReader, int minBid, int maxBid) {

    /*if (pgReader.finishedReading && totalSentBuffers == pgReader.totalReadBuffers)
        return false;*/
    if (dataReader.getFinishedReading()) {
        for (int i = minBid; i < maxBid; i++) {
            if (flagArr[i] == 0)
                return true;
        }
        return false;
    }

    return true;

}


int XDBCServer::send(int thr, DataSource &dataReader) {

    int port = 1234 + thr + 1;
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor listenerAcceptor(ioContext,
                                                    boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(),
                                                                                   port));
    boost::asio::ip::tcp::socket socket(ioContext);
    listenerAcceptor.accept(socket);

    spdlog::get("XDBC.SERVER")->info("Send thread {0} accepting on port: {1}", thr, port);
    //get client
    string readThreadId = read_(socket);
    readThreadId.erase(std::remove(readThreadId.begin(), readThreadId.end(), '\n'), readThreadId.cend());

    //decide partitioning
    int minBId = thr * (xdbcEnv.bufferpool_size / xdbcEnv.network_parallelism);
    int maxBId = (thr + 1) * (xdbcEnv.bufferpool_size / xdbcEnv.network_parallelism);

    //int minBId = 0;
    //int maxBId = xdbcEnv.bufferpool_size;

    spdlog::get("XDBC.SERVER")->info(
            "Send thread {0} paired with Client read thread {1} assigned buffers [{2},{3}]",
            thr, readThreadId, minBId, maxBId);

    auto start = std::chrono::steady_clock::now();

    int bufferId = minBId;
    size_t totalSentBytes = 0;
    int threadSentBuffers = 0;

    boost::asio::const_buffer tmpHeaderBuff;
    boost::asio::const_buffer tmpMsgBuff;
    std::vector<boost::asio::const_buffer> sendBuffer;

    int loops = 0;
    bool boostError = false;
    bool cont = true;
    while (hasUnsent(dataReader, minBId, maxBId) && !boostError && cont) {

        while (flagArr[bufferId] == 1) {
            bufferId++;
            if (bufferId == maxBId) {
                bufferId = minBId;

                loops++;
                /*if (loops == 1000000) {
                    loops = 0;
                    spdlog::get("XDBC.SERVER")->warn(
                            "Send thread {0} stuck in send at buffer: {1}, threadSentBuffs/totalSentBuffs: {2}/{3}, totalReadBuffs: {4} ",
                            thr, bufferId, threadSentBuffers, totalSentBuffers, dataReader.getTotalReadBuffers());

                    std::this_thread::sleep_for(xdbcEnv.sleep_time);
                }*/
                if (!hasUnsent(dataReader, minBId, maxBId)) {
                    spdlog::get("XDBC.SERVER")->warn("Send thread {0} exiting unexpectedly", thr);
                    cont = false;
                    break;
                }

            }
        }

        if (cont) {
            //TODO: replace function with a hashmap or similar
            //0 nocomp, 1 zstd, 2 snappy, 3 lzo, 4 lz4, 5 zlib, 6 cols
            size_t compId = Compressor::getCompId(xdbcEnv.compression_algorithm);

            //spdlog::get("XDBC.SERVER")->warn("Send thread {0} entering compression", thr);

            std::array<size_t, MAX_ATTRIBUTES> compressed_sizes = Compressor::compress_buffer(
                    xdbcEnv.compression_algorithm, bp[bufferId].data(),
                    xdbcEnv.buffer_size * xdbcEnv.tuple_size,
                    xdbcEnv.buffer_size, xdbcEnv.schema);

            size_t totalSize = 0;
            //TODO: check if schema larger than MAX_ATTRIBUTES

            if (xdbcEnv.compression_algorithm == "cols" &&
                compressed_sizes[0] == xdbcEnv.buffer_size * xdbcEnv.tuple_size)
                totalSize = compressed_sizes[0];
            else {
                for (int i = 0; i < xdbcEnv.schema.size(); i++) {
                    totalSize += compressed_sizes[i];
                }

            }

            //spdlog::get("XDBC.SERVER")->warn("Send thread {0} exited compression with total size {1}/{2}", thr,
            //                                             totalSize, xdbcEnv.buffer_size * xdbcEnv.tuple_size);

            if (totalSize > xdbcEnv.buffer_size * xdbcEnv.tuple_size) {
                spdlog::get("XDBC.SERVER")->warn("Send thread {0} compression more than buffer", thr);
                compId = 0;
            }
            if (totalSize == xdbcEnv.buffer_size * xdbcEnv.tuple_size) {
                compId = 0;
            }

            if (totalSize <= 0)
                spdlog::get("XDBC.SERVER")->error("Send thread {0} compression: {1}, totalSize: {2}",
                                                  thr, compId, totalSize);

/*            if (bufferId == 0)
                spdlog::get("XDBC.SERVER")->info("Send thread {0}, buffer: {1}, buffSize: {2}, ratio: {3}",
                                                 thr, bufferId, totalSize,
                                                 static_cast<double>(totalSize) /
                                                 (xdbcEnv.buffer_size * xdbcEnv.tuple_size));*/

            //TODO: create more sophisticated header with checksum etc

            Header head;
            head.compressionType = compId;
            head.totalSize = totalSize;
            head.intermediateFormat = static_cast<size_t>(xdbcEnv.iformat);
            //head.crc = compute_crc(bp[bufferId].data(), totalSize);
            head.attributeComp;


            std::copy(compressed_sizes.begin(), compressed_sizes.end(), head.attributeSize);
            //head.attributeSize = compressed_sizes;

            //std::array<size_t, 4> header{compId, compressed_size, compute_crc(bp[bufferId].data(), compressed_size),
            //                             static_cast<size_t>(xdbcEnv.iformat)};

            //tmpHeaderBuff = boost::asio::buffer(header);
            tmpHeaderBuff = boost::asio::buffer(&head, sizeof(Header));
            tmpMsgBuff = boost::asio::buffer(bp[bufferId], totalSize);
            sendBuffer = {tmpHeaderBuff, tmpMsgBuff};

            try {
                totalSentBytes += boost::asio::write(socket, sendBuffer);
                threadSentBuffers++;
                totalSentBuffers.fetch_add(1);
                flagArr[bufferId].store(1);
            } catch (const boost::system::system_error &e) {
                spdlog::get("XDBC.SERVER")->error("Error writing to socket:  {0} ", e.what());
                boostError = true;
                // Handle the error...
            }
        }
    }

    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("Send thread {0} finished. Elapsed time: {1} ms, bytes {2}, #buffers {3} ",
                                     thr, std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalSentBytes, threadSentBuffers);

    socket.close();

    return 1;
}


int XDBCServer::serve(int parallelism) {


    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor acceptor(ioContext,
                                            boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 1234));
    boost::asio::ip::tcp::socket baseSocket(ioContext);
    acceptor.accept(baseSocket);

    //read operation
    tableName = read_(baseSocket);

    tableName.erase(std::remove(tableName.begin(), tableName.end(), '\n'), tableName.cend());

    spdlog::get("XDBC.SERVER")->info("Client wants to read table {0} ", tableName);

    std::vector<thread> threads(parallelism);
    std::thread t1;
    std::unique_ptr<DataSource> ds;

    if (xdbcEnv.system == "postgres") {
        ds = std::make_unique<PGReader>(xdbcEnv, tableName);
    } else if (xdbcEnv.system == "clickhouse") {
        ds = std::make_unique<CHReader>(xdbcEnv, tableName);
    }

    t1 = std::thread([&ds]() {
        ds->readData();
    });

    spdlog::get("XDBC.SERVER")->info("Created {0} read threads", xdbcEnv.system);


    for (int i = 0; i < parallelism; i++) {
        threads[i] = std::thread(&XDBCServer::send, this, i, std::ref(*ds));
    }


    spdlog::get("XDBC.SERVER")->info("Created send threads: {0} ", parallelism);

    const std::string msg = "Server ready\n";
    boost::system::error_code error;
    size_t bs = boost::asio::write(baseSocket, boost::asio::buffer(msg), error);
    if (error) {
        spdlog::get("XDBC.SERVER")->warn("Boost error while writing: ", error.message());
    }

    //spdlog::get("XDBC.SERVER")->info("Basesocket signaled with bytes: {0} ", bs);


    // Join all the threads
    for (auto &thread: threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    t1.join();
    baseSocket.close();

    return 1;
}





