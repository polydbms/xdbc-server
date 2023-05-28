#include <chrono>
#include <iostream>
#include "xdbcserver.h"

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/crc.hpp>
#include <thread>
#include <atomic>

#include "Compression/Compressor.h"
#include "DataSources/PGReader/PGReader.h"
#include "spdlog/spdlog.h"


using namespace std;
using namespace boost::asio;
using ip::tcp;

size_t compute_crc(const boost::asio::const_buffer &buffer) {
    boost::crc_32_type crc;
    crc.process_bytes(boost::asio::buffer_cast<const void *>(buffer), boost::asio::buffer_size(buffer));
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

bool XDBCServer::hasUnsent(PGReader &pgReader, int minBid, int maxBid) {

    /*if (pgReader.finishedReading && totalSentBuffers == pgReader.totalReadBuffers)
        return false;*/
    if (pgReader.finishedReading) {
        for (int i = minBid; i < maxBid - 1; i++) {
            if (flagArr[i] == 0)
                return true;
        }
        return false;
    }

    return true;

}


int XDBCServer::send(int thr, PGReader &pgReader) {

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

    spdlog::get("XDBC.SERVER")->info(
            "Send thread {0} paired with Client read thread {1} assigned buffers [{2},{3}]",
            thr, readThreadId, minBId, maxBId);

    auto start = std::chrono::steady_clock::now();

    int bufferId = minBId;
    size_t totalSentBytes = 0;
    int threadSentBuffers = 0;
    boost::asio::mutable_buffer tmpBuff;
    boost::asio::const_buffer tmpHeaderBuff;
    boost::asio::const_buffer tmpMsgBuff;
    std::vector<boost::asio::const_buffer> sendBuffer;

    int loops = 0;
    bool boostError = false;
    bool cont = true;
    while (hasUnsent(pgReader, minBId, maxBId) && !boostError && cont) {

        while (flagArr[bufferId] == 1) {
            bufferId++;
            if (bufferId == maxBId) {
                bufferId = minBId;

                loops++;
                if (loops == 1000000) {
                    loops = 0;
                    spdlog::get("XDBC.SERVER")->warn(
                            "Send thread {0} stuck in send at buffer: {1}, sentBuffs: ({2}/{3}), totalReadBuffs: {4} ",
                            thr, bufferId, threadSentBuffers, totalSentBuffers, pgReader.totalReadBuffers);

                    std::this_thread::sleep_for(xdbcEnv.sleep_time);
                }
                if (!hasUnsent(pgReader, minBId, maxBId)) {
                    cont = false;
                    break;
                }

            }
        }
        //cout << "tuple cnt = " << totalCnt << endl;

        if (cont) {
            tmpBuff = boost::asio::buffer(bp[bufferId], xdbcEnv.buffer_size * xdbcEnv.tuple_size);

            if (xdbcEnv.compression_algorithm != "nocomp") {
                //std::vector<boost::asio::mutable_buffer> buffers;
                //TODO: check for errors in compression
                Compressor::compress_buffer(xdbcEnv.compression_algorithm, tmpBuff);
            }
            //cout << "compressed buffer:" << buffer.size() << " ratio "<<  buffer.size()/(BUFFER_SIZE*TUPLE_SIZE)<< endl;

            //TODO: replace function with a hashmap or similar
            //0 nocomp, 1 zstd, 2 snappy, 3 lzo, 4 lz4
            size_t compId = Compressor::getCompId(xdbcEnv.compression_algorithm);

            //TODO: create more sophisticated header with checksum etc
            //uint16_t checksum = compute_checksum(static_cast<const uint8_t *>(tmpBuff.data()), tmpBuff.size());

            std::array<size_t, 4> header{compId, tmpBuff.size(), compute_crc(tmpBuff),
                                         static_cast<size_t>(xdbcEnv.iformat)};
            tmpHeaderBuff = boost::asio::buffer(header);
            tmpMsgBuff = boost::asio::buffer(tmpBuff);
            sendBuffer = {tmpHeaderBuff, tmpMsgBuff};

            try {
                totalSentBytes += boost::asio::write(socket, sendBuffer);
                threadSentBuffers++;
                totalSentBuffers.fetch_add(1);
                flagArr[bufferId] = 1;
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

    PGReader pgReader("", xdbcEnv, tableName);

    std::thread t1(&PGReader::readData, &pgReader, tableName, 3);

    spdlog::get("XDBC.SERVER")->info("Created PG read threads");

    while (pgReader.totalReadBuffers == 0) {
        std::this_thread::sleep_for(xdbcEnv.sleep_time);
    }

    std::vector<thread> threads(parallelism);
    for (int i = 0; i < parallelism; i++) {
        threads[i] = std::thread(&XDBCServer::send, this, i, std::ref(pgReader));
    }

    spdlog::get("XDBC.SERVER")->info("Created send threads: {0} ", parallelism);

    const std::string msg = "Server ready\n";
    boost::system::error_code error;
    size_t bs = boost::asio::write(baseSocket, boost::asio::buffer(msg), error);
    if (error) {
        spdlog::get("XDBC.SERVER")->warn("Boost error while writing: ", error.message());
    }

    spdlog::get("XDBC.SERVER")->info("Basesocket signaled with bytes: {0} ", bs);


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





