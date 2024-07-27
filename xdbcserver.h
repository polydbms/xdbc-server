#ifndef XDBCSERVER_H
#define XDBCSERVER_H


#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <utility>
#include <atomic>

#include "DataSources/PGReader/PGReader.h"

using namespace boost::asio;
using ip::tcp;

constexpr size_t MAX_ATTRIBUTES = 230;
struct Header {

    size_t compressionType;
    size_t totalSize;
    size_t totalTuples;
    size_t intermediateFormat;
    size_t crc;
    size_t attributeSize[MAX_ATTRIBUTES];
    size_t attributeComp[MAX_ATTRIBUTES];

};

class XDBCServer {
public:
    explicit XDBCServer(RuntimeEnv &env);

    int serve();

    int send(int threadno, DataSource &dataReader);

private:
    RuntimeEnv *xdbcEnv;
    std::vector<std::vector<std::byte>> bp;
    std::atomic<int> totalSentBuffers;
    std::string tableName;
    std::thread _monitorThread;

    void monitorQueues(int interval_ms);
};


#endif //XDBCSERVER_H
