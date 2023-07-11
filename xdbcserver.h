#ifndef XDBCSERVER_H
#define XDBCSERVER_H


#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <utility>
#include <atomic>

#include "DataSources/PGReader/PGReader.h"

//TODO: deprecate constants as they are declared now in xdbcEnv
//#define TOTAL_TUPLES 10000000
//#define BUFFER_SIZE 1000
//#define BUFFERPOOL_SIZE 1000
//#define TUPLE_SIZE 48
//#define PARALLELISM 4
//#define SLEEP_TIME 2ms


using namespace boost::asio;
using ip::tcp;

constexpr size_t MAX_ATTRIBUTES = 10;
struct Header {

    size_t compressionType;
    size_t totalSize;
    size_t intermediateFormat;
    size_t crc;
    size_t attributeSize[MAX_ATTRIBUTES];
    size_t attributeComp[MAX_ATTRIBUTES];

};

class XDBCServer {
public:
    explicit XDBCServer(const RuntimeEnv &env);

    int serve(int parallelism);

    int send(int threadno, DataSource &dataReader);

    bool hasUnsent(DataSource &dataReader, int minBid, int maxBid);

private:
    RuntimeEnv xdbcEnv;
    std::vector<std::vector<std::byte>> bp;
    std::vector<std::atomic<int>> flagArr;
    std::atomic<int> totalSentBuffers;
    std::string tableName;

};


#endif //XDBCSERVER_H
