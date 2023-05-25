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
#define SLEEP_TIME 5ms


using namespace boost::asio;
using ip::tcp;


class XDBCServer {
public:
    explicit XDBCServer(const RuntimeEnv &env);

    int serve();

    int send(ip::tcp::socket &socket, PGReader &pgReader);

    bool hasUnsent(PGReader &pgReader);

private:
    RuntimeEnv xdbcEnv;
    std::vector<std::vector<std::byte>> bp;
    std::vector<std::atomic<int>> flagArr;
    std::atomic<int> totalSentBuffers;
    std::string tableName;

};


#endif //XDBCSERVER_H
