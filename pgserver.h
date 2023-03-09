//
// Created by harry on 11/19/22.
//

#ifndef UNTITLED_PGSERVER_H
#define UNTITLED_PGSERVER_H


#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>

#define TOTAL_TUPLES 10000000
#define BUFFER_SIZE 1000
#define BUFFERPOOL_SIZE 1000
#define TUPLE_SIZE 48
#define SLEEP_TIME 5ms
#define PARALLELISM 4


//TODO: benchmark all baselines libpq, libpqxx, csv, binary etc
using namespace boost::asio;
using ip::tcp;

struct shortLineitem {
    int l_orderkey;
    int l_partkey;
    int l_suppkey;
    int l_linenumber;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    double l_tax;
};

class PGServer {
private:
    std::vector<std::array<shortLineitem, BUFFER_SIZE>> bp;
    std::array<std::atomic<int>, BUFFERPOOL_SIZE> flagArr;
    int totalRead;
    std::atomic<bool> finishedReading{};
    std::string tableName;
public:
    PGServer();

    //int read();

    int serve();

    void send(tcp::socket &socket);

    void readFromDB(int x);

    int pqWriteToBp(int thr, int from, long to, int &totalCnt);

    bool hasUnsent();

};


#endif //UNTITLED_PGSERVER_H
