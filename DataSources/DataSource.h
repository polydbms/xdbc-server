#ifndef XDBC_SERVER_DATASOURCE_H
#define XDBC_SERVER_DATASOURCE_H

#include <string>
#include <atomic>
#include <vector>
#include <chrono>

//#define BUFFER_SIZE 1000
//#define BUFFERPOOL_SIZE 1000
//#define TUPLE_SIZE 48
//#define SLEEP_TIME 5ms

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

struct shortLineitemColBatch {
    std::vector<int> l_orderkey;
    std::vector<int> l_partkey;
    std::vector<int> l_suppkey;
    std::vector<int> l_linenumber;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
};
struct RuntimeEnv {
    std::string compression_algorithm;
    int iformat;
    int buffer_size;
    int bufferpool_size;
    int tuple_size;
    std::chrono::milliseconds sleep_time;
    int read_parallelism;
    int network_parallelism;
    std::vector<std::atomic<int>> *flagArrPtr;
    std::vector<std::vector<std::byte>> *bpPtr;
    std::string system;
    std::vector<std::tuple<std::string, std::string, int>> schema;
};

class DataSource {
public:
    DataSource(RuntimeEnv &xdbcEnv, std::string tableName);

    virtual ~DataSource() = default;

    virtual int getTotalReadBuffers() const = 0;

    virtual bool getFinishedReading() const = 0;

    virtual void readData() = 0;

    std::string slStr(shortLineitem *t);

    double double_swap(double d);

    std::string formatSchema(const std::vector<std::tuple<std::string, std::string, int>> &schema);

    std::string getAttributesAsStr(const std::vector<std::tuple<std::string, std::string, int>> &schema);

private:
    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    std::vector<std::atomic<int>> &flagArr;
    RuntimeEnv *xdbcEnv;
    std::string tableName;

};


#endif //XDBC_SERVER_DATASOURCE_H
