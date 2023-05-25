#ifndef PG_READER_H
#define PG_READER_H

#include <string>
#include <vector>
#include <array>
#include <atomic>

//#define BUFFER_SIZE 1000
//#define BUFFERPOOL_SIZE 1000
//#define TUPLE_SIZE 48
#define SLEEP_TIME 5ms

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
    int sleep_time;
    int parallelism;
    std::vector<std::atomic<int>> *flagArrPtr;
    std::vector<std::vector<std::byte>> *bpPtr;
};

class PGReader {
public:

    PGReader(std::string connectionString, RuntimeEnv &xdbcEnv, std::string tableName);

    void readData(const std::string &tableName, int method);

    int pqWriteToBp(int thr, int from, long to, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    int getMaxCtId(std::string tableName);

    std::string slStr(shortLineitem *t);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    std::vector<std::atomic<int>> &flagArr;
    RuntimeEnv *xdbcEnv;
    std::string tableName;


private:
    std::string _connectionString;

    void printSl(shortLineitem *t);


};

#endif // PG_READER_H
