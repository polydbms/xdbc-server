#ifndef XDBC_SERVER_DATASOURCE_H
#define XDBC_SERVER_DATASOURCE_H

#include <string>
#include <atomic>
#include <vector>
#include <chrono>
#include <map>
#include "../customQueue.h"

//#define BUFFER_SIZE 1000
//#define BUFFERPOOL_SIZE 1000
//#define TUPLE_SIZE 48
//#define SLEEP_TIME 5ms

struct Part {
    int id;
    int startOff;
    long endOff;
};

struct SchemaAttribute {
    std::string name;
    std::string tpe;
    int size;
};
struct ProfilingTimestamps {
    std::chrono::high_resolution_clock::time_point timestamp;
    int thread;
    std::string component;
    std::string event;
};
typedef std::shared_ptr<customQueue<int>> FBQ_ptr;
typedef std::shared_ptr<customQueue<Part>> FPQ_ptr;
typedef std::shared_ptr<customQueue<ProfilingTimestamps>> PTQ_ptr;


struct RuntimeEnv {
    long transfer_id;
    std::string compression_algorithm;
    int iformat;
    int buffer_size;
    int tuples_per_buffer;
    int buffers_in_bufferpool;
    int tuple_size;
    std::chrono::milliseconds sleep_time;
    int read_partitions;
    int read_parallelism;
    int deser_parallelism;
    int network_parallelism;
    int compression_parallelism;
    bool skip_deserializer;

    std::atomic<int> finishedReadThreads;
    std::atomic<int> finishedDeserThreads;
    std::atomic<int> finishedCompThreads;
    std::atomic<int> finishedSendThreads;

    FBQ_ptr freeBufferPtr;
    FBQ_ptr deserBufferPtr;
    FBQ_ptr compBufferPtr;
    FBQ_ptr sendBufferPtr;
    FPQ_ptr partPtr;

    std::vector<std::vector<std::byte>> *bpPtr;
    std::string system;
    std::vector<SchemaAttribute> schema;
    std::string schemaJSON;
    std::vector<FBQ_ptr> sendThreadReady;
    std::vector<std::tuple<long long, size_t, size_t, size_t, size_t>> queueSizes;
    std::atomic<bool> monitor;
    int profilingInterval;

    PTQ_ptr pts;

};

class DataSource {
public:
    DataSource(RuntimeEnv &xdbcEnv, std::string tableName);

    virtual ~DataSource() = default;

    virtual int getTotalReadBuffers() const = 0;

    virtual bool getFinishedReading() const = 0;

    virtual void readData() = 0;

    double double_swap(double d);

    std::string formatSchema(const std::vector<SchemaAttribute> &schema);

    std::string getAttributesAsStr(const std::vector<SchemaAttribute> &schema);

    static int getSchemaSize(const std::vector<SchemaAttribute> &schema);

private:
    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;

protected:
    std::string tableName;

};


#endif //XDBC_SERVER_DATASOURCE_H
