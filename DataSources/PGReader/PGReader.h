#ifndef PG_READER_H
#define PG_READER_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <chrono>
#include "../DataSource.h"

class PGReader : public DataSource {
public:

    PGReader(RuntimeEnv &xdbcEnv, const std::string &tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData() override;

private:

    int pqWriteToBp(int thr, int from, long to, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    int read_pqxx_stream();

    int read_pq_exec();

    int read_pq_copy();

    static int getMaxCtId(const std::string &tableName);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    std::vector<std::atomic<int>> &flagArr;
    RuntimeEnv *xdbcEnv;
    std::string tableName;
};

#endif // PG_READER_H
