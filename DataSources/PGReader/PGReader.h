#ifndef PG_READER_H
#define PG_READER_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <chrono>
#include <stack>
#include <mutex>
#include "../DataSource.h"

class PGReader : public DataSource {
    typedef std::shared_ptr<customQueue<std::vector<std::string>>> Q_ptr;
public:

    PGReader(RuntimeEnv &xdbcEnv, const std::string &tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData() override;

private:

    int readPG(int thr);

    int deserializePG(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    int read_pqxx_stream();

    int read_pq_exec();

    int read_pq_copy();

    static int getMaxCtId(const std::string &tableName);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;

};

#endif // PG_READER_H
