#ifndef PG_INTERNAL_READER_H
#define PG_INTERNAL_READER_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <chrono>
#include <stack>
#include <mutex>
#include "../DataSource.h"

class PGInternalReader : public DataSource {
    typedef std::shared_ptr<customQueue<std::vector<std::string>>> Q_ptr;
public:

    PGInternalReader(RuntimeEnv &xdbcEnv, const std::string &tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData() override;

private:
    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;
private:
    int readDbData();
    int readPG(int thr);
    int deserializePG(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);
    int getMaxCtId(const std::string &tableName);
};

#endif // PG_READER_H
