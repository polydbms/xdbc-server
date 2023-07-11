#ifndef CH_READER_H
#define CH_READER_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <chrono>
#include "../DataSource.h"

class CHReader : public DataSource {
public:

    CHReader(RuntimeEnv &xdbcEnv, const std::string tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData();

private:
    int chWriteToBp(int thr, int from, long to, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    static int getMaxRowNum(const std::string &tableName);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    std::vector<std::atomic<int>> &flagArr;
    RuntimeEnv *xdbcEnv;
    std::string tableName;
};

#endif // CH_READER_H
