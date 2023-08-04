#ifndef CH_READER_H
#define CH_READER_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <chrono>
#include <stack>
#include <mutex>
#include "../DataSource.h"

class CHReader : public DataSource {
public:

    CHReader(RuntimeEnv &xdbcEnv, const std::string tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData();

private:
    int chWriteToBp(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    static int getMaxRowNum(const std::string &tableName);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;
    std::string tableName;
    std::stack<struct Part> partStack;
    std::mutex partStackMutex;
};

#endif // CH_READER_H
