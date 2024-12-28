#ifndef XDBC_SERVER_PQREADER_H
#define XDBC_SERVER_PQREADER_H


#include <stack>
#include "../DataSource.h"

class PQReader : public DataSource {

public:
    PQReader(RuntimeEnv &xdbcEnv, const std::string &tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData() override;

private:

    int readPQ(int thr);

    int deserializePQ(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;

};

#endif //XDBC_SERVER_PQREADER_H
