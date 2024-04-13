#ifndef XDBC_SERVER_CSVREADER_H
#define XDBC_SERVER_CSVREADER_H

#include <stack>
#include "../DataSource.h"


class CSVReader : public DataSource {

public:
    CSVReader(RuntimeEnv &xdbcEnv, const std::string &tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData() override;

private:

    int readCSV(int thr);

    int deserializeCSV(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;

};


#endif //XDBC_SERVER_CSVREADER_H
