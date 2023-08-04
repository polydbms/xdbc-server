#ifndef XDBC_SERVER_CSVREADER_H
#define XDBC_SERVER_CSVREADER_H

#include <stack>
#include "../DataSource.h"


class CSVReader : public DataSource {
    typedef std::shared_ptr<queue<std::vector<std::string>>> Q_ptr;

public:
    CSVReader(RuntimeEnv &xdbcEnv, const std::string &tableName);

    int getTotalReadBuffers() const override;

    bool getFinishedReading() const override;

    void readData() override;

private:

    int csvWriteToBp(int thr);

    int writeTuplesToBp(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;
    std::string tableName;
    std::stack<struct Part> partStack;
    std::mutex partStackMutex;
    std::vector<Q_ptr> qs;

};


#endif //XDBC_SERVER_CSVREADER_H
