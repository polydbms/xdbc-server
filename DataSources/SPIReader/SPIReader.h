#ifndef SPI_READER_H
#define SPI_READER_H

#include <string>
#include <vector>
#include <atomic>
#include "../DataSource.h"

// Forward declaration to avoid including postgres headers here if possible,
// but SPI functions usually require them. For now, we keep it clean.

class SPIReader : public DataSource {
public:
    SPIReader(RuntimeEnv &xdbcEnv, const std::string &tableName);
    ~SPIReader() override = default;

    int getTotalReadBuffers() const override;
    bool getFinishedReading() const override;
    void readData() override;

private:
    int readSPI();
    int deserializeSPI(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers);

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;
};

#endif // SPI_READER_H
