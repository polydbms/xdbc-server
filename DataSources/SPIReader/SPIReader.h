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

    // Double-Buffering / Pipeline Architecture
    struct RawBatch {
        std::vector<char> data;
        int rowCount;
        
        RawBatch() {
            data.reserve(1024 * 1024); // Reserve 1MB initial capacity
            rowCount = 0;
        }
        
        void reset() {
            data.clear();
            rowCount = 0;
        }
    };

    void processBatches(); // Consumer thread function

    std::atomic<bool> finishedReading;
    std::atomic<int> totalReadBuffers;
    std::vector<std::vector<std::byte>> &bp;
    RuntimeEnv *xdbcEnv;
    
    // Queues for double buffering (borrowed from xdbc-server's customQueue)
    std::shared_ptr<customQueue<RawBatch*>> batchQueue;      // Filled batches
    std::shared_ptr<customQueue<RawBatch*>> freeBatchQueue;  // Empty batches (recycling)
    std::vector<RawBatch*> batchPool;                        // Ownership of batches
};

#endif // SPI_READER_H
