#include "CHReader.h"
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include "spdlog/spdlog.h"
#include <clickhouse/client.h>

using namespace clickhouse;
using namespace std;
using namespace boost::asio;
using ip::tcp;

//TODO: refactor for new buffer_size -> tuples_per_buffer and deserialization method

CHReader::CHReader(RuntimeEnv &xdbcEnv, const std::string tableName) :
        DataSource(xdbcEnv, tableName),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false),
        xdbcEnv(&xdbcEnv),
        tableName(tableName) {

    spdlog::get("XDBC.SERVER")->info("CH Reader, table schema:\n{0}", formatSchema(xdbcEnv.schema));

}

int CHReader::getTotalReadBuffers() const {
    return totalReadBuffers;
}

bool CHReader::getFinishedReading() const {
    return finishedReading;
}

void CHReader::readData() {
    auto start = std::chrono::steady_clock::now();
    int totalCnt = 0;

    spdlog::get("XDBC.SERVER")->info("Using CH cpp lib, parallelism: {0}", xdbcEnv->read_parallelism);
    spdlog::get("XDBC.SERVER")->info("Using compression: {0}", xdbcEnv->compression_algorithm);

    int threadWrittenTuples[xdbcEnv->read_parallelism];
    int threadWrittenBuffers[xdbcEnv->read_parallelism];
    thread threads[xdbcEnv->read_parallelism];

    // TODO: throw something when table does not exist
    int maxRowNum = getMaxRowNum(tableName);


    int partNum = xdbcEnv->read_partitions;
    div_t partSizeDiv = div(maxRowNum, partNum);

    int partSize = partSizeDiv.quot;

    if (partSizeDiv.rem > 0)
        partSize++;


    for (int i = partNum - 1; i >= 0; i--) {
        Part p;
        p.id = i;
        p.startOff = i * partSize;
        p.endOff = ((i + 1) * partSize);

        if (i == partNum - 1)
            p.endOff = UINT32_MAX;

        partStack.push(p);

    }

    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {

        threads[i] = std::thread(&CHReader::chWriteToBp,
                                 this, i,
                                 std::ref(threadWrittenTuples[i]), std::ref(threadWrittenBuffers[i])
        );
        threadWrittenTuples[i] = 0;
        threadWrittenBuffers[i] = 0;

    }
    int total = 0;
    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {
        threads[i].join();

        total += threadWrittenTuples[i];
    }
    finishedReading.store(true);
    totalCnt += total;

    auto end = std::chrono::steady_clock::now();
    spdlog::get("XDBC.SERVER")->info("Read  | Elapsed time: {0} ms for #tuples: {1}",
                                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
                                     totalCnt);

    Client client(ClientOptions().SetHost("ch").SetPort(9000));
    client.Execute("DROP VIEW tmp_view");

    //return 0;
}

int CHReader::getMaxRowNum(const string &tableName) {

    // TODO: check connection properly
    Client client(ClientOptions().SetHost("ch").SetPort(9000));
    client.Execute("DROP VIEW IF EXISTS tmp_view");

    // TODO: check order
    client.Execute("CREATE VIEW tmp_view AS SELECT rowNumberInAllBlocks() as row_no,* FROM " + tableName +
                   " ORDER BY 2, 3, 4");
    int max = 0;
    string q = "SELECT CAST(max(rowNumberInAllBlocks()) AS Int32) AS maxrid FROM " + tableName;

    client.Select(q, [&max](const Block &block) {
                      for (size_t i = 0; i < block.GetRowCount(); ++i) {
                          max = block[0]->As<ColumnInt32>()->At(i);

                      }
                  }
    );

    spdlog::get("XDBC.SERVER")->info("CH getMaxNumRow: {0}, query: {1} ", max, q);

    return max;
}

int CHReader::chWriteToBp(int thr, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    int minBId = thr * (xdbcEnv->buffers_in_bufferpool / xdbcEnv->read_parallelism);
    int maxBId = (thr + 1) * (xdbcEnv->buffers_in_bufferpool / xdbcEnv->read_parallelism);

    spdlog::get("XDBC.SERVER")->info("CH thread {0} assigned ({1},{2})", thr, minBId, maxBId);

    Client client(ClientOptions().SetHost("ch").SetPort(9000));

    int curBid = xdbcEnv->deserBufferPtr->pop();
    int bufferTupleId = 0;

    while (true) {
        std::unique_lock<std::mutex> lock(partStackMutex);

        if (!partStack.empty()) {
            Part part = partStack.top();
            partStack.pop();

            lock.unlock();

            //TODO: fix dynamic schema
            //TODO: fix clickhouse partitioning
            std::string qStr =
                    "SELECT " + getAttributesAsStr(xdbcEnv->schema) +
                    //" FROM (SELECT rowNumberInAllBlocks() as row_no,* FROM " + tableName +
                    //" ORDER BY l_orderkey, l_partkey, l_suppkey)" +
                    " FROM tmp_view"
                    " WHERE row_no >= " + std::to_string(part.startOff) +
                    " AND row_no < " + std::to_string(part.endOff);

            spdlog::get("XDBC.SERVER")->info("CH thread {0} runs query: {1}", thr, qStr);
            /*std::string qStr = "SELECT rowNumberInAllBlocks() as row_no,* FROM " + tableName +
                               " WHERE row_no >= " + to_string(from) +
                               " AND row_no < " + to_string(to) + " ORDER BY l_orderkey";*/
            int schemaSize = xdbcEnv->schema.size();

            client.Select(qStr,
                          [this, &curBid, &totalThreadWrittenBuffers, &bufferTupleId, &totalThreadWrittenTuples, &thr, &schemaSize](
                                  const Block &block) {


                              for (size_t i = 0; i < block.GetRowCount(); i++) {
                                  auto bpPtr = bp[curBid].data();
                                  int ti = 0;
                                  int bytesInTuple = 0;

                                  for (int attPos = 0; attPos < schemaSize; attPos++) {
                                      auto &attribute = xdbcEnv->schema[attPos];

                                      void *writePtr;
                                      if (xdbcEnv->iformat == 1) {
                                          writePtr = bpPtr + bufferTupleId * xdbcEnv->tuple_size + bytesInTuple;
                                      } else if (xdbcEnv->iformat == 2) {
                                          writePtr = bpPtr + bytesInTuple * xdbcEnv->buffer_size +
                                                     bufferTupleId * attribute.size;
                                      }

                                      if (attribute.tpe == "INT") {
                                          memcpy(writePtr, &block[ti]->As<ColumnInt32>()->At(i), 4);

                                      } else if (attribute.tpe == "DOUBLE") {

                                          // TODO: fix decimal/double column
                                          auto col = block[ti]->As<ColumnDecimal>();
                                          auto val = (double) col->At(i) * 0.01;

                                          memcpy(writePtr, &val, 8);

                                      }
                                      ti++;
                                      bytesInTuple += attribute.size;
                                  }

                                  totalThreadWrittenTuples++;
                                  bufferTupleId++;

                                  if (bufferTupleId == xdbcEnv->buffer_size) {
                                      //cout << "wrote buffer " << bufferId << endl;
                                      bufferTupleId = 0;

                                      //totalReadBuffers.fetch_add(1);
                                      totalThreadWrittenBuffers++;
                                      xdbcEnv->compBufferPtr->push(curBid);

                                      curBid = xdbcEnv->deserBufferPtr->pop();

                                  }
                              }
                          }
            );

            //remaining tuples
            if (totalReadBuffers > 0 && bufferTupleId != xdbcEnv->buffer_size) {
                spdlog::get("XDBC.SERVER")->info("CH thread {0} has {1} remaining tuples",
                                                 thr, xdbcEnv->buffer_size - bufferTupleId);

                //TODO: remove dirty fix, potentially with buffer header or resizable buffers
                int mone = -1;

                for (int i = bufferTupleId; i < xdbcEnv->buffer_size; i++) {

                    void *writePtr;
                    if (xdbcEnv->iformat == 1) {
                        writePtr = bp[curBid].data() + bufferTupleId * xdbcEnv->tuple_size;
                    } else if (xdbcEnv->iformat == 2) {
                        writePtr = bp[curBid].data() + bufferTupleId * xdbcEnv->schema[0].size;
                    }

                    memcpy(writePtr, &mone, 4);
                }

                xdbcEnv->compBufferPtr->push(curBid);
                totalReadBuffers.fetch_add(1);
                totalThreadWrittenBuffers++;
            }
            spdlog::get("XDBC.SERVER")->info("CH thread {0} wrote buffers: {1}, tuples {2}",
                                             thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);
        } else {
            break;
        }

    }
    //notify that we finished
    xdbcEnv->compBufferPtr->push(-1);


    return 1;
}
