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


CHReader::CHReader(RuntimeEnv &xdbcEnv, const std::string tableName) :
        DataSource(xdbcEnv, tableName),
        flagArr(*xdbcEnv.flagArrPtr),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false),
        xdbcEnv(&xdbcEnv),
        tableName(tableName),
        schema() {

    // TODO: move schema creation somewhere else
    schema.emplace_back("l_orderkey", "INT", 4);
    schema.emplace_back("l_partkey", "INT", 4);
    schema.emplace_back("l_suppkey", "INT", 4);
    schema.emplace_back("l_linenumber", "INT", 4);
    schema.emplace_back("l_quantity", "DOUBLE", 8);
    schema.emplace_back("l_extendedprice", "DOUBLE", 8);
    schema.emplace_back("l_discount", "DOUBLE", 8);
    schema.emplace_back("l_tax", "DOUBLE", 8);

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

    int maxRowNum = getMaxRowNum(tableName);


    div_t div1 = div(maxRowNum, xdbcEnv->read_parallelism);
    int partSize = div1.quot;
    if (div1.rem > 0)
        partSize += 1;

    for (int i = 0; i < xdbcEnv->read_parallelism; i++) {

        int startOff = i * partSize;
        long endOff = ((i + 1) * partSize);

        if (i == xdbcEnv->read_parallelism - 1)
            endOff = maxRowNum + 1;

        threads[i] = std::thread(&CHReader::chWriteToBp,
                                 this, i, startOff, endOff,
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

    Client client(ClientOptions().SetHost("ch").SetPort(9000));
    client.Execute("DROP VIEW IF EXISTS tmp_view");
    client.Execute("CREATE VIEW tmp_view AS SELECT rowNumberInAllBlocks() as row_no,* FROM " + tableName +
                   " ORDER BY 2, 3, 4");
    int max = 0;

    string q = "SELECT CAST(max(rowNumberInAllBlocks()) AS Int32) AS maxrid FROM " + tableName;
    spdlog::get("XDBC.SERVER")->info("Got into getMaxNumRow: {0}, query: {1} ", max, q);
    client.Select(q, [&max](const Block &block) {
                      for (size_t i = 0; i < block.GetRowCount(); ++i) {
                          max = block[0]->As<ColumnInt32>()->At(i);

                      }
                  }
    );
    spdlog::get("XDBC.SERVER")->info("Return getMaxNumRow: {0} ", max);

    return max;
}

int CHReader::chWriteToBp(int thr, int from, long to, int &totalThreadWrittenTuples, int &totalThreadWrittenBuffers) {

    int minBId = thr * (xdbcEnv->bufferpool_size / xdbcEnv->read_parallelism);
    int maxBId = (thr + 1) * (xdbcEnv->bufferpool_size / xdbcEnv->read_parallelism);

    spdlog::get("XDBC.SERVER")->info("CH thread {0} assigned ({1},{2})", thr, minBId, maxBId);

    Client client(ClientOptions().SetHost("ch").SetPort(9000));

    int curBid = minBId;
    int bufferTupleId = 0;

    //TODO: fix dynamic schema
    //TODO: fix clickhouse partitioning
    std::string qStr =
            "SELECT " + getAttributesAsStr(schema) +
            //" FROM (SELECT rowNumberInAllBlocks() as row_no,* FROM " + tableName +
            //" ORDER BY l_orderkey, l_partkey, l_suppkey)" +
            " FROM tmp_view"
            " WHERE row_no >= " + to_string(from) + " AND row_no < " + to_string(to);

    spdlog::get("XDBC.SERVER")->info("CH thread {0} runs query: {1}", thr, qStr);
    /*std::string qStr = "SELECT rowNumberInAllBlocks() as row_no,* FROM " + tableName +
                       " WHERE row_no >= " + to_string(from) +
                       " AND row_no < " + to_string(to) + " ORDER BY l_orderkey";*/

    client.Select(qStr,
                  [this, &curBid, &totalThreadWrittenBuffers, &bufferTupleId, &totalThreadWrittenTuples, &maxBId, &minBId, &thr](
                          const Block &block) {

                      for (size_t i = 0; i < block.GetRowCount(); i++) {
                          int sleepCtr = 0;
                          while (flagArr[curBid] == 0) {
                              curBid++;
                              if (curBid == maxBId) {
                                  curBid = minBId;
                              }

                              if (sleepCtr == 1000) {
                                  sleepCtr = 0;
                                  spdlog::get("XDBC.SERVER")->warn(
                                          "CH thread {0}: Stuck at buffer {1} not ready to be written at tuple {2}. Total read buffers: {3}",
                                          thr, curBid, totalThreadWrittenBuffers, totalReadBuffers);

                              }
                              std::this_thread::sleep_for(xdbcEnv->sleep_time);
                              sleepCtr++;
                          }

                          int mv = 0;

                          if (xdbcEnv->iformat == 1)
                              mv = bufferTupleId * (xdbcEnv->tuple_size);
                          else if (xdbcEnv->iformat == 2)
                              mv = bufferTupleId * std::get<2>(schema[0]);

                          int ti = 0;
                          for (auto it = schema.begin(); it != std::prev(schema.end()); ++it) {
                              const auto &tuple = *it;
                              if (std::get<1>(tuple) == "INT") {
                                  memcpy(bp[curBid].data() + mv, &block[ti]->As<ColumnInt32>()->At(i), 4);

                                  if (xdbcEnv->iformat == 1)
                                      mv += 4;
                                  else if (xdbcEnv->iformat == 2)
                                      mv += xdbcEnv->buffer_size * 4;
                              }

                              if (std::get<1>(tuple) == "DOUBLE") {
                                  auto col = block[ti]->As<ColumnDecimal>();
                                  auto val = (int) col->At(i);
                                  memcpy(bp[curBid].data() + mv, &val, 8);

                                  if (xdbcEnv->iformat == 1)
                                      mv += 8;
                                  else if (xdbcEnv->iformat == 2)
                                      mv += xdbcEnv->buffer_size * 8;
                              }
                              ti++;
                          }

                          totalThreadWrittenTuples++;
                          bufferTupleId++;

                          if (bufferTupleId == xdbcEnv->buffer_size) {
                              //cout << "wrote buffer " << bufferId << endl;
                              bufferTupleId = 0;
                              flagArr[curBid] = 0;

                              totalReadBuffers.fetch_add(1);
                              totalThreadWrittenBuffers++;
                              curBid++;

                              if (curBid == maxBId)
                                  curBid = minBId;

                          }
                      }
                  }
    );

    //remaining tuples
    if (totalReadBuffers > 0 && bufferTupleId != xdbcEnv->buffer_size) {
        spdlog::get("XDBC.SERVER")->info("CH thread {0} has {1} remaining tuples",
                                         0, xdbcEnv->buffer_size - bufferTupleId);

        //TODO: remove dirty fix, potentially with buffer header or resizable buffers
        int mone = -1;
        double dmone = -1;
        int mv = 0;

        for (int i = bufferTupleId; i < xdbcEnv->buffer_size; i++) {

            if (xdbcEnv->iformat == 1)
                mv = bufferTupleId * (xdbcEnv->tuple_size);
            if (xdbcEnv->iformat == 2)
                mv = bufferTupleId * std::get<2>(schema[0]);

            for (const auto &tuple: schema) {

                if (std::get<1>(tuple) == "INT")
                    memcpy(bp[curBid].data() + mv, &mone, std::get<2>(tuple));

                if (std::get<1>(tuple) == "DOUBLE")
                    memcpy(bp[curBid].data() + mv, &dmone, std::get<2>(tuple));

                if (xdbcEnv->iformat == 1)
                    mv += std::get<2>(tuple);

                if (xdbcEnv->iformat == 2)
                    mv += xdbcEnv->buffer_size * std::get<2>(tuple);


            }
        }

        flagArr[curBid] = 0;
        totalReadBuffers.fetch_add(1);
        totalThreadWrittenBuffers++;
    }
    spdlog::get("XDBC.SERVER")->info("CH thread {0} wrote buffers: {1}, tuples {2}",
                                     thr, totalThreadWrittenBuffers, totalThreadWrittenTuples);
    return 1;
}
