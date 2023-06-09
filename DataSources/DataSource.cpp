#include "DataSource.h"

DataSource::DataSource(RuntimeEnv &xdbcEnv, std::string tableName) :
        xdbcEnv(&xdbcEnv),
        tableName(std::move(tableName)),
        flagArr(*xdbcEnv.flagArrPtr),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false) {
}

std::string DataSource::slStr(shortLineitem *t) {

    return std::to_string(t->l_orderkey) + std::string(", ") +
           std::to_string(t->l_partkey) + std::string(", ") +
           std::to_string(t->l_suppkey) + std::string(", ") +
           std::to_string(t->l_linenumber) + std::string(", ") +
           std::to_string(t->l_quantity) + std::string(", ") +
           std::to_string(t->l_extendedprice) + std::string(", ") +
           std::to_string(t->l_discount) + std::string(", ") +
           std::to_string(t->l_tax);
}

double DataSource::double_swap(double d) {
    union {
        double d;
        unsigned char bytes[8];
    } src, dest;

    src.d = d;
    dest.bytes[0] = src.bytes[7];
    dest.bytes[1] = src.bytes[6];
    dest.bytes[2] = src.bytes[5];
    dest.bytes[3] = src.bytes[4];
    dest.bytes[4] = src.bytes[3];
    dest.bytes[5] = src.bytes[2];
    dest.bytes[6] = src.bytes[1];
    dest.bytes[7] = src.bytes[0];
    return dest.d;
}