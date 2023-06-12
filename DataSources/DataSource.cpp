#include "DataSource.h"
#include <iomanip>

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

std::string DataSource::formatSchema(const std::vector<std::tuple<std::string, std::string, int>> &schema) {
    std::stringstream ss;

    // Header line
    ss << std::setw(20) << std::left << "Name"
       << std::setw(15) << std::left << "Type"
       << std::setw(10) << std::left << "Size"
       << '\n';

    for (const auto &tuple: schema) {
        ss << std::setw(20) << std::left << std::get<0>(tuple)
           << std::setw(15) << std::left << std::get<1>(tuple)
           << std::setw(10) << std::left << std::get<2>(tuple)
           << '\n';
    }

    return ss.str();
}

std::string DataSource::getAttributesAsStr(const std::vector<std::tuple<std::string, std::string, int>> &schema) {
    std::string result;
    for (const auto &tuple: schema) {
        result += std::get<0>(tuple) + ", ";
    }
    if (!result.empty()) {
        result.erase(result.size() - 2); // Remove the trailing comma and space
    }
    return result;
}
