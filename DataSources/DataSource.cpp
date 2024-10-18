#include "DataSource.h"
#include <iomanip>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

SchemaAttribute createSchemaAttribute(std::string name, std::string tpe, int size) {
    SchemaAttribute att;
    att.name = std::move(name);
    att.tpe = std::move(tpe);
    att.size = size;
    return att;
}

std::vector<SchemaAttribute> createSchemaFromJsonString(const std::string &jsonString) {
    nlohmann::json schemaJson = nlohmann::json::parse(jsonString);

    std::vector<SchemaAttribute> schema;
    for (const auto &item: schemaJson) {
        schema.emplace_back(SchemaAttribute{
                item["name"],
                item["type"],
                item["size"]
        });
    }
    return schema;
}


DataSource::DataSource(RuntimeEnv &xdbcEnv, std::string tbl) :
        xdbcEnv(&xdbcEnv),
        tableName(std::move(tbl)),
        //flagArr(*xdbcEnv.flagArrPtr),
        bp(*xdbcEnv.bpPtr),
        totalReadBuffers(0),
        finishedReading(false) {
    spdlog::get("XDBC.SERVER")->info("Entered DataSource constructor for table {0}", tableName);
    //create schema
    xdbcEnv.schema = createSchemaFromJsonString(xdbcEnv.schemaJSON);

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

std::string DataSource::formatSchema(const std::vector<SchemaAttribute> &schema) {
    std::stringstream ss;

    // Header line
    ss << std::setw(20) << std::left << "Name"
       << std::setw(15) << std::left << "Type"
       << std::setw(10) << std::left << "Size"
       << '\n';

    for (const auto &tuple: schema) {
        ss << std::setw(20) << std::left << tuple.name
           << std::setw(15) << std::left << tuple.tpe
           << std::setw(10) << std::left << tuple.size
           << '\n';
    }

    return ss.str();
}

std::string DataSource::getAttributesAsStr(const std::vector<SchemaAttribute> &schema) {
    std::string result;
    for (const auto &tuple: schema) {
        result += tuple.name + ", ";
    }
    if (!result.empty()) {
        result.erase(result.size() - 2); // Remove the trailing comma and space
    }
    return result;
}

int DataSource::getSchemaSize(const std::vector<SchemaAttribute> &schema) {
    int size = 0;
    for (const auto &tuple: schema) {
        size += tuple.size;
    }
    return size;
}
