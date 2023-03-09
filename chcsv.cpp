//
// Created by harry on 10/12/22.
//
#include <sstream>
#include <bits/stdc++.h>
#include <clickhouse/client.h>
#include "chcsv.h"
#include "csv.hpp"

int ch()
{
    using namespace csv;

    std::cout << "Hello, World!" << std::endl;

    CSVFormat format;
    format.delimiter('|')
            .quote('"')
            .no_header();  // Parse CSVs without a header row
    // .quote(false); // Turn off quoting

    CSVReader reader("/home/harry/datasets/tpch/supplier.tbl", format);

    using namespace std;
    vector<int> s_suppkey;
    vector<string> s_name;
    vector<string> s_address;
    vector<int> s_nationkey;
    vector<string> s_phone;
    vector<float> s_acctbal;
    vector<string> s_comment;


    for (CSVRow& row: reader) { // Input iterator
        int i = 0;
        for (CSVField& field: row) {

            // By default, get<>() produces a std::string.
            // A more efficient get<string_view>() is also available, where the resulting
            // string_view is valid as long as the parent CSVRow is alive

            switch (i){
                case 0: s_suppkey.push_back(stoi(field.get<>()));
                    break;
                case 1: s_name.push_back(field.get<>());
                    break;
                case 2: s_address.push_back(field.get<>());
                    break;
                case 3: s_nationkey.push_back(stoi(field.get<>()));
                    break;
                case 4: s_phone.push_back(field.get<>());
                    break;
                case 5: s_acctbal.push_back(stof(field.get<>()));
                    break;
                case 6: s_comment.push_back(field.get<>());
                    break;
            }

            i++;
        }

    }

    using namespace clickhouse;
    Client client(ClientOptions().SetHost("localhost").SetPort(19000));
    //client.Execute("CREATE TABLE IF NOT EXISTS test.numbers (id UInt64, name String) ENGINE = Memory");


    {
        Block block;

        auto att_s_suppkey = std::make_shared<ColumnInt32>();
        auto att_s_name = std::make_shared<ColumnString>();
        auto att_s_address = std::make_shared<ColumnString>();
        auto att_s_nationkey = std::make_shared<ColumnInt32>();
        auto att_s_phone = std::make_shared<ColumnString>();
        auto att_s_acctbal = std::make_shared<ColumnDecimal>(9,2);
        auto att_s_comment = std::make_shared<ColumnString>();


        for (int i = 0; i< s_name.size();i++)
        {
            att_s_suppkey->Append(s_suppkey[i]);
            att_s_name->Append(s_name[i]);
            att_s_address->Append(s_address[i]);
            att_s_nationkey->Append(s_nationkey[i]);
            att_s_phone->Append(s_phone[i]);
            att_s_acctbal->Append(21111);
            att_s_comment->Append(s_comment[i]);

        }

        block.AppendColumn("s_suppkey"  , att_s_suppkey);
        block.AppendColumn("s_name", att_s_name);
        block.AppendColumn("s_address", att_s_address);
        block.AppendColumn("s_nationkey", att_s_nationkey);
        block.AppendColumn("s_phone", att_s_phone);
        block.AppendColumn("s_acctbal", att_s_acctbal);
        block.AppendColumn("s_comment", att_s_comment);


        client.Insert("click_sf1_supplier", block);
    }


    client.Select("SELECT s_suppkey, s_name FROM click_sf1_supplier LIMIT 5", [] (const Block& block)
                  {
                      for (size_t i = 0; i < block.GetRowCount(); ++i) {
                          std::cout << block[0]->As<ColumnInt32>()->At(i) << " "
                                    << block[1]->As<ColumnString>()->At(i) << "\n";
                      }
                  }
    );

    return 0;
}