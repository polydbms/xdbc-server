//
// Created by harry on 10/12/22.
//
#include <clickhouse/client.h>
#include "ch.h"
#include <iostream>
#include <chrono>

using namespace clickhouse;
using namespace std;

int ch_row() {


    vector<tuple<int, int, int, int, double, double, double, double, string, string, string, string, string, string, string, string>> lineitems;
    lineitems.reserve(6001215);

    //start reading
    auto start = chrono::steady_clock::now();

    Client client(ClientOptions().SetHost("localhost").SetPort(19000));
    client.Select("SELECT * FROM click_sf1_lineitem", [&lineitems](const Block &block) {

                      for (size_t i = 0; i < block.GetRowCount(); ++i) {
                          tuple<int, int, int, int, float, float, float, float, string, string, string, string, string, string, string, string> lineitem;
                          lineitem = make_tuple(
                                  block[0]->As<ColumnInt64>()->At(i),
                                  block[1]->As<ColumnInt32>()->At(i),
                                  block[2]->As<ColumnInt32>()->At(i),
                                  block[3]->As<ColumnInt32>()->At(i),
                                  (int) block[4]->As<ColumnDecimal>()->At(i),
                                  (int) block[5]->As<ColumnDecimal>()->At(i),
                                  (int) block[6]->As<ColumnDecimal>()->At(i),
                                  (int) block[7]->As<ColumnDecimal>()->At(i),
                                  block[8]->As<ColumnString>()->At(i),
                                  block[9]->As<ColumnString>()->At(i),
                                  block[10]->As<ColumnDate>()->At(i),
                                  block[11]->As<ColumnDate>()->At(i),
                                  block[12]->As<ColumnDate>()->At(i),
                                  block[13]->As<ColumnString>()->At(i),
                                  block[14]->As<ColumnString>()->At(i),
                                  block[15]->As<ColumnString>()->At(i)
                          );
                          lineitems[i] = lineitem;

                          /* cout << block[0]->As<ColumnInt64>()->At(i) << " | "
                                << block[1]->As<ColumnInt32>()->At(i) << " | "
                                << block[2]->As<ColumnInt32>()->At(i) << " | "
                                << block[3]->As<ColumnInt32>()->At(i) << " | "
                                << (double) block[4]->As<ColumnDecimal>()->At(i) << " | "
                                << (double) block[5]->As<ColumnDecimal>()->At(i) << " | "
                                << (double) block[6]->As<ColumnDecimal>()->At(i) << " | "
                                << (double) block[7]->As<ColumnDecimal>()->At(i) << " | "
                                << block[8]->As<ColumnString>()->At(i) << " | "
                                << block[9]->As<ColumnString>()->At(i) << " | "
                                << block[10]->As<ColumnDate>()->At(i) << " | "
                                << block[11]->As<ColumnDate>()->At(i) << " | "
                                << block[12]->As<ColumnDate>()->At(i) << " | "
                                << block[13]->As<ColumnString>()->At(i) << " | "
                                << block[14]->As<ColumnString>()->At(i) << " | "
                                << block[15]->As<ColumnString>()->At(i) << " | "
                                << "\n";*/
                      }
                  }
    );
    auto end = chrono::steady_clock::now();
    cout << "Read | Elapsed time in milliseconds: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;

    //start writing
    start = chrono::steady_clock::now();
    Block block;

    auto att_l_orderkey = std::make_shared<ColumnInt64>();
    auto att_l_partkey = std::make_shared<ColumnInt32>();
    auto att_l_suppkey = std::make_shared<ColumnInt32>();
    auto att_l_linenumber = std::make_shared<ColumnInt32>();
    auto att_l_quantity = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_extendedprice = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_discount = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_tax = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_returnflag = std::make_shared<ColumnString>();
    auto att_l_linestatus = std::make_shared<ColumnString>();
    auto att_l_shipdate = std::make_shared<ColumnDate>();
    auto att_l_commitdate = std::make_shared<ColumnDate>();
    auto att_l_receiptdate = std::make_shared<ColumnDate>();
    auto att_l_shipinstruct = std::make_shared<ColumnString>();
    auto att_l_shipmode = std::make_shared<ColumnString>();
    auto att_l_comment = std::make_shared<ColumnString>();

    for (auto &lineitem: lineitems) {
        att_l_orderkey->Append(get<0>(lineitem));
        att_l_partkey->Append(get<1>(lineitem));
        att_l_suppkey->Append(get<2>(lineitem));
        att_l_linenumber->Append(get<3>(lineitem));
        att_l_quantity->Append((int) get<4>(lineitem));
        att_l_extendedprice->Append((int) get<5>(lineitem));
        att_l_discount->Append((int) get<6>(lineitem));
        att_l_tax->Append((int) get<7>(lineitem));
        att_l_returnflag->Append(get<8>(lineitem));
        att_l_linestatus->Append(get<9>(lineitem));
        att_l_shipdate->Append(time(nullptr));
        att_l_commitdate->Append(time(nullptr));
        att_l_receiptdate->Append(time(nullptr));
        att_l_shipinstruct->Append(get<13>(lineitem));
        att_l_shipmode->Append(get<14>(lineitem));
        att_l_comment->Append(get<15>(lineitem));

    }

    block.AppendColumn("l_orderkey", att_l_orderkey);
    block.AppendColumn("l_partkey", att_l_partkey);
    block.AppendColumn("l_suppkey", att_l_suppkey);
    block.AppendColumn("l_linenumber", att_l_linenumber);
    block.AppendColumn("l_quantity", att_l_quantity);
    block.AppendColumn("l_extendedprice", att_l_extendedprice);
    block.AppendColumn("l_discount", att_l_discount);
    block.AppendColumn("l_tax", att_l_tax);
    block.AppendColumn("l_returnflag", att_l_returnflag);
    block.AppendColumn("l_linestatus", att_l_linestatus);
    block.AppendColumn("l_shipdate", att_l_shipdate);
    block.AppendColumn("l_commitdate", att_l_commitdate);
    block.AppendColumn("l_receiptdate", att_l_receiptdate);
    block.AppendColumn("l_shipinstruct", att_l_shipinstruct);
    block.AppendColumn("l_shipmode", att_l_shipmode);
    block.AppendColumn("l_comment", att_l_comment);


    client.Execute("truncate tmp");
    client.Insert("tmp", block);

    end = chrono::steady_clock::now();
    cout << "Write | Elapsed time in milliseconds: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;

    return 0;
}

int ch_col() {

    vector<int> l_orderkey;
    l_orderkey.reserve(6001215);
    vector<int> l_partkey;
    l_partkey.reserve(6001215);
    vector<int> l_suppkey;
    l_suppkey.reserve(6001215);
    vector<int> l_linenumber;
    l_linenumber.reserve(6001215);
    vector<float> l_quantity;
    l_quantity.reserve(6001215);
    vector<float> l_extendedprice;
    l_extendedprice.reserve(6001215);
    vector<float> l_discount;
    l_discount.reserve(6001215);
    vector<float> l_tax;
    l_tax.reserve(6001215);
    vector<string> l_returnflag;
    l_returnflag.reserve(6001215);
    vector<string> l_linestatus;
    l_linestatus.reserve(6001215);
    vector<time_t> l_shipdate;
    l_shipdate.reserve(6001215);
    vector<time_t> l_commitdate;
    l_commitdate.reserve(6001215);
    vector<time_t> l_receiptdate;
    l_receiptdate.reserve(6001215);
    vector<string> l_shipinstruct;
    l_shipinstruct.reserve(6001215);
    vector<string> l_shipmode;
    l_shipmode.reserve(6001215);
    vector<string> l_comment;
    l_comment.reserve(6001215);

    //start reading
    auto start = chrono::steady_clock::now();

    Client client(ClientOptions().SetHost("localhost").SetPort(19000));
    client.Select("SELECT * FROM click_sf1_lineitem",
                  [&l_orderkey, &l_partkey, &l_suppkey, &l_linenumber, &l_quantity, &l_extendedprice, &l_discount, &l_tax, &l_returnflag, &l_linestatus, &l_shipdate, &l_commitdate, &l_receiptdate, &l_shipinstruct, &l_shipmode, &l_comment](
                          const Block &block) {

                      for (size_t i = 0; i < block.GetRowCount(); ++i) {

                          l_orderkey.push_back(block[0]->As<ColumnInt64>()->At(i));
                          l_partkey.push_back(block[1]->As<ColumnInt32>()->At(i));
                          l_suppkey.push_back(block[2]->As<ColumnInt32>()->At(i));
                          l_linenumber.push_back(block[3]->As<ColumnInt32>()->At(i));
                          l_quantity.push_back((int) block[4]->As<ColumnDecimal>()->At(i));
                          l_extendedprice.push_back((int) block[5]->As<ColumnDecimal>()->At(i));
                          l_discount.push_back((int) block[6]->As<ColumnDecimal>()->At(i));
                          l_tax.push_back((int) block[7]->As<ColumnDecimal>()->At(i));
                          l_returnflag.push_back((string) block[8]->As<ColumnString>()->At(i));
                          l_linestatus.push_back((string) block[9]->As<ColumnString>()->At(i));
                          l_shipdate.push_back(block[10]->As<ColumnDate>()->At(i));
                          l_commitdate.push_back(block[11]->As<ColumnDate>()->At(i));
                          l_receiptdate.push_back(block[12]->As<ColumnDate>()->At(i));
                          l_shipinstruct.push_back((string) block[13]->As<ColumnString>()->At(i));
                          l_shipmode.push_back((string) block[14]->As<ColumnString>()->At(i));
                          l_comment.push_back((string) block[15]->As<ColumnString>()->At(i));

                      }
                  }
    );

    auto end = chrono::steady_clock::now();
    cout << "Read | Elapsed time in milliseconds: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;

    //start writing
    start = chrono::steady_clock::now();
    Block block;

    auto att_l_orderkey = std::make_shared<ColumnInt64>();
    auto att_l_partkey = std::make_shared<ColumnInt32>();
    auto att_l_suppkey = std::make_shared<ColumnInt32>();
    auto att_l_linenumber = std::make_shared<ColumnInt32>();
    auto att_l_quantity = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_extendedprice = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_discount = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_tax = std::make_shared<ColumnDecimal>(12, 2);
    auto att_l_returnflag = std::make_shared<ColumnString>();
    auto att_l_linestatus = std::make_shared<ColumnString>();
    auto att_l_shipdate = std::make_shared<ColumnDate>();
    auto att_l_commitdate = std::make_shared<ColumnDate>();
    auto att_l_receiptdate = std::make_shared<ColumnDate>();
    auto att_l_shipinstruct = std::make_shared<ColumnString>();
    auto att_l_shipmode = std::make_shared<ColumnString>();
    auto att_l_comment = std::make_shared<ColumnString>();


    for (int i = 0; i < l_orderkey.size(); i++) {
        att_l_orderkey->Append(l_orderkey[i]);
        att_l_partkey->Append(l_partkey[i]);
        att_l_suppkey->Append(l_suppkey[i]);
        att_l_linenumber->Append(l_linenumber[i]);
        att_l_quantity->Append((int) l_quantity[i]);
        att_l_extendedprice->Append((int) l_extendedprice[i]);
        att_l_discount->Append((int) l_discount[i]);
        att_l_tax->Append((int) l_tax[i]);
        att_l_returnflag->Append(l_returnflag[i]);
        att_l_linestatus->Append(l_linestatus[i]);
        att_l_shipdate->Append(time(nullptr));
        att_l_commitdate->Append(time(nullptr));
        att_l_receiptdate->Append(time(nullptr));
        att_l_shipinstruct->Append(l_shipinstruct[i]);
        att_l_shipmode->Append(l_shipmode[i]);
        att_l_comment->Append(l_comment[i]);

    }

    block.AppendColumn("l_orderkey", att_l_orderkey);
    block.AppendColumn("l_partkey", att_l_partkey);
    block.AppendColumn("l_suppkey", att_l_suppkey);
    block.AppendColumn("l_linenumber", att_l_linenumber);
    block.AppendColumn("l_quantity", att_l_quantity);
    block.AppendColumn("l_extendedprice", att_l_extendedprice);
    block.AppendColumn("l_discount", att_l_discount);
    block.AppendColumn("l_tax", att_l_tax);
    block.AppendColumn("l_returnflag", att_l_returnflag);
    block.AppendColumn("l_linestatus", att_l_linestatus);
    block.AppendColumn("l_shipdate", att_l_shipdate);
    block.AppendColumn("l_commitdate", att_l_commitdate);
    block.AppendColumn("l_receiptdate", att_l_receiptdate);
    block.AppendColumn("l_shipinstruct", att_l_shipinstruct);
    block.AppendColumn("l_shipmode", att_l_shipmode);
    block.AppendColumn("l_comment", att_l_comment);


    client.Execute("truncate tmp");
    client.Insert("tmp", block);
    end = chrono::steady_clock::now();
    cout << "Write | Elapsed time in milliseconds: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;

    return 0;
}