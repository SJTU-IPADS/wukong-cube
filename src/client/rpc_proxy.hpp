/*
 * Copyright (c) 2021 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

#pragma once

#include <memory>
#include <sstream> 
#include <string>
#include <utility>
#include <vector>

#include "nlohmann/json.hpp"
#include "rpc/rpc_server.hpp"

#include "core/common/global.hpp"
#include "core/common/errors.hpp"
#include "core/common/status.hpp"
#include "utils/logger2.hpp"

#include "client/proxy.hpp"
namespace wukong {
    using json = nlohmann::json;

/**
 * @brief A kind of proxy that supports remote procedure call (RPC)
 *
 */
class RPCProxy: public Proxy {
public:
    explicit RPCProxy(int sid, int tid, std::string host_fname, StringMapping* str_server, DGraph* graph, Adaptor* adaptor, Stats* stats)
        : Proxy(sid, tid, str_server, graph, adaptor, stats) {
        // set hostname and port
        this->hostname = "localhost";
        this->port = Global::server_port_base + tid;

        logstream(LOG_INFO) << "Wukong proxy will listen on " << hostname << ":" << port << " for RPC" << LOG_endl;
    }

    ~RPCProxy() {
        delete srv;
    }

    void serve() {
        // initialize server
        srv = new RPCS(port);
        // register handlers
        srv->reg(RPC_CODE::INFO_RPC, this, &RPCProxy::retrieve_cluster_info);
        srv->reg(RPC_CODE::SPARQL_RPC, this, &RPCProxy::execute_sparql_task);
        // start server
        srv->start();
    }

protected:
    std::string hostname;
    uint32_t port;
    RPCS *srv;

    void reply2json(SPARQLQuery& reply, json& json_result) {
        static const char* type_name[3] = {"INT_t", "DOUBLE_t", "FLOAT_t"};

        SPARQLQuery::Result& result = reply.result;
        int display_rows = result.row_num < 100 ? result.row_num : 100;

        if (reply.q_type == SPARQLQuery::ASK) {
            json_result["Type"] = "ASK";
            json_result["Value"] = reply.result.row_num ? true : false;
            return;
        } else {
            logstream(LOG_INFO) << "(last) result row num: " << reply.result.row_num
                                << " , col num:" << reply.result.get_col_num() << LOG_endl;
            json_result["Type"] = "SELECT";
            // result2json(reply.result, json_res["Result"]);
        }

        // the size of the result(cols, rows)
        json_result["Size"]["Col"] = result.get_col_num() + result.get_attr_col_num();
        json_result["Size"]["Row"] = result.row_num;
        json_result["Data"]={};

        // the result data
        for (int i = 0; i < display_rows; i++) {
            json row;

            // result vid
            int num_time = 0;
            for (int j = 0; j < result.required_vars.size(); j++) {
                ssid_t var = result.required_vars[j];
                std::string col_name = result.required_vars_name[j];
                auto type = result.var_type(var);
#ifdef TRDF_MODE
                if (type == TIME_t) {
                    int64_t time = result.get_time_row_col(i, num_time);
                    json element = {{"type", "TIME_t"}};
                    element["value"] = time_tool::int2str(time);
                    row[col_name] = element;
                    num_time++;
                } else 
#endif
                {
                    int id = result.get_row_col(i, j - num_time);
                    auto map_result = str_server->id2str(tid, id);

                    json element = {{"type", "STRING_t"}};
                    if (map_result.first) element["value"] = map_result.second;
                    else element["value"] = "ID" + std::to_string(id);
                    row[col_name] = element;
                }
            }

            // result attr
            for (int c = 0; c < result.get_attr_col_num(); c++) {
                json element;
                attr_t attr = result.get_attr_row_col(i, c);
                switch (attr.which())
                {
                case 0:
                    element["type"] = "INT_t";
                    element["value"] = boost::get<int>(attr);
                    break;
                case 1:
                    element["type"] = "DOUBLE_t";
                    element["value"] = boost::get<double>(attr);
                    break;
                case 2:
                    element["type"] = "FLOAT_t";
                    element["value"] = boost::get<float>(attr);
                    break;
                default:
                    assert(false);
                }
                row["attr" + std::to_string(c)] = element;
            }

            // add row to json
            json_result["Data"].push_back(row);
        }
    }


    int run_single_query(std::istream& is, std::istream& fmt_stream,
                         SPARQLQuery& reply, std::map<std::string, std::string>& params) override {
        // get parameters
        int nopts, mt_factor;
        bool snd2gpu;

        nopts = std::stoi(params["nopts"]);
        mt_factor = std::stoi(params["mt_factor"]);
        snd2gpu = (params["snd2gpu"] == "true");

        SPARQLQuery request;

        // Parse the SPARQL query
        int ret = parser.parse(is, request);
        if (ret) {
            logstream(LOG_ERROR) << "Error occurs in query parsing!" << LOG_endl;
            return ret;
            ASSERT_ERROR_CODE(false, ret);
        }
        request.mt_factor = std::min(mt_factor, Global::mt_threshold);

        // Generate query plan if SPARQL optimizer is enabled.
        // FIXME: currently, the optimizater only works for standard SPARQL query.
        if (Global::enable_planner) {
            for (int i = 0; i < nopts; i++)
                planner.test_plan(request);

            // A shortcut for contradictory queries (e.g., empty result)
            if (planner.generate_plan(request) == false) {
                logstream(LOG_INFO) << "Query has no bindings, no need to execute it." << LOG_endl;
                return 0;  // success, skip execution
            }
        } else {
            ASSERT(fmt_stream.good());
            planner.set_plan(request.pattern_group, fmt_stream);
        }

        // GPU-accelerate or not
        if (snd2gpu) {
            request.dev_type = SPARQLQuery::DeviceType::GPU;
        } else {
            request.dev_type = SPARQLQuery::DeviceType::CPU;
        }

        // Execute the SPARQL query
        setpid(request);
        // only take back results of the last request if not silent
        request.result.blind = Global::silent;
        send_request(request);

        uint64_t start, end;
        start = timer::get_usec();
        reply = recv_reply();
        end = timer::get_usec();
        logstream(LOG_INFO) << "latency: " << (end - start) << " usec" << LOG_endl;
       
        reply.result.required_vars_name = request.result.required_vars_name;

        return 0;
    }  // end of run_single_query

    int execute_sparql_task(std::string msg_in, std::string plan, std::string& msg_out) {
        // forward to engines
        logstream(LOG_INFO) << "[RPCProxy] receive SPARQL_RPC request." << LOG_endl;

        uint64_t start, end;
        start = timer::get_usec();

        json json_res;
        try {
            SPARQLQuery reply;
            std::istringstream query(msg_in);
            std::istringstream query_fmt(plan);
            std::map<std::string, std::string> params;
            params["nopts"] = "1";
            params["mt_factor"] = "1";
            params["snd2gpu"] = "false";
            run_single_query(query, query_fmt, reply, params);

            // generate reply result
            Bundle bundle(reply);
            // std::cout << "receive reply from engine!" << std::endl;
            
            end = timer::get_usec();
            json_res["latency"] = end - start;

            json_res["StatusMsg"] = reply.result.status_code;
            // std::cout << "[execute_sparql_task0]" << json_res.dump() << std::endl;
            if (reply.result.status_code == SUCCESS) {
                reply2json(reply, json_res["Result"]);
            } else throw WukongException(reply.result.status_code);
        } catch (WukongException &ex) {
            // generate error msg reply
            json_res.clear();
            json_res["StatusMsg"] = ex.code();
            json_res["ErrorMsg"] = std::string(ERR_MSG(ex.code()));
            logstream(LOG_ERROR) << "Query failed [ERRNO " << ex.code() << "]: "
                                 << ERR_MSG(ex.code()) << LOG_endl;
            return ex.code();
        }
        msg_out = json_res.dump();
        return SUCCESS;
    }

    int retrieve_cluster_info(int cid, std::string& msg_out) {
        logstream(LOG_INFO) << "[RPCProxy] receive INFO_RPC request." << LOG_endl;
        msg_out = "\tnode num: " + std::to_string(Global::num_servers) + "\n";
        msg_out += "\tcurrent node: " + std::to_string(this->sid) + "\n";
        msg_out += "\tproxy num(per node): " + std::to_string(Global::num_proxies) + "\n";
        msg_out += "\tengine num(per node): " + std::to_string(Global::num_engines) + "\n";
        return SUCCESS;
    }
};

}  // namespace wukong
