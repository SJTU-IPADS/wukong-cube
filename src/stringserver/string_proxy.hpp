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
#include <string>
#include <utility>

#include "rpc/rpc_server.hpp"

#include "client/proxy.hpp"
#include "utils/logger2.hpp"
#include "core/common/status.hpp"

namespace wukong {

/**
 * @brief String server proxy
 * 
 * Receive SSCacheRequests from StringClient 
 * and call StringServer to process requests
 */
class StringProxy : public Proxy {
public:
    explicit StringProxy(int port, StringMapping* str_server)
        : Proxy(0, 0, str_server, nullptr, nullptr, nullptr) {
        // set hostname and port
        this->hostname = "localhost";
        this->port = port;

        logstream(LOG_INFO) << "String server proxy will listen on " << hostname << ":" << port << " for RPC" << LOG_endl;
    }

    ~StringProxy() {
        delete srv;
    }

    void serve() {
        // initialize server
        srv = new RPCS(port);
        // register handlers
        srv->reg(RPC_CODE::STRING_RPC, this, &StringProxy::execute_string_task);
        // start server
        srv->start();
    }

private:
    std::string hostname;
    uint32_t port;
    RPCS *srv;

    int execute_string_task(std::string msg_in, std::string& msg_out) {
        logstream(LOG_DEBUG) << "[StringProxy] receive STRING_RPC request." << LOG_endl;
        Bundle bundle;
        bundle.init(msg_in);
        ASSERT(bundle.type == SSCACHE_REQ);
        SSCacheRequest req = bundle.get_sscache_req();

        // translate string<-->id
        if (req.req_type == SSCacheReqType::TRANS_ID) {
            auto trans_result = this->str_server->id2str(tid, req.vid);
            req.success = trans_result.first;
            if (req.success) {
                req.str = trans_result.second;
                logstream(LOG_DEBUG) << "Translate " << req.vid
                                    << "->" << req.str
                                    << " success" << LOG_endl;
            } else {
                logstream(LOG_INFO) << "Translate " << req.vid
                                    << "->" << req.str
                                    << " fail" << LOG_endl;
            }
        } else if (req.req_type == SSCacheReqType::TRANS_STR) {
            auto trans_result = this->str_server->str2id(tid, req.str);
            req.success = trans_result.first;
            if (req.success) {
                req.vid = trans_result.second;
                logstream(LOG_DEBUG) << "Translate " << req.str
                                    << "->" << req.vid
                                    << " success" << LOG_endl;
            } else {
                logstream(LOG_INFO) << "Translate " << req.str
                                    << "->" << req.vid
                                    << " fail" << LOG_endl;
            }
        }

        // reply
        Bundle reply(req);
        msg_out = reply.to_str();
        return SUCCESS;
    }
};

}  // namespace wukong
