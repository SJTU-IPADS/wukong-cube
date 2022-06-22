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
#include <mutex>
#include <string>
#include <vector>

#include "core/common/global.hpp"
#include "core/common/errors.hpp"
#include "core/common/status.hpp"

#include "utils/assertion.hpp"
#include "utils/logger2.hpp"
#include "utils/timer.hpp"

#include "rpc/rpc_client.hpp"

namespace wukong {

class RPCClient {
protected:
    RPCC *cl;
    mutable bool is_connected;

    // A mutex which protects the client.
    std::recursive_mutex client_mutex;
public:
    RPCClient(): is_connected(false) {}

    ~RPCClient() {
        disconnect();
    }

    /**
     * @brief Indicate whether client has connected with Wukong server.
     */
    bool connected() const {
        return is_connected;
    }

    /**
     * @brief Connect to Wukong using the given TCP `host` and `port`.
     *
     * @param host The host of Wukong server.
     * @param port The TCP port of Wukong server's RPC service.
     *
     * @return Status that indicates whether the connect has succeeded.
     */
    Status connect_to_server(const std::string& host, uint32_t port) {
        std::lock_guard<std::recursive_mutex> guard_(client_mutex);

        // initialize RPCC with server addr and call bind
        cl = new RPCC(host.c_str(), port);
        ASSERT_EQ(cl->bind(), 0);
        is_connected = true;

        return Status::OK();
    }

    /**
     * @brief Disconnect client from server.
     */
    void disconnect() {
        std::lock_guard<std::recursive_mutex> guard_(client_mutex);
        if (!this->is_connected) return;
        delete cl;
        is_connected = false;
    }

    /**
     * @brief Retrieve and print cluster information from RPC server.
     * 
     * @return Status that indicates whether the RPC has succeeded.
     */
    Status retrieve_cluster_info(int timeout = ConnectTimeoutMs) {
        if (timeout <= 0) timeout = ConnectTimeoutMs;
        // call info RPC
        std::string reply_msg;
        int ret = cl->call(RPC_CODE::INFO_RPC, reply_msg, timeout, cl->id());
        ASSERT_GE(ret, 0);
        // show cluster info
        std::cout << "[Cluster Info]: " << std::endl;
        std::cout << reply_msg << std::endl;
        return Status(ret, err_msgs[ret]);
    }

    /**
     * @brief Retrieve and print cluster information from RPC server.
     * 
     * @param query The query to be executed.
     * @param result The result of the input query.
     *
     * @return Status that indicates whether the connect has succeeded.
     */
    Status execute_sparql_query(std::string query, std::string& result, int timeout = ConnectTimeoutMs) {
        if (timeout <= 0) timeout = ConnectTimeoutMs;
        int ret = cl->call(RPC_CODE::SPARQL_RPC, result, timeout, query, "");
        ASSERT_GE(ret, 0);
        return Status(ret, err_msgs[ret]);
    }

    Status execute_sparql_query_with_plan(std::string query, std::string plan, std::string& result, int timeout = ConnectTimeoutMs) {
        if (timeout <= 0) timeout = ConnectTimeoutMs;
        int ret = cl->call(RPC_CODE::SPARQL_RPC, result, timeout, query, plan);
        ASSERT_GE(ret, 0);
        return Status(ret, err_msgs[ret]);
    }

};

}  // namespace wukong
