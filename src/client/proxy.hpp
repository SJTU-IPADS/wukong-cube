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

#include <unistd.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "core/common/bundle.hpp"
#include "core/common/coder.hpp"
#include "core/common/errors.hpp"
#include "core/common/global.hpp"
#include "core/common/monitor.hpp"

#include "core/sparql/parser.hpp"
#include "core/sparql/query.hpp"

#include "core/network/adaptor.hpp"

#include "optimizer/planner.hpp"
#include "optimizer/stats.hpp"

#include "stringserver/string_mapping.hpp"

// utils
#include "utils/math.hpp"
#include "utils/timer.hpp"
#include "utils/time_tool.hpp"

namespace wukong {

class Proxy;

// a vector of pointers of all local proxies
// we need a global reference of the server_ptr to do cleanup
std::vector<std::shared_ptr<wukong::Proxy>> proxies;

class Proxy {
public:
    Proxy(int sid, int tid, StringMapping* str_server, DGraph* graph, Adaptor* adaptor, Stats* stats)
        : sid(sid), tid(tid), str_server(str_server), adaptor(adaptor), stats(stats), coder(sid, tid), parser(tid, str_server), planner(tid, graph, stats) {}

    int get_sid() { return this->sid; }

    int get_tid() { return this->tid; }

    /**
     * @brief Run a single query
     * 
     * @param is input
     * @param fmt_stream format file input
     * @param reply result
     * @param params query parameters
     * @return int 
     */
    virtual int run_single_query(std::istream& is, std::istream& fmt_stream,
                                 SPARQLQuery& reply, std::map<std::string, std::string>& params) {return 0;}

#ifdef DYNAMIC_GSTORE
    virtual int dynamic_load_data(std::string& dname, RDFLoad& reply, bool& check_dup) {return 0;}
#endif

protected:
    class Message {
    public:
        int sid;
        int tid;
        std::string msg;

        Message(int sid, int tid, std::string &msg)
            : sid(sid), tid(tid), msg(std::move(msg)) { }
    };

    std::vector<Message> pending_msgs;  // pending msgs to send

    int sid;  // server id
    int tid;  // thread id

    StringMapping* str_server;  // string server
    Adaptor* adaptor;
    Stats* stats;

    Coder coder;
    Parser parser;
    Planner planner;

    void setpid(SPARQLQuery& r) { r.pqid = coder.get_and_inc_qid(); }

    void setpid(RDFLoad& r) { r.pqid = coder.get_and_inc_qid(); }

    void setpid(GStoreCheck& r) { r.pqid = coder.get_and_inc_qid(); }

    /**
     * Send msg data to certain engine in given server(@dst_sid).
     * Return false if it fails. Bundle is pending in pending_msgs.
     */
    inline bool send(std::string& msg, int dst_sid, int dst_tid) {
        if (adaptor->send(dst_sid, dst_tid, msg))
            return true;

        pending_msgs.push_back(Message(dst_sid, dst_tid, msg));
        return false;
    }

    /**
     *  Send given bundle to given server(@dst_sid).
     */
    inline bool send(Bundle& bundle, int dst_sid) {
        std::string msg = bundle.to_str();
        send(msg, dst_sid);
    }

    /**
     *  Send given msg data to given server(@dst_sid).
     */
    inline bool send(std::string& msg, int dst_sid) {
        // NOTE: the partitioned mapping has better tail latency in batch mode
        int range = Global::num_engines / Global::num_proxies;
        // FIXME: BUG if Global::num_engines < Global::num_proxies
        ASSERT(range > 0);

        int base = Global::num_proxies + (range * tid);
        // randomly choose engine without preferred one
        int dst_eid = coder.get_random() % range;

        // If the preferred engine is busy, try the rest engines with round robin
        for (int i = 0; i < range; i++)
            if (adaptor->send(dst_sid, base + (dst_eid + i) % range, msg))
                return true;

        pending_msgs.push_back(Message(dst_sid, (base + dst_eid), msg));
        return false;
    }

    /**
     *  Try send all msgs in pending_msgs.
     */
    inline void sweep_msgs() {
        if (!pending_msgs.size()) return;

        logstream(LOG_DEBUG) << "#" << tid << " " << pending_msgs.size()
                             << " pending msgs on proxy." << LOG_endl;
        for (std::vector<Message>::iterator it = pending_msgs.begin();
             it != pending_msgs.end();) {
            if (adaptor->send(it->sid, it->tid, it->msg))
                it = pending_msgs.erase(it);
            else
                ++it;
        }
    }

    /**
     * Send SPARQLQuery to certain engine.
     */
    void send_request(SPARQLQuery& r) {
        ASSERT(r.pqid != -1);

        // submit the request to a certain server
        int start_sid = PARTITION(r.pattern_group.get_start());
        Bundle bundle(r);

        if (r.dev_type == SPARQLQuery::DeviceType::CPU) {
            logstream(LOG_DEBUG) << "dev_type is CPU, send to engine. r.pqid=" << r.pqid << LOG_endl;
            send(bundle, start_sid);
#ifdef USE_GPU
        } else if (r.dev_type == SPARQLQuery::DeviceType::GPU) {
            logstream(LOG_DEBUG) << "dev_type is GPU, send to GPU agent. r.pqid=" << r.pqid << LOG_endl;
            send(bundle, start_sid, WUKONG_GPU_AGENT_TID);
#endif
        } else {
            ASSERT_MSG(false, "Unknown device type");
        }
    }

    /**
     * Recv reply from engines.
     */
    SPARQLQuery recv_reply(void) {
        Bundle bundle = Bundle(adaptor->recv());
        ASSERT(bundle.type == SPARQL_QUERY);
        SPARQLQuery r = bundle.get_sparql_query();
        return r;
    }

    /**
     * Try recv reply from engines.
     */
    bool tryrecv_reply(SPARQLQuery& r) {
        std::string reply_msg;
        bool success = adaptor->tryrecv(reply_msg);
        Bundle bundle(reply_msg);
        if (success) {
            ASSERT(bundle.type == SPARQL_QUERY);
            r = bundle.get_sparql_query();
        }

        return success;
    }
};

}  // namespace wukong
