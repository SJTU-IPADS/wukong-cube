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

#include <algorithm> // sort
#include <regex>

#include <boost/unordered_set.hpp>
#include <boost/unordered_map.hpp>

#include <tbb/concurrent_queue.h>

#include "core/common/global.hpp"
#include "core/common/type.hpp"
#include "core/common/coder.hpp"
#include "core/common/errors.hpp"
#include "core/common/string_server.hpp"

#include "core/common/bind.hpp"

#include "core/store/dgraph.hpp"

#include "core/hyperquery/query.hpp"

// engine
#include "core/engine/rmap.hpp"
#include "core/engine/msgr.hpp"

// utils
#include "utils/assertion.hpp"
#include "utils/math.hpp"
#include "utils/timer.hpp"

namespace wukong {

class HyperEngine {
private:
    static const ssid_t NO_VAR = 0;

    int sid;    // server id
    int tid;    // thread id

    StringServer *str_server;
    DGraph *graph;
    Coder *coder;
    Messenger *msgr;

    HyperRMap rmap; // a map of replies for pending (fork-join) queries
    pthread_spinlock_t rmap_lock;

    void op_get_vertices(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute V() op:" << LOG_endl;
        // TODO-zyw:
    }

    void op_get_edges(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute E() op:" << LOG_endl;
        // TODO-zyw:
    }

    void op_get_properties(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute P() op:" << LOG_endl;
    }

    void op_get_e2v(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute containV() op:" << LOG_endl;
    }

    void op_get_v2e(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute inE() op:" << LOG_endl;
    }

    void op_get_v2v(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute intersectV() op:" << LOG_endl;
    }

    void op_get_e2e(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute intersectE() op:" << LOG_endl;
    }

    std::vector<HyperQuery> generate_sub_query(HyperQuery &query, HyperQuery::Pattern& op) {
        // generate sub requests for all servers
        std::vector<HyperQuery> sub_queries(Global::num_servers);
        for (int i = 0; i < Global::num_servers; i++) {
            // TODO-zyw:
        }
    }

    // determine fork-join or in-place execution
    bool need_fork_join(HyperQuery &query, HyperQuery::Pattern& op) {
        // always need NOT fork-join when executing on single machine
        if (Global::num_servers == 1) return false;

        // TODO-zyw:
        return false;
    }

    // deal with global op (Eg., V(), E())
    bool dispatch(HyperQuery &query, HyperQuery::Pattern& op) {
        if (Global::num_servers * query.mt_factor == 1) return false;

        // TODO-zyw:
        return false;
    }

    void execute_one_op(HyperQuery& query, HyperQuery::Pattern& op) {
        switch(op.type){
        case HyperQuery::PatternType::GV:
            op_get_vertices(query, op);
            break;
        case HyperQuery::PatternType::GE:
            op_get_edges(query, op);
            break;
        case HyperQuery::PatternType::E2V:
            op_get_e2v(query, op);
            break;
        case HyperQuery::PatternType::V2E:
            op_get_v2e(query, op);
            break;
        case HyperQuery::PatternType::V2V:
            op_get_v2v(query, op);
            break;
        case HyperQuery::PatternType::E2E_ITSCT:
            op_get_e2e(query, op);
            break;
        case HyperQuery::PatternType::E2E_CT:
            op_get_e2e(query, op);
            break;
        case HyperQuery::PatternType::E2E_IN:
            op_get_e2e(query, op);
            break;
        default:
            assert(false);
        }
    }

    bool execute_ops(HyperQuery &query) {
        uint64_t time, access;
        logstream(LOG_INFO) << "[" << sid << "-" << tid << "] execute ops of "
                             << "Q(pqid=" << query.pqid << ", qid=" << query.qid << ")"
                             << LOG_endl;
        do {
            HyperQuery::Pattern& op = query.get_pattern();

            if (need_fork_join(query, op)) {
                std::vector<HyperQuery> sub_queries = generate_sub_query(query, op);
                rmap.put_parent_request(query, sub_queries.size());
                for (int i = 0; i < sub_queries.size(); i++) {
                    Bundle bundle(sub_queries[i]);
                    msgr->send_msg(bundle, i, tid);
                }
                return false; // outstanding
            }

            // exploit parallelism (multi-server and multi-threading)
            if (dispatch(query, op)) {
                return false;
            }

            time = timer::get_usec();
            execute_one_op(query, op);
            logstream(LOG_DEBUG) << "[" << sid << "-" << tid << "]"
                                 << " exec-time = " << (timer::get_usec() - time) << " usec"
                                 << LOG_endl;

            if (query.done(HyperQuery::SQ_PATTERN)) {
                return true;  // done
            }
        } while (true);
    }

public:
    tbb::concurrent_queue<HyperQuery> prior_stage;

    HyperEngine(int sid, int tid, StringServer *str_server,
                 DGraph *graph, Coder *coder, Messenger *msgr)
        : sid(sid), tid(tid), str_server(str_server),
          graph(graph), coder(coder), msgr(msgr) {

        pthread_spin_init(&rmap_lock, 0);
    }

    void execute_hyper_query(HyperQuery &query) {
        try {
            // encode the lineage of the query (server & thread)
            if (query.qid == -1) query.qid = coder->get_and_inc_qid();

            // 0. query has done
            if (query.state == HyperQuery::SQState::SQ_REPLY) {
                pthread_spin_lock(&rmap_lock);
                rmap.put_reply(query);

                if (!rmap.is_ready(query.pqid)) {
                    pthread_spin_unlock(&rmap_lock);
                    return;  // not ready (waiting for the rest)
                }

                // all sub-queries have done, continue to execute
                query = rmap.get_reply(query.pqid);
                pthread_spin_unlock(&rmap_lock);
            }

            // 1. not done, execute ops
            if (!query.done(HyperQuery::SQState::SQ_REPLY)) {
                if (!execute_ops(query)) return;  // outstanding
            }

        } catch (const char *msg) {
            //query.result.set_status_code(UNKNOWN_ERROR);
        } catch (WukongException &ex) {
            //query.result.set_status_code(ex.code());
        }
        // 6. Reply
        query.shrink();
        query.state = HyperQuery::SQState::SQ_REPLY;
        Bundle bundle(query);
        msgr->send_msg(bundle, coder->sid_of(query.pqid), coder->tid_of(query.pqid));
    }

};

} // namespace wukong
