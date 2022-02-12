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

// Returns the number of identical items among 2 sets
uint32_t intersect_set(std::set<sid_t> a, std::set<sid_t> b) {
    uint32_t intersect_num = 0;
    for (auto &&ai : a) {
        auto res = b.find(ai);
        if (res != b.end()) {
            b.erase(res);
            intersect_num++;
        }
    }
    return intersect_num;
}

// Returns true if a contains b
bool contain_set(std::set<sid_t> a, std::set<sid_t> b) {
    for (auto &&ai : a) {
        auto res = b.find(ai);
        if (res != b.end()) {
            b.erase(res);
            if (b.empty()) return true;
        }
    }
    return b.empty();
}

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

    void op_get_e2v(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute containV() op:" << LOG_endl;
        // multi-edges e2v
        if(op.input_eids.size() > 1) {
            // TODO:
        }
        // single-const-edge e2v
        else if (op.input_eids.size() == 1) {
            heid_t start = op.input_eids[0];
            ssid_t end = op.output_var;

            HyperQuery::Result& res = query.result;

            // MUST be the first triple pattern
            ASSERT_ERROR_CODE(res.empty(), FIRST_PATTERN_ERROR);

            uint64_t sz = 0;
            sid_t* vids = graph->get_edge_by_heid(tid, start, sz);
            std::vector<sid_t> updated_result_table;
            for(uint64_t k = 0; k < sz; k++)
                updated_result_table.push_back(vids[k]);

            // update result and metadata
            res.vid_res_table.load_data(updated_result_table);
            res.add_var2col(end, res.get_col_num(HyperQuery::Result::TYPE_VERTEX));
            res.set_col_num(res.get_col_num(HyperQuery::Result::TYPE_VERTEX) + 1, SID_t);
            res.update_nrows();
        } 
        // single-var-vertex e2v
        else {
            ssid_t start = op.input_vids[0];
            ssid_t end = op.output_var;

            HyperQuery::Result& res = query.result;

            std::vector<sid_t> updated_result_table;

            sid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            sid_t* vids = NULL;
            uint64_t sz = 0;
            int nrows = res.get_row_num();

            for(int i = 0; i < nrows; i++) {
                heid_t cur = res.get_row_col(i, res.var2col(start), HEID_t);

                if (cur != cached) { // new KNOWN
                    cached = cur;
                    vids = graph->get_edge_by_heid(tid, cur, sz);
                }

                for (uint64_t k = 0; k < sz; k++) {
                    res.vid_res_table.append_row_to(i, updated_result_table);
                    updated_result_table.push_back(vids[k]);
                }
            }

            res.vid_res_table.load_data(updated_result_table);
            res.add_var2col(end, res.get_col_num(HyperQuery::Result::TYPE_VERTEX));
            res.set_col_num(res.get_col_num(HyperQuery::Result::TYPE_VERTEX) + 1, SID_t);
            res.update_nrows();
        }

        query.advance_step();
    }

    void op_get_v2e(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute inE() op:" << LOG_endl;
        ASSERT_GT(op.input_vids.size(), 0);
        // multi-vertices v2e
        if(op.input_vids.size() > 1) {
            // TODO:
        }
        // single-const-vertex v2e
        else if (op.input_vids[0] > 0) {
            sid_t start = op.input_vids[0];
            sid_t htid = op.he_type;
            ssid_t end = op.output_var;

            HyperQuery::Result& res = query.result;

            // MUST be the first triple pattern
            ASSERT_ERROR_CODE(res.empty(), FIRST_PATTERN_ERROR);

            uint64_t sz = 0;
            heid_t* eids = graph->get_heids_by_vertex_and_type(tid, start, htid, sz);
            std::vector<heid_t> updated_result_table;
            for(uint64_t k = 0; k < sz; k++)
                updated_result_table.push_back(eids[k]);

            // update result and metadata
            res.heid_res_table.load_data(updated_result_table);
            res.add_var2col(end, res.get_col_num(HyperQuery::Result::TYPE_EDGE));
            res.set_col_num(res.get_col_num(HyperQuery::Result::TYPE_EDGE) + 1, HEID_t);
            res.update_nrows();
        } 
        // single-var-vertex v2e
        else {
            ssid_t start = op.input_vids[0];
            sid_t htid = op.he_type;
            ssid_t end = op.output_var;

            HyperQuery::Result& res = query.result;

            std::vector<heid_t> updated_result_table;

            sid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            heid_t* eids = NULL;
            uint64_t sz = 0;
            int nrows = res.get_row_num();

            for(int i = 0; i < nrows; i++) {
                sid_t cur = res.get_row_col(i, res.var2col(start), SID_t);

                if (cur != cached) { // new KNOWN
                    cached = cur;
                    eids = graph->get_heids_by_vertex_and_type(tid, cur, htid, sz);
                }

                for (uint64_t k = 0; k < sz; k++) {
                    res.heid_res_table.append_row_to(i, updated_result_table);
                    updated_result_table.push_back(eids[k]);
                }
            }

            res.heid_res_table.load_data(updated_result_table);
            res.add_var2col(end, res.get_col_num(HyperQuery::Result::TYPE_EDGE));
            res.set_col_num(res.get_col_num(HyperQuery::Result::TYPE_EDGE) + 1, HEID_t);
            res.update_nrows();
        }

        query.advance_step();
    }

    void op_get_v2v(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute intersectV() op:" << LOG_endl;
    }

    void op_get_e2e(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_INFO) << "Execute E2E intersect op:" << LOG_endl;
        HyperQuery::Result &res = query.result;
        HyperQuery::ResultTable<heid_t> &res_he = res.heid_res_table;

        // check if the pattern is valid
        std::vector<heid_t> const_inputs;
        std::vector<ssid_t> var_inputs;
        std::vector<int> var_cols;
        for (auto &&input : op.input_vars) {
            if (input > 0) const_inputs.push_back(input);
            else if (input < 0) {
                int col = res.var2col(input);
                ASSERT_ERROR_CODE(col != NO_RESULT_COL, VERTEX_INVALID);
                var_inputs.push_back(input);
                var_cols.push_back(col);
            }
        }
        ASSERT_ERROR_CODE(!var_inputs.empty() || query.pattern_step == 0, VERTEX_INVALID);
        ASSERT_ERROR_CODE(!is_htid(op.bind_node), BIND_NODE_INVALID);
        ASSERT_ERROR_CODE((op.type == HyperQuery::PatternType::E2E_ITSCT && op.k != 0) ||
                            (op.type != HyperQuery::PatternType::E2E_ITSCT && op.k == 0), K_INVALID);

        uint64_t he_sz, vid_sz;
        sid_t* vids;
        heid_t* heids;
        HyperQuery::ResultTable<heid_t> updated_result_table;

        // valid check for intersect/in/contain operation
        auto valid_hes = [&](std::set<sid_t> &known, std::set<sid_t> &unknown) -> bool {
            switch (op.type)
            {
            case HyperQuery::PatternType::E2E_ITSCT:
                return (intersect_set(known, unknown) >= op.k);
            case HyperQuery::PatternType::E2E_CT:
                return (contain_set(known, unknown));
            case HyperQuery::PatternType::E2E_IN:
                return (contain_set(unknown, known));
            default: 
                ASSERT(false);
            }
        };

        // get const hyperedges first
        std::vector<std::set<sid_t>> const_hes(const_inputs.size());
        for (size_t i = 0; i < const_inputs.size(); i++) {
            vids = graph->get_edge_by_heid(tid, const_inputs[i], vid_sz);
            for (size_t k = 0; k < vid_sz; k++) const_hes[i].insert(vids[k]);
        }

        // get all hyperedges by hyper type
        heids = graph->get_heids_by_type(tid, op.bind_node, he_sz);

        // iterate through each hyper edge, and compare them to each const hyperedge
        for (size_t i = 0; i < he_sz; i++) {
            std::set<sid_t> curr_he;
            
            // get hyperedge content
            vids = graph->get_edge_by_heid(tid, heids[i], vid_sz);
            for (size_t k = 0; k < vid_sz; k++) curr_he.insert(vids[k]);

            // check if intersect with each const hyperedge
            bool flag = true;
            for (auto &&const_he : const_hes)
                if (!valid_hes(const_he, curr_he)) {flag = false; break;}
            if (!flag) continue;
            else if (var_inputs.empty()) {
                updated_result_table.result_data.push_back(heids[i]);
                continue;
            }

            // check if intersect with each var hyperedge
            int row2match = res.get_row_num();
            int col2match = var_inputs.size();
            for (size_t r = 0; r < row2match; r++) {
                flag = true;
                // get known hyperedges
                std::vector<std::set<sid_t>> known_hes(col2match);
                for (size_t c = 0; c < col2match; c++) {
                    vids = graph->get_edge_by_heid(tid, res_he.get_row_col(r, var_cols[c]), vid_sz);
                    for (size_t k = 0; k < vid_sz; k++) known_hes[c].insert(vids[k]);
                }
                
                // check if intersect with each var hyperedge
                for (auto &&known_he : known_hes)
                    if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                if (!flag) continue;
                
                // if valid, put the origin row and new heid into res_he
                res_he.append_row_to(r, updated_result_table);
                updated_result_table.result_data.push_back(heids[i]);
            }
        }

        // update result
        res_he.swap(updated_result_table);
        res.add_var2col(op.output_var, res.get_col_num(HEID_t), HEID_t);
        res.set_col_num(res.get_col_num(HEID_t) + 1, HEID_t);
        res.update_nrows();
        // increase pattern step
        query.pattern_step++;
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
