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

template<class DataType>
using idList = std::pair<DataType*, uint64_t>;

template<class DataType>
void intersect_set(std::set<DataType>& a, std::set<DataType>& b) {
    for (auto iter = std::begin(a); iter != std::end(a);) {
        auto res = b.find(*iter);
        if (res == b.end()) {
            iter = a.erase(iter);
        } else {
            iter++;
        }
    }
}

template<class DataType>
void intersect_set_v2(std::set<DataType>& a, DataType* b, uint64_t sz) {
    std::set<DataType> new_a;
    for(int i = 0; i < sz; i++) {
        if(a.count(b[i])) {
            new_a.insert(b[i]);
        }
    }
    new_a.swap(a);
}

template<class DataType>
void union_set(std::set<DataType>& a, std::set<DataType>& b) {
    for (auto&& item : b) {
        a.insert(item);
    }
}

// Returns the number of identical items among 2 sets
template<class DataType>
uint32_t intersect_set_num(idList<DataType> a, idList<DataType> b) {
    uint32_t intersect_num = 0;
    std::set<DataType> b_set(b.first, b.first + b.second);
    for (uint32_t i = 0; i < a.second; i++) {
        auto res = b_set.find(a.first[i]);
        if (res != b_set.end()) {
            b_set.erase(res);
            intersect_num++;
        }
    }
    return intersect_num;
}

// Returns true if a contains b
template<class DataType>
bool contain_set(idList<DataType> a, idList<DataType> b) {
    if (a.second < b.second) return false;
    
    std::set<DataType> b_set(b.first, b.first + b.second);
    for (uint32_t i = 0; i < a.second; i++) {
        auto res = b_set.find(a.first[i]);
        if (res != b_set.end()) {
            b_set.erase(res);
            if (b_set.empty()) return true;
        }
    }
    return b_set.empty();
}

// Returns the number of identical items among 2 sets
template<class DataType>
uint32_t intersect_set_num(std::set<DataType> a, std::set<DataType> b) {
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
template<class DataType>
bool contain_set(std::set<DataType> a, std::set<DataType> b) {
    if (a.size() < b.size()) return false;
    
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

    void op_get_edge_types(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute GE_TYPE op:" << LOG_endl;

        // MUST be the first triple pattern
        ASSERT_ERROR_CODE(query.result.empty(), FIRST_PATTERN_ERROR);

        // check if element and parameters valid
        ASSERT_ERROR_CODE(op.params.empty(), PARAMETER_INVALID);

        ssid_t end = op.output_var;
        HyperQuery::Result& res = query.result;

        uint64_t sz = 0;
        sid_t* tids = graph->get_edge_types(sz);

        // update result and metadata
        res.vid_res_table.result_data.assign(tids, tids + sz);
        res.add_var2col(end, res.get_col_num(SID_t));
        res.set_col_num(res.get_col_num(SID_t) + 1, SID_t);
        res.update_nrows();
        query.advance_step();
    }

    void op_get_vertices(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute GV op:" << LOG_endl;

        // TODO: right now we get vertices by hypertype
        // MUST be the first triple pattern
        ASSERT_ERROR_CODE(query.result.empty(), FIRST_PATTERN_ERROR);

        // check if the pattern is valid
        ASSERT_ERROR_CODE(op.input_vars.size() == 1, UNKNOWN_PATTERN);
        HyperQuery::Result& res = query.result;
        sid_t type_id = op.output_var;
        ssid_t end = op.input_vars[0];

        uint64_t sz = 0;
        sid_t* vids = graph->get_vids_by_htype(tid, type_id, sz);

        // update result and metadata
        res.vid_res_table.result_data.assign(vids, vids + sz);
        res.add_var2col(end, res.get_col_num(SID_t));
        res.set_col_num(res.get_col_num(SID_t) + 1, SID_t);
        res.update_nrows();
        query.advance_step();
    }

    void op_get_edges(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute GE op:" << LOG_endl;
        
        // valid GE op: 
        //      1. const type -> single Unknown var (first pattern)
        //      2. const type -> single Known var
        // Now we don't deal with parameters in GE
        
        // check if the pattern is valid
        ASSERT_ERROR_CODE(op.input_vars.size() == 1, UNKNOWN_PATTERN);
        HyperQuery::Result& res = query.result;
        sid_t type_id = op.output_var;
        ssid_t end = op.input_vars[0];
        int col = res.var2col(end);

        // single-const-to-unknown
        if (col == NO_RESULT_COL) {
            // MUST be the first triple pattern
            ASSERT_ERROR_CODE(query.result.empty(), FIRST_PATTERN_ERROR);

            // get hyperedges by htid
            uint64_t sz = 0;
            heid_t* eids = graph->get_heids_by_type(tid, type_id, sz);

            // update result data and metadata
            res.heid_res_table.result_data.assign(eids, eids + sz);
            res.add_var2col(end, res.get_col_num(HEID_t), HEID_t);
            res.set_col_num(res.get_col_num(HEID_t) + 1, HEID_t);
        }
        // single-known-to-unknown
        else {
            HyperQuery::Result updated_result;
            heid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            heid_t* htids = NULL;
            uint64_t tid_sz = 0;
            bool match = false;
            int nrows = res.get_row_num();

            for(int i = 0; i < nrows; i++) {
                heid_t cur = res.get_row_col_he(i, col);
                if (PARTITION(cur) != sid) continue;

                if (cur != cached) { // new KNOWN
                    cached = cur;
                    htids = graph->get_type_by_heid(tid, cur, tid_sz);
                    match = false;
                    for (uint64_t k = 0; k < tid_sz; k++)
                        if (htids[k] == type_id) {match = true; break;}
                }

                if (match) res.append_res_table_row_to(i, updated_result);
            }

            // update result data
            res.load_data(updated_result);
        }

        // update result metadata
        res.update_nrows();
        query.advance_step();
    }

    void op_get_e2v(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute E2V op:" << LOG_endl;

        // valid E2V op: 
        //      1. multi Const var -> single Unknown var
        //      2. multi Known var -> single Unknown var
        //      3. multi Known+Const var -> single Unknown var
        // Now we don't deal with parameters in E2V

        // check if the pattern is valid
        HyperQuery::HPState& state = query.pstate;
        HyperQuery::Result& res = query.result;
        auto& e2v_middle_map = query.result.e2v_middle_map;
        ssid_t& end = op.output_var;
        std::vector<int> known_cols;
        for (auto &&var : op.input_vars) {
            int col = res.var2col(var);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, UNKNOWN_PATTERN);   
            known_cols.push_back(col);
        }
        if (op.input_vars.empty()) ASSERT_ERROR_CODE(res.empty(), FIRST_PATTERN_ERROR);     // MUST be the first triple pattern

        uint64_t vid_sz = 0;
        sid_t* vids = NULL;
        if (op.input_vars.empty() && op.input_eids.size() == 1) {   // single-const-to-unknown
            heid_t& start = op.input_eids[0];
            // match the first const vid
            vids = graph->get_edge_by_heid(tid, start, vid_sz);
            // update result data
            res.vid_res_table.result_data.assign(vids, vids + vid_sz);
        } else if (op.input_eids.empty() && op.input_vars.size() == 1) {    // single-known-to-unknown
            HyperQuery::Result updated_result;
            int start_col = known_cols[0];
            heid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            sid_t *cached_vids = NULL;
            uint64_t cached_vid_sz = 0;
            int nrows = res.get_row_num();

            for(int i = 0; i < nrows; i++) {
                heid_t start = res.get_row_col_he(i, start_col);

                // skip the hyperedge on other nodes
                if (PARTITION(start) != sid) continue;

                // cache the first known eid
                if (start != cached) { // new KNOWN
                    cached = start;
                    cached_vids = graph->get_edge_by_heid(tid, start, cached_vid_sz);
                }
                vids = cached_vids;
                vid_sz = cached_vid_sz;

                for (int j = 0; j < vid_sz; j++) {
                    res.append_res_table_row_to(i, updated_result);
                    updated_result.vid_res_table.result_data.push_back(vids[j]);   
                }
            }

            // update result data
            res.load_data(updated_result);
        } else if (state == HyperQuery::HP_STEP_GET) {
            // do the first step: get as many hyperedges from local kv as possible

            // get hyperedges by input const eid
            for(int i = 0; i < op.input_eids.size(); i++) {
                heid_t cur = op.input_eids[i];
                if (PARTITION(cur) != sid) continue;
                vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                e2v_middle_map[cur] = std::vector<sid_t>(vids, vids + vid_sz);
            }

            // get hyperedges by input known eid
            int nrows = res.get_row_num();
            for(int i = 0; i < nrows; i++) {
                for (int j = 0; j < known_cols.size(); j++) {
                    heid_t cur = res.get_row_col_he(i, known_cols[j]);
                    if (PARTITION(cur) != sid || e2v_middle_map.find(cur) != e2v_middle_map.end()) continue;
                    vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                    e2v_middle_map[cur] = std::vector<sid_t>(vids, vids + vid_sz);
                }
            }
            
            // set pstate
            state = HyperQuery::HP_STEP_MATCH;
            return;
        } else if (state == HyperQuery::HP_STEP_MATCH) {
            // do the second step: intersect all hids sets from different nodes

            // match hyperedges from const eid
            std::set<sid_t> start_vids;
            int he_cnt = 0;
            for(int i = 0; i < op.input_eids.size(); i++) {
                auto& vids_vec = e2v_middle_map[op.input_eids[i]];
                if (i == 0) start_vids.insert(vids_vec.begin(), vids_vec.end());
                else intersect_set_v2(start_vids, vids_vec.data(), vids_vec.size());
            }
            if (op.input_vars.empty()) {      // multi-const-to-unknown
                // update result data
                res.vid_res_table.result_data.assign(start_vids.begin(), start_vids.end());
                goto done;
            }
            
            // match hyperedges from known eid
            HyperQuery::Result updated_result;
            int nrows = res.get_row_num();
            for(int i = 0; i < nrows; i++) {
                // start vid set for current row
                std::set<sid_t> cur_vids;
                heid_t start = res.get_row_col_he(i, known_cols[0]);
                if (!op.input_eids.empty()) {    // const+known-to-unknown
                    cur_vids = start_vids;
                    auto& vids_vec = e2v_middle_map[start];
                    intersect_set_v2(start_vids, vids_vec.data(), vids_vec.size());
                }
                else {                          // multi-known-to-unknown
                    auto& vids_vec = e2v_middle_map[start];
                    cur_vids.insert(vids_vec.begin(), vids_vec.end());
                }

                // rest vid set for current row
                for (int j = 1; j < known_cols.size(); j++) {
                    heid_t cur = res.get_row_col_he(i, known_cols[j]);
                    auto& vids_vec = e2v_middle_map[cur];
                    intersect_set_v2(cur_vids, vids_vec.data(), vids_vec.size());
                }

                // update result data
                for (auto &&vid : start_vids) {
                    res.append_res_table_row_to(i, updated_result);
                    updated_result.vid_res_table.result_data.push_back(vid);   
                }
            }

            // update result data
            res.load_data(updated_result);
        } else {ASSERT(false);}

    done:
        // update result metadata
        res.add_var2col(end, res.get_col_num(SID_t), SID_t);
        res.set_col_num(res.get_col_num(SID_t) + 1, SID_t);
        res.update_nrows();
        query.advance_step();
    }

    void op_get_v2e(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute V2E op:" << LOG_endl;

        // valid V2E op: 
        //      1. multi Const var -> single Unknown var
        //      2. multi Known var -> single Unknown var
        //      3. multi Known+Const var -> single Unknown var
        // Now we have a mandatory parameter in V2E:
        //      "etype": Specify target hyperedge type

        // check if the pattern is valid
        HyperQuery::HPState& state = query.pstate;
        HyperQuery::Result& res = query.result;
        auto& v2e_middle_map = query.result.v2e_middle_map;
        ssid_t& end = op.output_var;
        ASSERT_ERROR_CODE(op.params.size() == 1 && op.params[0].p_type == HyperQuery::P_ETYPE, PARAMETER_INVALID);
        sid_t edge_type = op.params[0].sid;
        std::vector<int> known_cols;
        for (auto &&var : op.input_vars) {
            int col = res.var2col(var);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, UNKNOWN_PATTERN);   
            known_cols.push_back(col);
        }
        if (op.input_vars.empty()) ASSERT_ERROR_CODE(res.empty(), FIRST_PATTERN_ERROR);     // MUST be the first triple pattern

        uint64_t eid_sz = 0;
        heid_t* eids = NULL;
        if (op.input_vars.empty() && op.input_vids.size() == 1) {   // single-const-to-unknown
            sid_t& start = op.input_vids[0];
            // match the first const vid
            eids = graph->get_heids_by_vertex_and_type(tid, start, edge_type, eid_sz);
            // update result data
            res.heid_res_table.result_data.assign(eids, eids + eid_sz);
        } else if (op.input_vids.empty() && op.input_vars.size() == 1) {    // single-known-to-unknown
            HyperQuery::Result updated_result;
            int start_col = known_cols[0];
            sid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            heid_t* cached_eids = NULL;
            uint64_t cached_eid_sz = 0;
            int nrows = res.get_row_num();

            for(int i = 0; i < nrows; i++) {
                sid_t start = res.get_row_col(i, start_col);

                // skip the hyperedge on other nodes
                if (PARTITION(start) != sid) continue;

                // cache the first known eid
                if (start != cached) { // new KNOWN
                    cached = start;
                    cached_eids = graph->get_heids_by_vertex_and_type(tid, start, edge_type, cached_eid_sz);
                }
                eids = cached_eids;
                eid_sz = cached_eid_sz;

                for (int j = 0; j < eid_sz; j++) {
                    res.append_res_table_row_to(i, updated_result);
                    updated_result.heid_res_table.result_data.push_back(eids[j]);   
                }
            }

            // update result data
            res.load_data(updated_result);
        } else if (state == HyperQuery::HP_STEP_GET) {
            // do the first step: get as many hyperedges from local kv as possible

            // get hyperedges by input const eid
            for(int i = 0; i < op.input_vids.size(); i++) {
                sid_t cur = op.input_vids[i];
                if (PARTITION(cur) != sid) continue;
                eids = graph->get_heids_by_vertex_and_type(tid, cur, edge_type, eid_sz);
                v2e_middle_map[cur] = std::vector<heid_t>(eids, eids + eid_sz);
            }

            // get hyperedges by input known eid
            int nrows = res.get_row_num();
            for(int i = 0; i < nrows; i++) {
                for (int j = 0; j < known_cols.size(); j++) {
                    sid_t cur = res.get_row_col(i, known_cols[j]);
                    if (PARTITION(cur) != sid || v2e_middle_map.find(cur) != v2e_middle_map.end()) continue;
                    eids = graph->get_heids_by_vertex_and_type(tid, cur, edge_type, eid_sz);
                    v2e_middle_map[cur] = std::vector<heid_t>(eids, eids + eid_sz);
                }
            }
            
            // set pstate
            state = HyperQuery::HP_STEP_MATCH;
            return;
        } else if (state == HyperQuery::HP_STEP_MATCH) {
            // do the second step: intersect all hids sets from different nodes

            // match hyperedges from const eid
            std::set<heid_t> start_eids;
            for(int i = 0; i < op.input_vids.size(); i++) {
                auto& eids_vec = v2e_middle_map[op.input_vids[i]];
                if (i == 0) start_eids.insert(eids_vec.begin(), eids_vec.end());
                else intersect_set_v2(start_eids, eids_vec.data(), eids_vec.size());
            }

            // multi-const-to-unknown
            if (op.input_vars.empty()) {
                // update result data
                res.heid_res_table.result_data.assign(start_eids.begin(), start_eids.end());
                goto done;
            }
            
            // match hyperedges from known eid
            HyperQuery::Result updated_result;
            int nrows = res.get_row_num();
            for(int i = 0; i < nrows; i++) {
                // start vid set for current row
                std::set<heid_t> cur_eids;
                sid_t start = res.get_row_col(i, known_cols[0]);
                if (!op.input_vids.empty()) {    // const+known-to-unknown
                    cur_eids = start_eids;
                    auto& eids_vec = v2e_middle_map[start];
                    intersect_set_v2(start_eids, eids_vec.data(), eids_vec.size());
                }
                else {                          // multi-known-to-unknown
                    auto& eids_vec = v2e_middle_map[start];
                    cur_eids.insert(eids_vec.begin(), eids_vec.end());
                }

                // rest vid set for current row
                for (int j = 1; j < known_cols.size(); j++) {
                    sid_t cur = res.get_row_col(i, known_cols[j]);
                    auto& eids_vec = v2e_middle_map[cur];
                    intersect_set_v2(cur_eids, eids_vec.data(), eids_vec.size());
                }

                // update result data
                for (auto &&eid : start_eids) {
                    res.append_res_table_row_to(i, updated_result);
                    updated_result.heid_res_table.result_data.push_back(eid);   
                }
            }

            // update result data
            res.load_data(updated_result);
        } else {ASSERT(false);}

    done:
        // update result metadata
        res.add_var2col(end, res.get_col_num(HEID_t), HEID_t);
        res.set_col_num(res.get_col_num(HEID_t) + 1, HEID_t);
        res.update_nrows();
        query.advance_step();
    }

    void op_get_v2v(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute V2V op:" << LOG_endl;
        
        // valid E2V op: 
        //      1. E2E_ITSCT: multi const/known/const+known var -> single Unknown var
        //          - mandatory first param "etype": Specify the hyperedge type
        //          - mandatory second param "ge"/"gt"/...: Specify intersect condition
        //      2. E2E_ITSCT: multi const/known/const+known var -> single known var
        //          - mandatory first param "etype": Specify the hyperedge type
        //          - mandatory param "ge"/"gt"/...: Specify intersect condition

        std::vector<sid_t> &input_vids = op.input_vids;
        std::vector<ssid_t> &input_vars = op.input_vars;
        std::vector<heid_t> &input_eids = op.input_eids;
        HyperQuery::Result &res = query.result;
        HyperQuery::ResultTable<sid_t> &res_vtable = res.vid_res_table;

        // check if the pattern is valid
        ASSERT_ERROR_CODE(input_eids.empty(), VERTEX_INVALID);
        ASSERT_ERROR_CODE(op.params.size() == 2, PARAMETER_INVALID);
        ASSERT_ERROR_CODE(op.params[0].type == SID_t && is_htid(op.params[0].sid), PARAMETER_INVALID);
        ASSERT_ERROR_CODE(op.params[1].type == INT_t, PARAMETER_INVALID);
        sid_t &he_type = op.params[0].sid;
        std::vector<int> known_cols;
        for (auto &&input : op.input_vars) {
            int col = res.var2col(input);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, VERTEX_INVALID);
            known_cols.push_back(col);
        }
        if (input_vars.empty()) ASSERT_ERROR_CODE(res.empty() && query.pattern_step == 0, FIRST_PATTERN_ERROR);
        ssid_t &end = op.output_var;
        int col = res.var2col(end);

        uint64_t he_sz, vid_sz;
        sid_t* vids;
        heid_t* heids;
        HyperQuery::Result updated_result;

        // valid check for intersect/in/contain operation
        auto valid_hes = [&](idList<heid_t> &input, idList<heid_t> &output) -> bool {
            HyperQuery::ParamType &p_type = op.params[1].p_type;
            int &limit = op.params[1].num;
            int intersect_factor = intersect_set_num(input, output);
            switch (p_type)
            {
            case HyperQuery::P_EQ:
                return (intersect_factor == limit);
            case HyperQuery::P_NE:
                return (intersect_factor != limit);
            case HyperQuery::P_LT:
                return (intersect_factor < limit);
            case HyperQuery::P_GT:
                return (intersect_factor > limit);
            case HyperQuery::P_LE:
                return (intersect_factor <= limit);
            case HyperQuery::P_GE:
                return (intersect_factor >= limit);
            default:
                logstream(LOG_ERROR) << "error parameter type!" << LOG_endl;
                ASSERT(false);
            }
        };

        // get hyperedges of the const vids
        std::vector<idList<heid_t>> const_hes(input_vids.size());
        for (size_t i = 0; i < input_vids.size(); i++) {
            const_hes[i].first = graph->get_heids_by_vertex_and_type(tid, input_vids[i], he_type, he_sz);
            const_hes[i].second = he_sz;
        }

        // get hyperedges of the known vids
        int nrows = res.get_row_num();
        int ncols = known_cols.size();
        sid_t cached = BLANK_ID;
        std::vector<std::vector<idList<heid_t>>> known_hes(nrows, std::vector<idList<heid_t>>(ncols));
        for (size_t c = 0; c < ncols; c++) {
            for (size_t r = 0; r < nrows; r++) {
                sid_t curr = res_vtable.get_row_col(r, known_cols[c]);
                if (curr != cached) {
                    cached = curr;
                    heids = graph->get_heids_by_vertex_and_type(tid, curr, he_type, he_sz);
                }
                known_hes[r][c].first = heids;
                known_hes[r][c].second = he_sz;
            }       
        }

        // const/known-to-unknown
        if (col == NO_RESULT_COL) {
            auto get_vids_by_eids = [&](idList<heid_t> &input, std::set<sid_t> &candidates) {
                candidates.clear();
                for (size_t i = 0; i < input.second; i++) {
                    vids = graph->get_edge_by_heid(tid, input.first[i], vid_sz);
                    for (size_t k = 0; k < vid_sz; k++) candidates.insert(vids[k]);
                }
                return candidates;
            };

            // const/const+known-to-unknown
            if (!const_hes.empty()) {
                // get all candidate vids by hyper type
                std::set<sid_t> candidates;
                get_vids_by_eids(const_hes[0], candidates);

                // iterate through each hyper edge, and compare them to each const hyperedge
                for (auto &&candidate : candidates) {
                    // get related hyperedges
                    heids = graph->get_heids_by_vertex_and_type(tid, candidate, he_type, he_sz);
                    idList<heid_t> curr_he(heids, he_sz);

                    // check if intersect with each const vid
                    bool flag = true;
                    for (auto &&const_he : const_hes)
                        if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                    if (!flag) continue;
                    else if (input_vars.empty()) {
                        updated_result.vid_res_table.result_data.push_back(candidate);
                        continue;
                    }

                    // check if intersect with each var vid
                    for (size_t r = 0; r < nrows; r++) {
                        flag = true;

                        // check if intersect with each var vid
                        for (auto &&known_he : known_hes[r])
                            if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                        if (!flag) continue;
                        
                        // if valid, put the origin row and new vid into res_he
                        res.append_res_table_row_to(r, updated_result);
                        updated_result.vid_res_table.result_data.push_back(candidate);
                    }
                }
            }
            // known-to-unknown
            else {
                // cache the first known sid
                cached = BLANK_ID;
                std::set<sid_t> candidates;

                // check if intersect with each var vid
                for (size_t r = 0; r < nrows; r++) {
                    // get the candidate vids of this row first
                    ssid_t &start = op.input_vars[0];
                    sid_t curr = res_vtable.get_row_col(r,res.var2col(start));
                    if (curr != cached) {
                        cached = curr;
                        get_vids_by_eids(known_hes[r][0], candidates);
                    }

                    for (auto &&candidate : candidates) {
                        // get related hyperedges
                        heids = graph->get_heids_by_vertex_and_type(tid, candidate, he_type, he_sz);
                        idList<heid_t> curr_he(heids, he_sz);

                        // check if intersect with each var vid
                        bool flag = true;
                        for (auto &&known_he : known_hes[r])
                            if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                        if (!flag) continue;
                        
                        // if valid, put the origin row and new vid into res_he
                        res.append_res_table_row_to(r, updated_result);
                        updated_result.vid_res_table.result_data.push_back(candidate);
                    }
                }
            }

            // update result data
            res.load_data(updated_result);
            res.add_var2col(end, res.get_col_num(SID_t), SID_t);
            res.set_col_num(res.get_col_num(SID_t) + 1, SID_t);
        }
        // const/known-to-known
        else {
            for (size_t r = 0; r < nrows; r++) {
                // get hyperedges related to current vid
                heids = graph->get_heids_by_vertex_and_type(tid, res_vtable.get_row_col(r, col), he_type, he_sz);
                idList<heid_t> curr_he(heids, he_sz);

                // check if intersect with each const vid
                bool flag = true;
                for (auto &&const_he : const_hes)
                    if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                if (!flag) continue;

                // check if intersect with each var vid
                for (auto &&known_he : known_hes[r])
                    if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                if (!flag) continue;
                
                // if valid, put current row into res_he
                res.append_res_table_row_to(r, updated_result);
            }

            // update result data
            res.load_data(updated_result);
        }

        // update result meta
        res.update_nrows();
        // increase pattern step
        query.advance_step();
    }

    void op_get_e2e(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute E2E op:" << LOG_endl;
        
        // valid E2V op: 
        //      1. E2E_ITSCT: multi const/known/const+known var -> single Unknown var
        //          - mandatory first param "etype": Specify target hyperedge type
        //          - mandatory second param "ge"/"gt"/...: Specify intersect condition
        //      2. E2E_ITSCT: multi const/known/const+known var -> single known var
        //          - mandatory param "ge"/"gt"/...: Specify intersect condition
        //      3. E2E_IN/E2E_CT: multi const/known/const+known var -> single Unknown var
        //          - mandatory param "etype": Specify target hyperedge type
        //      4. E2E_IN/E2E_CT: multi const/known/const+known var -> single known var

        HyperQuery::PatternType &type = op.type;
        std::vector<sid_t> &input_vids = op.input_vids;
        std::vector<ssid_t> &input_vars = op.input_vars;
        std::vector<heid_t> &input_eids = op.input_eids;
        HyperQuery::Result &res = query.result;
        HyperQuery::ResultTable<heid_t> &res_he = res.heid_res_table;

        // check if the pattern is valid
        ASSERT_ERROR_CODE(input_vids.empty(), VERTEX_INVALID);
        std::vector<int> known_cols;
        for (auto &&input : op.input_vars) {
            int col = res.var2col(input);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, VERTEX_INVALID);
            known_cols.push_back(col);
        }
        if (input_vars.empty()) ASSERT_ERROR_CODE(res.empty() && query.pattern_step == 0, FIRST_PATTERN_ERROR);
        ssid_t &end = op.output_var;
        int col = res.var2col(end);

        uint64_t he_sz, vid_sz;
        sid_t* vids;
        heid_t* heids;
        HyperQuery::Result updated_result;

        // valid check for intersect/in/contain operation
        auto valid_hes = [&](idList<sid_t> &input, idList<sid_t> &output) -> bool {
            switch (type)
            {
            case HyperQuery::PatternType::E2E_ITSCT:
            {
                int param_idx = col == NO_RESULT_COL? 1: 0;
                HyperQuery::ParamType &p_type = op.params[param_idx].p_type;
                int &limit = op.params[param_idx].num;
                int intersect_factor = intersect_set_num(input, output);
                switch (p_type)
                {
                case HyperQuery::P_EQ:
                    return (intersect_factor == limit);
                case HyperQuery::P_NE:
                    return (intersect_factor != limit);
                case HyperQuery::P_LT:
                    return (intersect_factor < limit);
                case HyperQuery::P_GT:
                    return (intersect_factor > limit);
                case HyperQuery::P_LE:
                    return (intersect_factor <= limit);
                case HyperQuery::P_GE:
                    return (intersect_factor >= limit);
                default:
                    logstream(LOG_ERROR) << "error parameter type!" << LOG_endl;
                    ASSERT(false);
                }
            }
            case HyperQuery::PatternType::E2E_CT:
                return (contain_set(input, output));
            case HyperQuery::PatternType::E2E_IN:
                return (contain_set(output, input));
            default: 
                logstream(LOG_ERROR) << "error pattern type!" << LOG_endl;
                ASSERT(false);
            }
        };

        // get const hyperedges
        std::vector<idList<sid_t>> const_hes(input_eids.size());
        for (size_t i = 0; i < input_eids.size(); i++) {
            const_hes[i].first = graph->get_edge_by_heid(tid, input_eids[i], vid_sz);
            const_hes[i].second = vid_sz;
        }

        // get all known input hyperedges
        int nrows = res.get_row_num();
        int ncols = known_cols.size();
        heid_t cached = BLANK_ID;
        std::vector<std::vector<idList<sid_t>>> known_hes(nrows, std::vector<idList<sid_t>>(ncols));
        for (size_t c = 0; c < ncols; c++) {
            for (size_t r = 0; r < nrows; r++) {
                heid_t curr = res_he.get_row_col(r, known_cols[c]);
                if (curr != cached) {
                    cached = curr;
                    vids = graph->get_edge_by_heid(tid, curr, vid_sz);
                }
                known_hes[r][c].first = vids;
                known_hes[r][c].second = vid_sz;
            }       
        }

        // const/known-to-unknown
        if (col == NO_RESULT_COL) {
            // check parameters
            ASSERT_ERROR_CODE((type == HyperQuery::PatternType::E2E_ITSCT && op.params.size() == 2) ||      // params: he_type + k
                            (type != HyperQuery::PatternType::E2E_ITSCT && op.params.size() == 1), PARAMETER_INVALID);  // params: he_type
            ASSERT_ERROR_CODE(op.params[0].type == SID_t && is_htid(op.params[0].sid), PARAMETER_INVALID);
            if (type == HyperQuery::PatternType::E2E_ITSCT) ASSERT_ERROR_CODE(op.params[1].type == INT_t, PARAMETER_INVALID);
            sid_t &he_type = op.params[0].sid;

            auto get_eids_by_vids = [&](idList<sid_t> &input, std::set<heid_t> &candidates) {
                candidates.clear();
                for (size_t i = 0; i < input.second; i++) {
                    heids = graph->get_heids_by_vertex_and_type(tid, input.first[i], he_type, he_sz);
                    for (size_t k = 0; k < he_sz; k++) candidates.insert(heids[k]);
                }
                return candidates;
            };

            // const/const+known-to-unknown
            if (!const_hes.empty()) {
                // get all candidate eids by the fiist const vid
                std::set<heid_t> candidates;
                get_eids_by_vids(const_hes[0], candidates);

                // iterate through each candidate, and compare them to each const/known hyperedge
                for (auto &&candidate : candidates) {
                    // get related hyperedges
                    vids = graph->get_edge_by_heid(tid, candidate, vid_sz);
                    idList<sid_t> curr_he(vids, vid_sz);

                    // check if intersect with each const hyperedge
                    bool flag = true;
                    for (auto &&const_he : const_hes)
                        if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                    if (!flag) continue;
                    else if (input_vars.empty()) {
                        updated_result.heid_res_table.result_data.push_back(candidate);
                        continue;
                    }

                    // check if intersect with each var vid
                    for (size_t r = 0; r < nrows; r++) {
                        flag = true;

                        // check if intersect with each var vid
                        for (auto &&known_he : known_hes[r])
                            if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                        if (!flag) continue;
                        
                        // if valid, put the origin row and new vid into res_he
                        res.append_res_table_row_to(r, updated_result);
                        updated_result.heid_res_table.result_data.push_back(candidate);
                    }
                }
            }
            // known-to-unknown
            else {
                // cache the first known sid
                cached = BLANK_ID;
                std::set<heid_t> candidates;

                // check if intersect with each var vid
                for (size_t r = 0; r < nrows; r++) {
                    // get the candidate vids of this row first
                    ssid_t &start = op.input_vars[0];
                    heid_t curr = res_he.get_row_col(r, res.var2col(start));
                    if (curr != cached) {
                        cached = curr;
                        get_eids_by_vids(known_hes[r][0], candidates);
                    }

                    for (auto &&candidate : candidates) {
                        // get related hyperedges
                        vids = graph->get_edge_by_heid(tid, candidate, vid_sz);
                        idList<sid_t> curr_he(vids, vid_sz);

                        // check if intersect with each var vid
                        bool flag = true;
                        for (auto &&known_he : known_hes[r])
                            if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                        if (!flag) continue;
                        
                        // if valid, put the origin row and new vid into res_he
                        res.append_res_table_row_to(r, updated_result);
                        updated_result.heid_res_table.result_data.push_back(candidate);
                    }
                }
            }

            // update result data
            res.load_data(updated_result);
            res.add_var2col(end, res.get_col_num(HEID_t), HEID_t);
            res.set_col_num(res.get_col_num(HEID_t) + 1, HEID_t);
        }
        // const/known-to-known
        else {
            // check parameters
            ASSERT_ERROR_CODE((type == HyperQuery::PatternType::E2E_ITSCT && op.params.size() == 1) ||      // params: he_type + k
                            (type != HyperQuery::PatternType::E2E_ITSCT && op.params.empty()), PARAMETER_INVALID);  // params: he_type
            if (type == HyperQuery::PatternType::E2E_ITSCT) ASSERT_ERROR_CODE(op.params[0].type == INT_t, PARAMETER_INVALID);

            for (size_t r = 0; r < nrows; r++) {
                // get current hyperedge content
                vids = graph->get_edge_by_heid(tid, res_he.get_row_col(r, col), vid_sz);
                idList<sid_t> curr_he(vids, vid_sz);

                // check if intersect with each const hyperedge
                bool flag = true;
                for (auto &&const_he : const_hes)
                    if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                if (!flag) continue;

                // check if intersect with each var hyperedge
                for (auto &&known_he : known_hes[r])
                    if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                if (!flag) continue;
                
                // if valid, put current row into res_he
                res.append_res_table_row_to(r, updated_result);
            }

            // update result data
            res.load_data(updated_result);
        }

        // update result meta
        res.update_nrows();
        // increase pattern step
        query.advance_step();
    }
    
    void op_get_v2v_distributed(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute V2V op:" << LOG_endl;
        
        // valid E2V op: 
        //      1. E2E_ITSCT: multi const/known/const+known var -> single Unknown var
        //          - mandatory first param "etype": Specify the hyperedge type
        //          - mandatory second param "ge"/"gt"/...: Specify intersect condition
        //      2. E2E_ITSCT: multi const/known/const+known var -> single known var
        //          - mandatory first param "etype": Specify the hyperedge type
        //          - mandatory param "ge"/"gt"/...: Specify intersect condition

        HyperQuery::HPState& state = query.pstate;
        std::vector<sid_t> &input_vids = op.input_vids;
        std::vector<ssid_t> &input_vars = op.input_vars;
        std::vector<heid_t> &input_eids = op.input_eids;
        HyperQuery::Result &res = query.result;
        HyperQuery::ResultTable<sid_t> &res_vtable = res.vid_res_table;
        auto& v2e_middle_map = res.v2e_middle_map;
        int nrows = res.get_row_num();

        // check if the pattern is valid
        ASSERT_ERROR_CODE(input_eids.empty(), VERTEX_INVALID);
        ASSERT_ERROR_CODE(op.params.size() == 2, PARAMETER_INVALID);
        ASSERT_ERROR_CODE(op.params[0].type == SID_t && is_htid(op.params[0].sid), PARAMETER_INVALID);
        ASSERT_ERROR_CODE(op.params[1].type == INT_t, PARAMETER_INVALID);
        std::vector<int> known_cols;
        for (auto &&input : op.input_vars) {
            int col = res.var2col(input);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, VERTEX_INVALID);
            known_cols.push_back(col);
        }
        ssid_t &end = op.output_var;
        int end_col = res.var2col(end);
        sid_t he_type;
        if (end_col == NO_RESULT_COL) {
            if (input_vars.empty()) ASSERT_ERROR_CODE(res.empty() && query.pattern_step == 0, FIRST_PATTERN_ERROR);
            ASSERT_ERROR_CODE(op.params.size() == 2, PARAMETER_INVALID);  // params: he_type
            ASSERT_ERROR_CODE(op.params[0].type == SID_t && is_htid(op.params[0].sid), PARAMETER_INVALID);
            ASSERT_ERROR_CODE(op.params[1].type == INT_t, PARAMETER_INVALID);
            he_type = op.params[0].sid;
        } else {
            ASSERT_ERROR_CODE(op.params.size() == 1 && op.params[0].type == INT_t, PARAMETER_INVALID);  // params: none
        }

        // valid check for intersect/in/contain operation
        auto valid_hes = [&](std::set<heid_t> &input, std::set<heid_t> &output) -> bool {
            int param_idx = end_col == NO_RESULT_COL? 1: 0;
            HyperQuery::ParamType &p_type = op.params[param_idx].p_type;
            int &limit = op.params[param_idx].num;
            int intersect_factor = intersect_set_num(input, output);
            switch (p_type)
            {
            case HyperQuery::P_EQ:
                return (intersect_factor == limit);
            case HyperQuery::P_NE:
                return (intersect_factor != limit);
            case HyperQuery::P_LT:
                return (intersect_factor < limit);
            case HyperQuery::P_GT:
                return (intersect_factor > limit);
            case HyperQuery::P_LE:
                return (intersect_factor <= limit);
            case HyperQuery::P_GE:
                return (intersect_factor >= limit);
            default:
                logstream(LOG_ERROR) << "error parameter type!" << LOG_endl;
                ASSERT(false);
            }
        };

        // get the set of vids by eid from e2v_middle_map
        auto get_vh_from_map = [&](sid_t &vid) -> std::set<heid_t>{
            auto map_res = v2e_middle_map.find(vid);
            ASSERT(map_res != v2e_middle_map.end());
            return std::set<heid_t>(map_res->second.begin(), map_res->second.end());
        };

        uint64_t he_sz, vid_sz;
        sid_t* vids;
        heid_t* heids;
        HyperQuery::Result updated_result;

        // TODO: optimize single K2U
        if (input_vids.size() == 1 && op.input_vars.empty()) {      // single-const-to-unknown/known
            // the const hyperedge should be in e2v_middle_map already
            std::set<heid_t> const_he = get_vh_from_map(input_vids[0]);

            if (end_col == NO_RESULT_COL) {     // single-const-to-unknown
                // get all candidate hyperedges by hyper type
                vids = graph->get_vids_by_htype(tid, he_type, vid_sz);

                // iterate through each candidate hyper edge
                for (size_t i = 0; i < vid_sz; i++) {            
                    heids = graph->get_heids_by_vertex_and_type(tid, vids[i], he_type, he_sz);
                    std::set<heid_t> curr_he(heids, heids + he_sz);
                    if (valid_hes(const_he, curr_he)) 
                        res.vid_res_table.result_data.push_back(vids[i]);
                }

                // update result meta
                res.add_var2col(end, res.get_col_num(SID_t), SID_t);
                res.set_col_num(res.get_col_num(SID_t) + 1, SID_t);
            } else {                            // single-const-to-known
                for(int i = 0; i < nrows; i++) {
                    // get output known eids
                    sid_t cur = res.get_row_col(i, end_col);
                    heids = graph->get_heids_by_vertex_and_type(tid, cur, he_type, he_sz);
                    std::set<heid_t> curr_he(heids, heids + he_sz);

                    // if valid, put current row into res_he
                    if (valid_hes(const_he, curr_he)) res.append_res_table_row_to(i, updated_result);
                }

                // update result data
                res.load_data(updated_result);
            }
        } else if (state == HyperQuery::HP_STEP_GET) {
            // do the first step: get as many hyperedges from local kv as possible
            
            // get hyperedges by input const eid
            for(int i = 0; i < input_vids.size(); i++) {
                sid_t cur = input_vids[i];
                if (PARTITION(cur) != sid) continue;
                heids = graph->get_heids_by_vertex_and_type(tid, cur, he_type, he_sz);
                v2e_middle_map[cur] = std::vector<heid_t>(heids, heids + he_sz);
            }

            // get hyperedges by input/output known eid
            for(int i = 0; i < nrows; i++) {
                // input known eids
                for (int j = 0; j < known_cols.size(); j++) {
                    sid_t cur = res.get_row_col(i, known_cols[j]);
                    if (PARTITION(cur) != sid || v2e_middle_map.find(cur) != v2e_middle_map.end()) continue;
                    heids = graph->get_heids_by_vertex_and_type(tid, cur, he_type, he_sz);
                    v2e_middle_map[cur] = std::vector<heid_t>(heids, heids + he_sz);
                }

                // output known eids
                if (end_col != NO_RESULT_COL) {
                    sid_t cur = res.get_row_col(i, end_col);
                    if (PARTITION(cur) != sid || v2e_middle_map.find(cur) != v2e_middle_map.end()) continue;
                    heids = graph->get_heids_by_vertex_and_type(tid, cur, he_type, he_sz);
                    v2e_middle_map[cur] = std::vector<heid_t>(heids, heids + he_sz);
                }
            }

            // if unknown, get candidates
            if (end_col == NO_RESULT_COL) {
                vids = graph->get_vids_by_htype(tid, he_type, vid_sz);
                res.candidates.assign(vids, vids + vid_sz);

                for (int j = 0; j < vid_sz; j++) {
                    sid_t cur = vids[j];
                    if (PARTITION(cur) != sid || v2e_middle_map.find(cur) != v2e_middle_map.end()) continue;
                    heids = graph->get_heids_by_vertex_and_type(tid, cur, he_type, he_sz);
                    v2e_middle_map[cur] = std::vector<heid_t>(heids, heids + he_sz);
                }
            }

            // set pstate
            state = HyperQuery::HP_STEP_MATCH;
            return;
        } else if (state == HyperQuery::HP_STEP_MATCH) {
            // do the second step: intersect all hids sets from different nodes

            if (end_col == NO_RESULT_COL) {     // to-unknown
                // iterate through each candidate hyper edge
                for (auto &&cand : res.candidates) {
                    sid_t candidate = (sid_t)cand;
                    auto curr_he = get_vh_from_map(candidate);

                    // check if intersect with each const hyperedge
                    bool flag = true;
                    for (auto &&vid : input_vids) {
                        auto const_he = get_vh_from_map(vid);
                        if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                    }
                    if (!flag) continue;
                    else if (input_vars.empty()) {
                        updated_result.vid_res_table.result_data.push_back(candidate);
                        continue;
                    }

                    // check if intersect with each var vid
                    for (size_t r = 0; r < nrows; r++) {
                        flag = true;
                        for (int j = 0; j < known_cols.size(); j++) {
                            sid_t cur = res.get_row_col(r, known_cols[j]);
                            auto known_he = get_vh_from_map(cur);
                            if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                        }
                        if (!flag) continue;
                        
                        // if valid, put the origin row and new vid into res_he
                        res.append_res_table_row_to(r, updated_result);
                        updated_result.vid_res_table.result_data.push_back(candidate);
                    }
                }

                // update result data and meta
                res.load_data(updated_result);
                res.add_var2col(end, res.get_col_num(SID_t), SID_t);
                res.set_col_num(res.get_col_num(SID_t) + 1, SID_t);
            } else {                            // to-known
                for(int i = 0; i < nrows; i++) {
                    sid_t curr_end = res.get_row_col(i, end_col);
                    auto curr_he = get_vh_from_map(curr_end);

                    // check if intersect with each const hyperedge
                    bool flag = true;
                    for (auto &&vid : input_vids) {
                        auto const_he = get_vh_from_map(vid);
                        if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                    }
                    if (!flag) continue;

                    // check if intersect with each var vid
                    for (int j = 0; j < known_cols.size(); j++) {
                        sid_t cur = res.get_row_col(i, known_cols[j]);
                        auto known_he = get_vh_from_map(cur);
                        if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                    }
                    if (!flag) continue;
                        
                    // if valid, put the origin row and new vid into res_he
                    res.append_res_table_row_to(i, updated_result);
                }

                // update result data
                res.load_data(updated_result);
            }
        } else {ASSERT(false);}

    done:
        // update result meta
        res.update_nrows();
        // increase pattern step
        query.advance_step();
    }

    void op_get_e2e_distributed(HyperQuery& query, HyperQuery::Pattern& op) {
        logstream(LOG_DEBUG) << "Execute E2E op:" << LOG_endl;
        
        // valid E2V op: 
        //      1. E2E_ITSCT: multi const/known/const+known var -> single Unknown var
        //          - mandatory first param "etype": Specify target hyperedge type
        //          - mandatory second param "ge"/"gt"/...: Specify intersect condition
        //      2. E2E_ITSCT: multi const/known/const+known var -> single known var
        //          - mandatory param "ge"/"gt"/...: Specify intersect condition
        //      3. E2E_IN/E2E_CT: multi const/known/const+known var -> single Unknown var
        //          - mandatory param "etype": Specify target hyperedge type
        //      4. E2E_IN/E2E_CT: multi const/known/const+known var -> single known var

        HyperQuery::HPState& state = query.pstate;
        HyperQuery::PatternType &type = op.type;
        std::vector<sid_t> &input_vids = op.input_vids;
        std::vector<ssid_t> &input_vars = op.input_vars;
        std::vector<heid_t> &input_eids = op.input_eids;
        HyperQuery::Result &res = query.result;
        HyperQuery::ResultTable<heid_t> &res_he = res.heid_res_table;
        auto& e2v_middle_map = res.e2v_middle_map;
        int nrows = res.get_row_num();

        // check if the pattern is valid
        ASSERT_ERROR_CODE(input_vids.empty(), VERTEX_INVALID);
        std::vector<int> known_cols;
        for (auto &&input : op.input_vars) {
            // all the vars in input_vars should be known
            int col = res.var2col(input);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, VERTEX_INVALID);
            known_cols.push_back(col);
        }
        ssid_t &end = op.output_var;
        int end_col = res.var2col(end);
        sid_t he_type = 0;
        if (end_col == NO_RESULT_COL) {
            if (input_vars.empty()) ASSERT_ERROR_CODE(res.empty() && query.pattern_step == 0, FIRST_PATTERN_ERROR);
            ASSERT_ERROR_CODE((type == HyperQuery::PatternType::E2E_ITSCT && op.params.size() == 2) ||      // params: he_type + k
                            (type != HyperQuery::PatternType::E2E_ITSCT && op.params.size() == 1), PARAMETER_INVALID);  // params: he_type
            ASSERT_ERROR_CODE(op.params[0].type == SID_t && is_htid(op.params[0].sid), PARAMETER_INVALID);
            if (type == HyperQuery::PatternType::E2E_ITSCT) ASSERT_ERROR_CODE(op.params[1].type == INT_t, PARAMETER_INVALID);
            he_type = op.params[0].sid;
        } else {
            ASSERT_ERROR_CODE((type == HyperQuery::PatternType::E2E_ITSCT && op.params.size() == 1) ||      // params: k
                            (type != HyperQuery::PatternType::E2E_ITSCT && op.params.empty()), PARAMETER_INVALID);  // params: none
            if (type == HyperQuery::PatternType::E2E_ITSCT) ASSERT_ERROR_CODE(op.params[0].type == INT_t, PARAMETER_INVALID);
        }

        // valid check for intersect/in/contain operation
        auto valid_hes = [&](std::set<sid_t> &input, std::set<sid_t> &output) -> bool {
            switch (type)
            {
            case HyperQuery::PatternType::E2E_ITSCT:
            {
                int param_idx = end_col == NO_RESULT_COL? 1: 0;
                HyperQuery::ParamType &p_type = op.params[param_idx].p_type;
                int &limit = op.params[param_idx].num;
                int intersect_factor = intersect_set_num(input, output);
                switch (p_type)
                {
                case HyperQuery::P_EQ:
                    return (intersect_factor == limit);
                case HyperQuery::P_NE:
                    return (intersect_factor != limit);
                case HyperQuery::P_LT:
                    return (intersect_factor < limit);
                case HyperQuery::P_GT:
                    return (intersect_factor > limit);
                case HyperQuery::P_LE:
                    return (intersect_factor <= limit);
                case HyperQuery::P_GE:
                    return (intersect_factor >= limit);
                default:
                    logstream(LOG_ERROR) << "error parameter type!" << LOG_endl;
                    ASSERT(false);
                }
            }
            case HyperQuery::PatternType::E2E_CT:
                return (contain_set(input, output));
            case HyperQuery::PatternType::E2E_IN:
                return (contain_set(output, input));
            default: 
                logstream(LOG_ERROR) << "error pattern type!" << LOG_endl;
                ASSERT(false);
            }
        };

        // get the set of vids by eid from e2v_middle_map
        auto get_he_from_map = [&](heid_t &eid) -> std::set<sid_t>{
            auto map_res = e2v_middle_map.find(eid);
            ASSERT(map_res != e2v_middle_map.end());
            return std::set<sid_t>(map_res->second.begin(), map_res->second.end());
        };

        uint64_t he_sz, vid_sz;
        sid_t* vids;
        heid_t* heids;
        HyperQuery::Result updated_result;

        // TODO: optimize single K2U
        if (input_eids.size() == 1 && op.input_vars.empty()) {      // single-const-to-unknown/known
            // the const hyperedge should be in e2v_middle_map already
            std::set<sid_t> const_he = get_he_from_map(input_eids[0]);

            if (end_col == NO_RESULT_COL) {     // single-const-to-unknown
                // get all candidate hyperedges by hyper type
                heids = graph->get_heids_by_type(tid, he_type, he_sz);

                // iterate through each candidate hyper edge
                for (size_t i = 0; i < he_sz; i++) {            
                    vids = graph->get_edge_by_heid(tid, heids[i], vid_sz);
                    std::set<sid_t> curr_he(vids, vids + vid_sz);
                    if (valid_hes(const_he, curr_he)) 
                        res.heid_res_table.result_data.push_back(heids[i]);
                }

                // update result meta
                res.add_var2col(end, res.get_col_num(HEID_t), HEID_t);
                res.set_col_num(res.get_col_num(HEID_t) + 1, HEID_t);
            } else {                            // single-const-to-known
                for(int i = 0; i < nrows; i++) {
                    // get output known eids
                    heid_t cur = res.get_row_col_he(i, end_col);
                    vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                    std::set<sid_t> curr_he(vids, vids + vid_sz);

                    // if valid, put current row into res_he
                    if (valid_hes(const_he, curr_he)) res.append_res_table_row_to(i, updated_result);
                }

                // update result data
                res.load_data(updated_result);
            }
        } else if (state == HyperQuery::HP_STEP_GET) {
            // do the first step: get as many hyperedges from local kv as possible
            
            // get hyperedges by input const eid
            for(int i = 0; i < input_eids.size(); i++) {
                heid_t cur = input_eids[i];
                if (PARTITION(cur) != sid) continue;
                vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                e2v_middle_map[cur] = std::vector<sid_t>(vids, vids + vid_sz);
            }

            // get hyperedges by input/output known eid
            for(int i = 0; i < nrows; i++) {
                // input known eids
                for (int j = 0; j < known_cols.size(); j++) {
                    heid_t cur = res.get_row_col_he(i, known_cols[j]);
                    if (PARTITION(cur) != sid || e2v_middle_map.find(cur) != e2v_middle_map.end()) continue;
                    vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                    e2v_middle_map[cur] = std::vector<sid_t>(vids, vids + vid_sz);
                }

                // output known eids
                if (end_col != NO_RESULT_COL) {
                    heid_t cur = res.get_row_col_he(i, end_col);
                    if (PARTITION(cur) != sid || e2v_middle_map.find(cur) != e2v_middle_map.end()) continue;
                    vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                    e2v_middle_map[cur] = std::vector<sid_t>(vids, vids + vid_sz);
                }
            }

            // if unknown, get candidates
            if (end_col == NO_RESULT_COL) {
                heids = graph->get_heids_by_type(tid, he_type, he_sz);
                res.candidates.assign(heids, heids + he_sz);

                for (int j = 0; j < he_sz; j++) {
                    heid_t cur = heids[j];
                    if (PARTITION(cur) != sid || e2v_middle_map.find(cur) != e2v_middle_map.end()) continue;
                    vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                    e2v_middle_map[cur] = std::vector<sid_t>(vids, vids + vid_sz);
                }
            }

            // set pstate
            state = HyperQuery::HP_STEP_MATCH;
            return;
        } else if (state == HyperQuery::HP_STEP_MATCH) {
            // do the second step: intersect all hids sets from different nodes

            if (end_col == NO_RESULT_COL) {     // to-unknown
                // iterate through each candidate hyper edge
                for (auto &&candidate : res.candidates) {
                    auto curr_he = get_he_from_map(candidate);

                    // check if intersect with each const hyperedge
                    bool flag = true;
                    for (auto &&eid : input_eids) {
                        auto const_he = get_he_from_map(eid);
                        if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                    }
                    if (!flag) continue;
                    else if (input_vars.empty()) {
                        updated_result.heid_res_table.result_data.push_back(candidate);
                        continue;
                    }

                    // check if intersect with each var vid
                    for (size_t r = 0; r < nrows; r++) {
                        flag = true;
                        for (int j = 0; j < known_cols.size(); j++) {
                            heid_t cur = res.get_row_col_he(r, known_cols[j]);
                            auto known_he = get_he_from_map(cur);
                            if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                        }
                        if (!flag) continue;
                        
                        // if valid, put the origin row and new vid into res_he
                        res.append_res_table_row_to(r, updated_result);
                        updated_result.heid_res_table.result_data.push_back(candidate);
                    }
                }

                // update result data and meta
                res.load_data(updated_result);
                res.add_var2col(end, res.get_col_num(HEID_t), HEID_t);
                res.set_col_num(res.get_col_num(HEID_t) + 1, HEID_t);
            } else {                            // to-known
                for(int i = 0; i < nrows; i++) {
                    heid_t curr_end = res.get_row_col_he(i, end_col);
                    auto curr_he = get_he_from_map(curr_end);

                    // check if intersect with each const hyperedge
                    bool flag = true;
                    for (auto &&eid : input_eids) {
                        auto const_he = get_he_from_map(eid);
                        if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                    }
                    if (!flag) continue;

                    // check if intersect with each var vid
                    for (int j = 0; j < known_cols.size(); j++) {
                        heid_t cur = res.get_row_col_he(i, known_cols[j]);
                        auto known_he = get_he_from_map(cur);
                        if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                    }
                    if (!flag) continue;
                        
                    // if valid, put the origin row and new vid into res_he
                    res.append_res_table_row_to(i, updated_result);
                }

                // update result data
                res.load_data(updated_result);
            }
        } else {ASSERT(false);}

    done:
        // update result meta
        res.update_nrows();
        // increase pattern step
        query.advance_step();
    }

    // generate sub requests for certain servers
    // return pairs of (dst_sid, sub_query)
    void generate_sub_query(HyperQuery &query, std::vector<std::pair<int, HyperQuery>>& sub_queries) {
        // check pstate
        ASSERT_EQ(query.pstate, HyperQuery::HP_STEP_GET);
        
        HyperQuery::Pattern& op = query.get_pattern();
        sub_queries.reserve(Global::num_servers);
        HyperQuery sub_query = query;
        sub_query.forked = true;
        sub_query.pqid = query.qid;
        sub_query.qid = -1;
        uint64_t sz;

        switch(op.type){
        case HyperQuery::PatternType::GE:
        {
            // single-const-to-known: generate sub-query for all node
            for (int i = 0; i < Global::num_servers; i++)
                sub_queries.push_back(std::make_pair(i, sub_query));
        }
        case HyperQuery::PatternType::E2V:
        {
            // multi-const-to-unknown: pick related node by const
            // const+known/known-to-unknown: generate sub-query for all node

            if (op.input_vars.empty()) {
                // multi-const-to-unknown: pick related node by const
                std::vector<bool> hit_server(Global::num_servers, false);
                for (auto &&eid : op.input_eids) hit_server[PARTITION(eid)] = true;
                for (int i = 0; i < Global::num_servers; i++)
                    if (hit_server[i]) sub_queries.push_back(std::make_pair(i, sub_query));
            } 
            else {
                // else: generate sub-query for all node
                for (int i = 0; i < Global::num_servers; i++)
                    sub_queries.push_back(std::make_pair(i, sub_query));
            }
            break;
        }
        case HyperQuery::PatternType::V2E:
        {
            // multi-const-to-unknown: pick related node by const
            // const+known/known-to-unknown: generate sub-query for all node

            if (op.input_vars.empty()) {
                // multi-const-to-unknown: pick related node by const
                std::vector<bool> hit_server(Global::num_servers, false);
                for (auto &&eid : op.input_vids) hit_server[PARTITION(eid)] = true;
                for (int i = 0; i < Global::num_servers; i++)
                    if (hit_server[i]) sub_queries.push_back(std::make_pair(i, sub_query));
            }
            else {
                // else: generate sub-query for all node
                for (int i = 0; i < Global::num_servers; i++)
                    sub_queries.push_back(std::make_pair(i, sub_query));
            }
            break;
        }
        case HyperQuery::PatternType::V2V:
        {
            // single-const-to-unknown/known: get const first
            // all: generate sub-query for all node

            if (op.input_vars.empty() && op.input_vids.size() == 1) {
                // single-const-to-unknown/known: get const first
                ASSERT_ERROR_CODE(!op.params.empty() && op.params[0].type == SID_t && is_htid(op.params[0].sid), PARAMETER_INVALID);
                sid_t &edge_type = op.params[0].sid;
                heid_t* eids = graph->get_heids_by_vertex_and_type(tid, op.input_vids[0], edge_type, sz);
                sub_query.result.v2e_middle_map[op.input_vids[0]] = std::vector<heid_t>(eids, eids + sz);
            }

            // all: generate sub-query for all node
            for (int i = 0; i < Global::num_servers; i++)
                sub_queries.push_back(std::make_pair(i, sub_query));
            break;
        }
        case HyperQuery::PatternType::E2E_ITSCT:
        case HyperQuery::PatternType::E2E_CT:
        case HyperQuery::PatternType::E2E_IN:
        {
            // single-const-to-unknown/known: get const first
            // all: generate sub-query for all node

            if (op.input_vars.empty() && op.input_eids.size() == 1) {
                // single-const-to-unknown/known: get const first
                sid_t* vids = graph->get_edge_by_heid(tid, op.input_eids[0], sz);
                sub_query.result.e2v_middle_map[op.input_eids[0]] = std::vector<sid_t>(vids, vids + sz);
            }

            // all: generate sub-query for all node
            for (int i = 0; i < Global::num_servers; i++)
                sub_queries.push_back(std::make_pair(i, sub_query));
            break;
        }
        default:
            assert(false);
        }

        logstream(LOG_DEBUG) << "[" << sid << "-" << tid << "] "
                        << sub_queries.size() << " sub queies for "
                        << "Q(pqid=" << query.pqid << ", qid=" << query.qid
                        << ", step=" << query.pattern_step << ")"
                        << LOG_endl;
    }

    // determine fork-join or in-place execution
    bool need_fork_join(HyperQuery &query) {
        // always need NOT fork-join when executing on single machine
        if (Global::num_servers == 1 || query.forked || query.pstate == HyperQuery::HP_STEP_MATCH) return false;

        HyperQuery::Result& res = query.result;
        HyperQuery::Pattern& op = query.get_pattern();
        switch(op.type){
        case HyperQuery::PatternType::GV:
        case HyperQuery::PatternType::GE:
        {
            // single-const-to-known
            int col = res.var2col(op.input_vars[0]);
            return (col != NO_RESULT_COL);
        }
        case HyperQuery::PatternType::E2V:
            // not single-const-to-unknown
            return !(op.input_eids.size() == 1 && op.input_vars.empty());
        case HyperQuery::PatternType::V2E:
            // not single-const-to-unknown
            return !(op.input_vids.size() == 1 && op.input_vars.empty());
        case HyperQuery::PatternType::V2V:
        case HyperQuery::PatternType::E2E_ITSCT:
        case HyperQuery::PatternType::E2E_CT:
        case HyperQuery::PatternType::E2E_IN:
            return true;
        default:
            assert(false);
        }

        return false;
    }

    // deal with global op (Eg., V(), E())
    bool dispatch(HyperQuery &query, bool is_start=true) {
        if (Global::num_servers * query.mt_factor == 1) return false;

        // first pattern need dispatch, to all servers and multi-threads threads
        HyperQuery::Pattern& op = query.get_pattern();
        if (is_start && QUERY_FROM_PROXY(query) && query.pattern_step == 0 && query.start_from_index() ) { 
            logstream(LOG_DEBUG) << "[" << sid << "-" << tid << "] dispatch "
                                 << "Q(qid=" << query.qid << ", pqid=" << query.pqid
                                 << ", step=" << query.pattern_step << ")" << LOG_endl;
            rmap.put_parent_request(query, Global::num_servers * query.mt_factor);

            HyperQuery sub_query = query;
            for (int i = 0; i < Global::num_servers; i++) {
                for (int j = 0; j < query.mt_factor; j++) {
                    sub_query.pqid = query.qid;
                    sub_query.qid = -1;
                    sub_query.mt_tid = j;

                    int dst_tid = Global::num_proxies
                                  + (tid + j + 1 - Global::num_proxies) % Global::num_engines;

                    Bundle bundle(sub_query);
                    msgr->send_msg(bundle, i, dst_tid);
                }
            }
            return true;
        } 

        return false;
    }

    void execute_one_op(HyperQuery& query) {
        HyperQuery::Pattern& op = query.get_pattern();
        switch(op.type){
        case HyperQuery::PatternType::GE_TYPE:
            op_get_edge_types(query, op);
            break;
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
            if (Global::num_servers == 1) op_get_v2v(query, op);
            else op_get_v2v_distributed(query, op);
            break;
        case HyperQuery::PatternType::E2E_ITSCT:
        case HyperQuery::PatternType::E2E_CT:
        case HyperQuery::PatternType::E2E_IN:
            if (Global::num_servers == 1) op_get_e2e(query, op);
            else op_get_e2e_distributed(query, op);
            break;
        default:
            assert(false);
        }
    }

    bool execute_ops(HyperQuery &query) {
        uint64_t start, end;
        logstream(LOG_DEBUG) << "[" << sid << "-" << tid << "] execute ops of "
                             << "Q(pqid=" << query.pqid << ", qid=" << query.qid
                             << ", step=" << query.pattern_step
                             << ", state=" << query.state << ")"
                             << " #cols = " << query.result.get_col_num()
                             << " #rows = " << query.result.get_row_num()
                             << " #v2e = " << query.result.v2e_middle_map.size()
                             << " #e2v = " << query.result.e2v_middle_map.size()
                             << " #cand = " << query.result.candidates.size()
                             << LOG_endl;
        do {
            if (need_fork_join(query)) {
                std::vector<std::pair<int, HyperQuery>> sub_queries;
                generate_sub_query(query, sub_queries);
                rmap.put_parent_request(query, sub_queries.size());
                for (int i = 0; i < sub_queries.size(); i++) {
                    // TODO: pick a random dst engine
                    int dst_tid = tid;
                    // int dst_tid = coder->get_random() % Global::num_engines + Global::num_proxies;
                    Bundle bundle(sub_queries[i].second);
                    msgr->send_msg(bundle, sub_queries[i].first, dst_tid);
                }
                return false; // outstanding
            }

            start = timer::get_usec();
            execute_one_op(query);
            end = timer::get_usec();
            query.result.step_latency.push_back(end - start);
            logstream(LOG_DEBUG) << "[" << sid << "-" << tid << "]"
                                 << " step = " << query.pattern_step
                                 << " exec-time = " << (end - start) << " usec"
                                 << " #cols = " << query.result.get_col_num()
                                 << " #rows = " << query.result.get_row_num()
                                 << " #e2v = " << query.result.e2v_middle_map.size()
                                 << " #v2e = " << query.result.v2e_middle_map.size()
                                 << LOG_endl;

            // print step result
            // logstream(LOG_INFO) << query.pattern_step - 1 << ": "
            //             << query.result.get_row_num() << " row, "
            //             << query.result.get_col_num() << " col" 
            //             << LOG_endl;

            if (query.pstate == HyperQuery::HP_STEP_MATCH) {
                query.state = HyperQuery::SQ_REPLY;
                Bundle bundle(query);
                msgr->send_msg(bundle, coder->sid_of(query.pqid), coder->tid_of(query.pqid), true);
                return false;
            }

            // if result row = 0 after one pattern, just skip the rest patterns
            if (query.result.get_row_num() == 0)
                query.pattern_step = query.pattern_group.patterns.size();

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
        // static int count = 0;
        // if (count++ > 100) exit(0);
        logstream(LOG_DEBUG) << "[" << sid << "-" << tid << "]"
                        << " get HyperQuery"
                        << "[ QID=" << query.qid 
                        << " | PQID=" << query.pqid 
                        << " | STATE=" << query.state 
                        << " | STEP=" << query.pattern_step
                        << " | PSTATE=" << query.pstate << " ]"
                        << query.result.get_row_num() << " rows, " << query.result.get_col_num() << " cols."
                        << LOG_endl;

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
            if (!query.done(HyperQuery::SQState::SQ_PATTERN)) {
                // exploit parallelism (multi-server and multi-threading)
                if (dispatch(query)) return;  // async waiting reply by rmap

                if (!execute_ops(query)) return;  // outstanding
            }

        } catch (const char *msg) {
            query.result.set_status_code(UNKNOWN_ERROR);
        } catch (WukongException &ex) {
            query.result.set_status_code(ex.code());
        }
        // 6. Reply
        query.shrink();
        query.state = HyperQuery::SQState::SQ_REPLY;
        Bundle bundle(query);
        msgr->send_msg(bundle, coder->sid_of(query.pqid), coder->tid_of(query.pqid), true);
    }

};

} // namespace wukong
