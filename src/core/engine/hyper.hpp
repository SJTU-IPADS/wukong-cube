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
        logstream(LOG_DEBUG) << "Execute GV op:" << LOG_endl;

        // TODO: right now we don's support vertice type
        ASSERT(false);

        // MUST be the first triple pattern
        ASSERT_ERROR_CODE(query.result.empty(), FIRST_PATTERN_ERROR);

        // check if element and parameters valid
        ASSERT_ERROR_CODE(op.params.size() == 1, PARAMETER_INVALID);
        ASSERT_ERROR_CODE(op.params[0].type == SID_t, PARAMETER_INVALID);

        sid_t type_id = op.params[0].sid;
        ssid_t end = op.output_var;
        HyperQuery::Result& res = query.result;

        uint64_t sz = 0;
        edge_t* vids = graph->get_index(tid, type_id, IN, sz);
        std::vector<sid_t> updated_result_table;
        for(uint64_t k = 0; k < sz; k++)
            updated_result_table.push_back(vids[k].val);

        // update result and metadata
        res.vid_res_table.load_data(updated_result_table);
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
        HyperQuery::Result& res = query.result;
        ssid_t& end = op.output_var;
        std::vector<int> known_cols;
        for (auto &&var : op.input_vars) {
            int col = res.var2col(var);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, UNKNOWN_PATTERN);   
            known_cols.push_back(col);
        }

        // multi-const-to-unknown
        if(op.input_vars.empty()) {
            // MUST be the first triple pattern
            ASSERT_ERROR_CODE(res.empty(), FIRST_PATTERN_ERROR);
            ASSERT_ERROR_CODE(!op.input_eids.empty(), UNKNOWN_PATTERN);
            heid_t start = op.input_eids[0];

            // match the first const vid
            uint64_t vid_sz = 0;
            sid_t* vids = graph->get_edge_by_heid(tid, start, vid_sz);

            // single-const
            if (op.input_eids.size() == 1) {
                res.vid_res_table.result_data.assign(vids, vids + vid_sz);
            } 
            // multi-const
            else {
                std::set<sid_t> start_vids(vids, vids + vid_sz);
                for(int i = 1; i < op.input_eids.size(); i++) {
                    vids = graph->get_edge_by_heid(tid, op.input_eids[i], vid_sz);
                    intersect_set_v2(start_vids, vids, vid_sz);
                }
            
                // update result data
                res.vid_res_table.result_data.assign(start_vids.begin(), start_vids.end());
            }
        }
        // multi-known-to-unknown
        else if (op.input_eids.empty()) {
            HyperQuery::Result updated_result;
            int start_col = known_cols[0];
            heid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            sid_t* vids = NULL, *cached_vids = NULL;
            uint64_t vid_sz = 0, cached_vid_sz = 0;
            std::set<sid_t> start_vids;
            int nrows = res.get_row_num();

            for(int i = 0; i < nrows; i++) {
                heid_t start = res.get_row_col_he(i, start_col);

                // cache the first known eid
                if (start != cached) { // new KNOWN
                    cached = start;
                    cached_vids = graph->get_edge_by_heid(tid, start, cached_vid_sz);
                }
                vids = cached_vids;
                vid_sz = cached_vid_sz;

                // single-known-to-unknown
                if (op.input_eids.size() == 1) {
                    for (int j = 0; j < vid_sz; j++) {
                        res.append_res_table_row_to(i, updated_result);
                        updated_result.vid_res_table.result_data.push_back(vids[j]);   
                    }
                }
                // multi-known-to-unknown
                else {
                    // match the first known eid
                    start_vids.clear();
                    start_vids.insert(vids, vids + vid_sz);

                    // match the rest known eids
                    for (int j = 1; j < known_cols.size(); j++) {
                        heid_t cur = res.get_row_col_he(i, j);
                        vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                        intersect_set_v2(start_vids, vids, vid_sz);
                    }
                    
                    // update result data
                    for (auto &&vid : start_vids) {
                        res.append_res_table_row_to(i, updated_result);
                        updated_result.vid_res_table.result_data.push_back(vid);   
                    }
                }
            }

            // update result data
            res.load_data(updated_result);
        }
        // multi-const+known-to-unknown
        else {
            HyperQuery::Result updated_result;
            heid_t start = op.input_eids[0];

            // match const eids
            uint64_t vid_sz = 0;
            sid_t* vids = graph->get_edge_by_heid(tid, start, vid_sz);
            std::set<sid_t> start_vids(vids, vids + vid_sz);
            for(int i = 1; i < op.input_eids.size(); i++) {
                vids = graph->get_edge_by_heid(tid, op.input_eids[i], vid_sz);
                intersect_set_v2(start_vids, vids, vid_sz);
            }

            // match known eids
            // heid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            int nrows = res.get_row_num();
            for(int i = 0; i < nrows; i++) {
                std::set<sid_t> cur_vids = start_vids;

                // match the rest known eids
                for (int j = 0; j < known_cols.size(); j++) {
                    heid_t cur = res.get_row_col_he(i, known_cols[j]);
                    vids = graph->get_edge_by_heid(tid, cur, vid_sz);
                    intersect_set_v2(cur_vids, vids, vid_sz);   
                }
                
                // update result data
                for (auto &&vid : cur_vids) {
                    res.append_res_table_row_to(i, updated_result);
                    updated_result.vid_res_table.result_data.push_back(vid);   
                }
            }
            
            // update result data
            res.load_data(updated_result);
        }

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
        HyperQuery::Result& res = query.result;
        ssid_t& end = op.output_var;
        ASSERT_ERROR_CODE(op.params.size() == 1 && op.params[0].p_type == HyperQuery::P_ETYPE, PARAMETER_INVALID);
        sid_t edge_type = op.params[0].sid;
        std::vector<int> known_cols;
        for (auto &&var : op.input_vars) {
            int col = res.var2col(var);
            ASSERT_ERROR_CODE(col != NO_RESULT_COL, UNKNOWN_PATTERN);   
            known_cols.push_back(col);
        }

        // multi-const-to-unknown
        if(op.input_vars.empty()) {
            // MUST be the first triple pattern
            ASSERT_ERROR_CODE(res.empty(), FIRST_PATTERN_ERROR);
            ASSERT_ERROR_CODE(!op.input_vids.empty(), UNKNOWN_PATTERN);
            sid_t start = op.input_vids[0];

            // match the first const vid
            uint64_t eid_sz = 0;
            heid_t* eids = graph->get_heids_by_vertex_and_type(tid, start, edge_type, eid_sz);

            // single-const
            if (op.input_vids.size() == 1) {
                res.heid_res_table.result_data.assign(eids, eids + eid_sz);
            } 
            // multi-const
            else {
                std::set<heid_t> start_eids(eids, eids + eid_sz);
                for(int i = 1; i < op.input_eids.size(); i++) {
                    eids = graph->get_heids_by_vertex_and_type(tid, op.input_vids[i], edge_type, eid_sz);
                    intersect_set_v2(start_eids, eids, eid_sz);
                }
            
                // update result data
                res.heid_res_table.result_data.assign(start_eids.begin(), start_eids.end());
            }
        }
        // multi-known-to-unknown
        else if (op.input_vids.empty()) {
            HyperQuery::Result updated_result;
            int start_col = known_cols[0];
            sid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            heid_t* eids = NULL, *cached_eids = NULL;
            uint64_t eid_sz = 0, cached_eid_sz = 0;
            std::set<heid_t> start_eids;
            int nrows = res.get_row_num();

            for(int i = 0; i < nrows; i++) {
                sid_t start = res.get_row_col(i, start_col);

                // cache the first known vid
                if (start != cached) { // new KNOWN
                    cached = start;
                    cached_eids = graph->get_heids_by_vertex_and_type(tid, start, edge_type, cached_eid_sz);
                }
                eids = cached_eids;
                eid_sz = cached_eid_sz;

                // single-known-to-unknown
                if (op.input_eids.size() == 1) {
                    for (int j = 0; j < eid_sz; j++) {
                        res.append_res_table_row_to(i, updated_result);
                        updated_result.heid_res_table.result_data.push_back(eids[j]);   
                    }
                }
                // multi-known-to-unknown
                else {
                    // match the first known vid
                    start_eids.clear();
                    start_eids.insert(eids, eids + eid_sz);

                    // match the rest known vid
                    for (int j = 1; j < known_cols.size(); j++) {
                        sid_t cur = res.get_row_col(i, j);
                        eids = graph->get_heids_by_vertex_and_type(tid, cur, edge_type, eid_sz);
                        intersect_set_v2(start_eids, eids, eid_sz);
                    }
                    
                    // update result data
                    for (auto &&heid : start_eids) {
                        res.append_res_table_row_to(i, updated_result);
                        updated_result.heid_res_table.result_data.push_back(heid);   
                    }
                }
            }

            // update result data
            res.load_data(updated_result);
        }
        // multi-const+known-to-unknown
        else {
            HyperQuery::Result updated_result;
            sid_t start = op.input_eids[0];

            // match const vid
            uint64_t eid_sz = 0;
            heid_t* eids = graph->get_heids_by_vertex_and_type(tid, start, edge_type, eid_sz);
            std::set<heid_t> start_eids(eids, eids + eid_sz);
            for(int i = 1; i < op.input_eids.size(); i++) {
                eids = graph->get_heids_by_vertex_and_type(tid, op.input_eids[i], edge_type, eid_sz);
                intersect_set_v2(start_eids, eids, eid_sz);
            }

            // match known vids
            // sid_t cached = BLANK_ID; // simple dedup for consecutive same vertices
            int nrows = res.get_row_num();
            for(int i = 0; i < nrows; i++) {
                std::set<heid_t> cur_eids = start_eids;

                // match the rest known vids
                for (int j = 0; j < known_cols.size(); j++) {
                    sid_t cur = res.get_row_col(i, known_cols[j]);
                    eids = graph->get_heids_by_vertex_and_type(tid, cur, edge_type, eid_sz);
                    intersect_set_v2(cur_eids, eids, eid_sz);   
                }
                
                // update result data
                for (auto &&heid : cur_eids) {
                    res.append_res_table_row_to(i, updated_result);
                    updated_result.heid_res_table.result_data.push_back(heid);   
                }
            }
            
            // update result data
            res.load_data(updated_result);
        }

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
            // get all candidate vids by hyper type
            vids = graph->get_vids_by_htype(tid, he_type, vid_sz);

            // iterate through each hyper edge, and compare them to each const hyperedge
            for (size_t i = 0; i < vid_sz; i++) {
                // get related hyperedges
                heids = graph->get_heids_by_vertex_and_type(tid, vids[i], he_type, he_sz);
                idList<heid_t> curr_he(heids, he_sz);

                // check if intersect with each const vid
                bool flag = true;
                for (auto &&const_he : const_hes)
                    if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                if (!flag) continue;
                else if (input_vars.empty()) {
                    updated_result.vid_res_table.result_data.push_back(vids[i]);
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
                    updated_result.vid_res_table.result_data.push_back(vids[i]);
                }
            }

            // update result data
            res.load_data(updated_result);
            res.add_var2col(op.output_var, res.get_col_num(SID_t), SID_t);
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

            // get all candidate hyperedges by hyper type
            heids = graph->get_heids_by_type(tid, he_type, he_sz);

            // iterate through each hyper edge, and compare them to each const hyperedge
            for (size_t i = 0; i < he_sz; i++) {            
                // get hyperedge content
                vids = graph->get_edge_by_heid(tid, heids[i], vid_sz);
                idList<sid_t> curr_he(vids, vid_sz);

                // check if intersect with each const hyperedge
                bool flag = true;
                for (auto &&const_he : const_hes)
                    if (!valid_hes(const_he, curr_he)) {flag = false; break;}
                if (!flag) continue;
                else if (input_vars.empty()) {
                    updated_result.heid_res_table.result_data.push_back(heids[i]);
                    continue;
                }

                // check if intersect with each var hyperedge
                for (size_t r = 0; r < nrows; r++) {
                    flag = true;

                    // check if intersect with each var hyperedge
                    for (auto &&known_he : known_hes[r])
                        if (!valid_hes(known_he, curr_he)) {flag = false; break;}
                    if (!flag) continue;
                    
                    // if valid, put the origin row and new heid into res_he
                    res.append_res_table_row_to(r, updated_result);
                    updated_result.heid_res_table.result_data.push_back(heids[i]);
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
        logstream(LOG_DEBUG) << "[" << sid << "-" << tid << "] execute ops of "
                             << "Q(pqid=" << query.pqid << ", qid=" << query.qid
                             << ", step=" << query.pattern_step << ")"
                             << " #cols = " << query.result.get_col_num()
                             << " #rows = " << query.result.get_row_num()
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
                                 << " step = " << query.pattern_step
                                 << " exec-time = " << (timer::get_usec() - time) << " usec"
                                 << " #cols = " << query.result.get_col_num()
                                 << " #rows = " << query.result.get_row_num()
                                 << LOG_endl;

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
        msgr->send_msg(bundle, coder->sid_of(query.pqid), coder->tid_of(query.pqid));
    }

};

} // namespace wukong
