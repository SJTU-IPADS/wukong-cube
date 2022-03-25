/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
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

#include <boost/unordered_map.hpp>

#include "core/sparql/query.hpp"
#include "core/hyperquery/query.hpp"

namespace wukong {

// The map is used to collect replies from sub_queries in fork-join execution mode
class RMap {
private:
    struct Item {
        int cnt; // #sub-queries
        SPARQLQuery parent;
        SPARQLQuery reply;
    };

    boost::unordered_map<int, Item> internal_map;

public:
    void put_parent_request(SPARQLQuery &r, int cnt) {
        logstream(LOG_DEBUG) << "add parent-qid=" << r.qid
                             << " and #sub-queries=" << cnt << LOG_endl;

        // not exist
        ASSERT(internal_map.find(r.qid) == internal_map.end());

        Item d = { .cnt = cnt, .parent = r, };
        //d.cnt = cnt;
        //d.parent = r;
        internal_map[r.qid] = d;
    }

    void put_reply(SPARQLQuery &r) {
        // exist
        ASSERT(internal_map.find(r.pqid) != internal_map.end());

        Item &d = internal_map[r.pqid];
        SPARQLQuery::Result &whole = d.reply.result;
        SPARQLQuery::Result &part = r.result;
        d.cnt--;

        // if the PatternGroup comes from a query's UNION part,
        // use merge_result to put result
        if (r.pg_type == SPARQLQuery::PGType::UNION)
            whole.merge_result(part);
        else
            whole.append_result(part);


        // NOTE: all sub-jobs have the same pattern_step, optional_step, and union_done
        // update parent's pattern step (progress)
        if (d.parent.state == SPARQLQuery::SQState::SQ_PATTERN)
            d.parent.pattern_step = r.pattern_step;

        // update parent's optional_step (avoid recursive execution)
        if (d.parent.pg_type == SPARQLQuery::PGType::OPTIONAL
                && r.done(SPARQLQuery::SQState::SQ_OPTIONAL))
            d.parent.optional_step = r.optional_step;

        // update parent's union_done (avoid recursive execution)
        if (r.done(SPARQLQuery::SQState::SQ_UNION))
            d.parent.union_done = true;
    }

    bool is_ready(int qid) {
        return internal_map[qid].cnt == 0;
    }

    SPARQLQuery get_reply(int qid) {
        SPARQLQuery r = internal_map[qid].parent;
        SPARQLQuery &reply = internal_map[qid].reply;

        // copy metadata of result
        r.result.row_num = reply.result.row_num;
        r.result.col_num = reply.result.col_num;
        r.result.heid_res_table.col_num = reply.result.heid_res_table.col_num;
        r.result.float_res_table.col_num = reply.result.heid_res_table.col_num;
        r.result.double_res_table.col_num = reply.result.heid_res_table.col_num;
    #ifdef TRDF_MODE
        r.result.time_col_num = reply.result.time_col_num;
    #endif
        r.result.attr_col_num = reply.result.attr_col_num;
        r.result.v2c_map = reply.result.v2c_map;
        // NOTE: no need to set nvars, required_vars, and blind

        // copy data of result
        r.result.result_table.swap(reply.result.result_table);
        r.result.heid_res_table.swap(reply.result.heid_res_table);
        r.result.float_res_table.swap(reply.result.float_res_table);
        r.result.double_res_table.swap(reply.result.double_res_table);
    #ifdef TRDF_MODE
        r.result.time_res_table.swap(reply.result.time_res_table);
    #endif
        r.result.attr_res_table.swap(reply.result.attr_res_table);

        internal_map.erase(qid);
        logstream(LOG_DEBUG) << "erase parent-qid=" << qid << LOG_endl;
        return r;
    }
};

// The map is used to collect replies from sub_queries in fork-join execution mode
class HyperRMap {
private:
    struct Item {
        int cnt; // #sub-queries
        HyperQuery parent;
        HyperQuery reply;
    };

    boost::unordered_map<int, Item> internal_map;

public:
    void put_parent_request(HyperQuery &r, int cnt) {
        logstream(LOG_DEBUG) << "add parent-qid=" << r.qid
                             << " and #sub-queries=" << cnt << LOG_endl;

        // not exist
        ASSERT(internal_map.find(r.qid) == internal_map.end());

        Item d = { .cnt = cnt, .parent = r, };
        //d.cnt = cnt;
        //d.parent = r;
        internal_map[r.qid] = d;
    }

    void put_reply(HyperQuery &r) {
        // exist
        ASSERT(internal_map.find(r.pqid) != internal_map.end());

        Item &d = internal_map[r.pqid];
        HyperQuery::Result &whole = d.reply.result;
        HyperQuery::Result &part = r.result;
        d.cnt--;

        // put part meta
        d.parent.pstate = r.pstate;
        d.parent.pattern_step = r.pattern_step;
        d.parent.forked = r.forked;
        d.parent.result.merge_step_latency(part);
        
        // put part data 
        if (r.pstate == HyperQuery::HP_STEP_GET) {
            d.parent.result.v2c_map = r.result.v2c_map;
            whole.append_result(part);
        } else {
            whole.e2v_middle_map.insert(part.e2v_middle_map.begin(), part.e2v_middle_map.end());
            whole.v2e_middle_map.insert(part.v2e_middle_map.begin(), part.v2e_middle_map.end());
        }
        logstream(LOG_DEBUG) << "put parent-qid=" << r.pqid
                             << " and #sub-qid=" << r.qid
                             << ", cnt = " << d.cnt 
                             << ", e2v_middle = " << whole.e2v_middle_map.size()
                             << ", v2e_middle = " << whole.v2e_middle_map.size()
                             << ", pstate = " << r.pstate
                             << ", forked = " << r.forked
                             << LOG_endl;
    }

   bool is_ready(int qid) {
        return internal_map[qid].cnt == 0; 
    }

    HyperQuery get_reply(int qid) {
        HyperQuery r = internal_map[qid].parent;
        HyperQuery &reply = internal_map[qid].reply;

        if (r.pstate == HyperQuery::HP_STEP_GET) {
            // NOTE: no need to set nvars, required_vars, and blind
            // r.result.v2c_map = reply.result.v2c_map;
            r.result.vid_res_table.col_num = reply.result.vid_res_table.col_num;
            r.result.heid_res_table.col_num = reply.result.heid_res_table.col_num;
            r.result.float_res_table.col_num = reply.result.float_res_table.col_num;
            r.result.double_res_table.col_num = reply.result.double_res_table.col_num;

            // copy data of result
            r.result.load_data(reply.result);
            r.result.update_nrows();
        } else {
            // copy data of middle result
            r.result.e2v_middle_map.swap(reply.result.e2v_middle_map);
            r.result.v2e_middle_map.swap(reply.result.v2e_middle_map);
        }        

        internal_map.erase(qid);
        logstream(LOG_DEBUG) << "erase parent-qid=" << qid << LOG_endl;
        return r;
    }
};


} // namespace wukong