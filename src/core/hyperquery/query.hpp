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

#include <set>
#include <vector>
#include <cstring>
#include <string>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/variant.hpp>
#include <boost/serialization/split_free.hpp>

#include "core/common/errors.hpp"
#include "core/common/type.hpp"
#include "core/common/hypertype.hpp"

#include "core/store/vertex.hpp"

// utils
#include "utils/logger2.hpp"
#include "utils/assertion.hpp"

namespace wukong {

using namespace boost::archive;

// EXT = [ TYPE:16 | COL:16 ]
#define TYPE_BITS   16   // column type
#define COL_BITS    16   // column number

// Init COL value with NO_RESULT_COL
#define NO_RESULT_COL ((1 << COL_BITS) - 1)

/**
 * HYPER Query
 */
class HyperQuery {
private:
    friend class boost::serialization::access;
    // template <typename Archive>
    // void serialize(Archive &ar, const unsigned int version) {
    //     ar & qid;
    //     ar & pqid;
    //     ar & pattern_step;
    //     ar & pattern_group;
    //     ar & result;
    // }

public:
    enum SQState { SQ_PATTERN = 0, SQ_UNION, SQ_FILTER, SQ_OPTIONAL, SQ_FINAL, SQ_REPLY };
    enum HPState { HP_STEP_GET = 0, HP_STEP_MATCH};
    enum PatternType { GV, GE, GP, V2E, E2V, E2E_ITSCT, E2E_CT, E2E_IN, V2V, LAST_TYPE};
    enum ParamType { P_ETYPE, P_VTYPE, P_GE, P_LE, P_GT, P_LT, P_EQ, P_NE, NO_TYPE };

    class Param {
        friend class boost::serialization::access;
    
    public:
        ParamType p_type = P_ETYPE;

        data_type type = SID_t; // TODO: remove it
        sid_t sid = 0;
        heid_t heid = 0;
        int num = 0;

        Param() {}
        Param(ParamType p_type, data_type type, sid_t sid): p_type(p_type), type(type), sid(sid) {ASSERT_EQ(type, SID_t);}
        Param(ParamType p_type, data_type type, heid_t heid): p_type(p_type), type(type), heid(heid) {ASSERT_EQ(type, HEID_t);}
        Param(ParamType p_type, data_type type, int num): p_type(p_type), type(type), num(num) {ASSERT_EQ(type, INT_t);}

        void print_param() const {
            static const char *ParamTypeName[9] = { "P_ETYPE", "P_VTYPE", "P_GE", "P_LE", "P_GT", "P_LT", "P_EQ", "P_NE", "NO_TYPE"};
            static const char *ParamDataTypeName[7] = { "SID_t", "HEID_t", "INT_t", "FLOAT_t", "DOUBLE_t", "TIME_t", "ALL_t"};
            
            // TODO: log error            
            logstream(LOG_INFO) << ParamTypeName[p_type] << ": (" << ParamDataTypeName[type] << ")";
            switch(type) {
            case SID_t:
                logstream(LOG_INFO) << sid;
                break;
            case HEID_t:
                logstream(LOG_INFO) << heid;
                break;
            case INT_t:
                logstream(LOG_INFO) << num;
                break;
            }
            logstream(LOG_INFO) << " ";
        }
    };

    // hyper pattern
    class Pattern {
    private:
        friend class boost::serialization::access;
        // template <typename Archive>
        // void serialize(Archive &ar, const unsigned int version) {
        //     ar & type;
        //     ar & input_eids;
        //     ar & input_vids;
        //     ar & output_var;
        //     ar & he_type;
        //     ar & bind_node;
        //     ar & k;
        // }

    public:
        PatternType type;
        
        // multiple input and single output
        std::vector<sid_t> input_vids;
        std::vector<ssid_t> input_vars;
        std::vector<heid_t> input_eids;
        ssid_t output_var;

        // aggreate all parameters in this single pattern into 1 sid_t vector
        std::vector<Param> params;

        // other constraints of the hyper pattern
        // sid_t he_type;
        // sid_t bind_node = 0;
        // uint32_t k = 0;

        Pattern() {}

        Pattern(PatternType type,
                std::vector<sid_t> input_vids,
                std::vector<ssid_t> input_vars,
                std::vector<heid_t> input_eids,
                ssid_t output_var,
                std::vector<Param> params):
                type(type), 
                input_vids(input_vids), 
                input_eids(input_eids), 
                input_vars(input_vars),
                output_var(output_var), 
                params(params) {
            ASSERT_GT(input_vids.size() + input_vars.size() + input_eids.size(), 0);
            ASSERT_LT(output_var, 0);
        }
    
        void print_pattern() const {
            static const char *PatternTypeName[9] = { "GV", "GE", "GP", "V2E", "E2V", "E2E_ITSCT", "E2E_CT", "E2E_IN", "V2V"};

            logstream(LOG_INFO) << "\t[ (vids) ";
            for (auto &&vid : input_vids)
                logstream(LOG_INFO) << vid << " "; 
            logstream(LOG_INFO) << " | (eids) "; 
            for (auto &&eid : input_eids)
                logstream(LOG_INFO) << eid << " "; 
            logstream(LOG_INFO) << " | (vars) "; 
            for (auto &&var : input_vars)
                logstream(LOG_INFO) << var << " "; 
            logstream(LOG_INFO) << " ]\t" << PatternTypeName[type] << " params( ";
            for (auto &&param : params) param.print_param();   
            logstream(LOG_INFO) << " )\t==>\t" <<output_var << LOG_endl; 
        }
    };

    class PatternGroup {
    private:
        friend class boost::serialization::access;
        // template <typename Archive>
        // void serialize(Archive &ar, const unsigned int version) {
        //     ar & patterns;
        // }

    public:
        std::vector<Pattern> patterns;

        void print_group() const {
            logstream(LOG_INFO) << "patterns[" << patterns.size() << "]:" << LOG_endl;
            for (auto const &p : patterns)
                p.print_pattern();
        }

        // used to calculate dst_sid
        ssid_t get_start() {
            if (this->patterns.size() > 0) {
                Pattern &pt = this->patterns[0];
                if (pt.input_eids.size() > 0)
                    return pt.input_eids[0];
                else if (pt.input_vids.size() > 0)
                    return pt.input_vids[0];
                else return BLANK_ID;
            }
            else
                ASSERT_ERROR_CODE(false, UNKNOWN_PATTERN);
            return BLANK_ID;
        }
    };

    template <class DataType> 
    class ResultTable {
    private:
        friend class boost::serialization::access;
        template <typename Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar & col_num;
            if(col_num > 0) {
                ar & result_data;
            }
        }
    public:
        ResultTable() { }

        int col_num = 0;
        std::vector<DataType> result_data;

        void load_data(std::vector<DataType> &update) {
            result_data.swap(update);
        }

        void swap(ResultTable<DataType> &update) {
            result_data.swap(update.result_data);
        }

        void set_col_num(int n) {
            col_num = n;
        }

        int get_col_num() const {
            return col_num;
        }

        int get_data_size() const {
            return result_data.size();
        }

        DataType get_row_col(int r, int c) {
            ASSERT(r >= 0 && c >= 0);
            return result_data[col_num * r + c];
        }

        void append_row_to(int r, std::vector<DataType> &update) {
            for (int c = 0; c < col_num; c++)
                update.push_back(get_row_col(r, c));
        }

        void append_row_to(int r, ResultTable<DataType> &update) {
            for (int c = 0; c < col_num; c++)
                update.result_data.push_back(get_row_col(r, c));
        }

        void dup_rows(ResultTable<DataType> &update){
            result_data.assign(update.result_data.begin(), 
                               update.result_data.end());
        }
        
        void append_result(ResultTable<DataType>& result) {
            col_num = result.col_num;
            result_data.insert(result_data.end(),
                               result.result_data.begin(),
                               result.result_data.end());
        }

        bool empty() {
            return (col_num == 0 && result_data.empty());
        }

        void clear() {
            col_num = 0;
            result_data.clear();
        }

        void print_table() {
            size_t row_num = col_num > 0? (result_data.size() / col_num): 0;
            logstream(LOG_INFO) << "table [ " << col_num << " cols, " << row_num << " rows ]:" << LOG_endl;
            for (size_t r = 0; r < row_num; r++) {
                for (size_t c = 0; c < col_num; c++) 
                    logstream(LOG_INFO) << "\t" << get_row_col(r, c) << "\t";
                logstream(LOG_INFO) << "\n";
            }
        }
    };

    class Result {
    private:
        friend class boost::serialization::access;
        // template <typename Archive>
        // void serialize(Archive &ar, const unsigned int version) {
        //     ar & col_nums;
        //     ar & row_num;
        //     ar & blind;
        //     ar & status_code;
        //     ar & nvars;
        //     ar & required_vars;
        //     ar & v2c_map;
        //     if(get_col_num(TYPE_VERTEX) > 0) {
        //         ar & vid_res_table;
        //     }
        //     if(get_col_num(TYPE_EDGE) > 0) {
        //         ar & heid_res_table;
        //     }
        //     if(get_col_num(TYPE_FLOAT) > 0) {
        //         ar & float_res_table;
        //     }
        //     if(get_col_num(TYPE_DOUBLE) > 0) {
        //         ar & double_res_table;
        //     }
        // }


    public:
        Result() { }

        // defined as constexpr due to switch-case
        constexpr int const_pair(int t1, int t2) { return ((t1 << 4) | t2); }

        enum vstat { KNOWN_VAR = 0, UNKNOWN_VAR, CONST_VAR }; // variable stat

        // conversion between col and ext
        int col2ext(int col, int type) { return ((type << COL_BITS) | col); }
        int ext2col(int ext) { return (ext & ((1 << COL_BITS) - 1)); }
        int ext2type(int ext) { return ((ext >> COL_BITS) & ((1 << COL_BITS) - 1)); }

        // metadata
        int row_num = 0;  // FIXME: vs. get_row_num()
        int attr_col_num = 0; // FIXME: why not no attr_row_num
        int status_code = SUCCESS;

        // middle result of a pattern
        std::map<sid_t, std::vector<heid_t>> v2e_middle_map;
        std::map<heid_t, std::vector<sid_t>> e2v_middle_map;
        std::vector<uint64_t> candidates;   // for e2e and v2v

        // data
        ResultTable<sid_t> vid_res_table;
        ResultTable<heid_t> heid_res_table;
        ResultTable<float> float_res_table;
        ResultTable<double> double_res_table;

        // variables
        bool blind = false;
        int nvars = 0; // the number of variables
        std::vector<ssid_t> required_vars; // variables selected to return
        std::vector<int> v2c_map; // from variable ID (vid) to column ID, index: vid, value: col

        // latency record
        std::vector<uint64_t> step_latency;

        void clear() {
            vid_res_table.clear();
            heid_res_table.clear();
            float_res_table.clear();
            double_res_table.clear();
            required_vars.clear();
            v2c_map.clear();
        }

        bool empty() {
            return vid_res_table.empty() && heid_res_table.empty() &&
                    float_res_table.empty() && double_res_table.empty();
        }

        vstat var_stat(ssid_t var) {
            if (var >= 0)
                return CONST_VAR;
            else if (var2col(var) == NO_RESULT_COL)
                return UNKNOWN_VAR;
            else
                return KNOWN_VAR;
        }

        // add column id to var (pattern variable)
        void add_var2col(ssid_t var, int col, data_type type = SID_t) {
            // number variables from -1 and decrease by 1 for each of rest. (i.e., -1, -2, ...)
            ASSERT(var < 0 && col >= 0);

            // the number of variables is known before calling var2col()
            ASSERT(nvars > 0);
            if (v2c_map.size() == 0)
                v2c_map.resize(nvars, NO_RESULT_COL); // init

            // calculate idx
            int idx = - (var + 1);
            ASSERT(idx < nvars && idx >= 0);

            // variable should not be set
            ASSERT(v2c_map[idx] == NO_RESULT_COL);
            v2c_map[idx] = col2ext(col, type);
        }

        // get column id from var (pattern variable)
        int var2col(ssid_t var) {
            // number variables from -1 and decrease by 1 for each of rest. (i.e., -1, -2, ...)
            ASSERT_ERROR_CODE(var < 0, VERTEX_INVALID);

            // the number of variables is known before calling var2col()
            ASSERT(nvars > 0);
            if (v2c_map.size() == 0)
                v2c_map.resize(nvars, NO_RESULT_COL); // init

            // calculate idx
            int idx = - (var + 1);
            ASSERT_ERROR_CODE(idx < nvars && idx >= 0, VERTEX_INVALID);

            // get col
            return ext2col(v2c_map[idx]);
        }

        // get data type from var (pattern variable)
        data_type var_type(ssid_t var) {
            // number variables from -1 and decrease by 1 for each of rest. (i.e., -1, -2, ...)
            ASSERT(var < 0);

            // the number of variables is known before calling var2col()
            ASSERT(nvars > 0);
            if (v2c_map.size() == 0) // init
                v2c_map.resize(nvars, NO_RESULT_COL);

            // calculate idx
            int idx = - (var + 1);
            ASSERT(idx < nvars);

            // get type
            int type = ext2type(v2c_map[idx]);

            if(type == 0) return SID_t;
            else if(type == 1) return HEID_t;
            else if(type == 2) return INT_t;
            else if(type == 3) return FLOAT_t;
            else if(type == 4) return DOUBLE_t;
            else{ assert(false); }

        }

        int get_col_num(data_type type = ALL_t) {
            switch (type) {
            case SID_t:
                return vid_res_table.get_col_num();
            case HEID_t:
                return heid_res_table.get_col_num();
            case FLOAT_t:
                return float_res_table.get_col_num();
            case DOUBLE_t:
                return double_res_table.get_col_num();
            case ALL_t:
                return vid_res_table.get_col_num() + 
                        heid_res_table.get_col_num() + 
                        float_res_table.get_col_num() + 
                        double_res_table.get_col_num();
            default: ASSERT(false);
            }
        }

        int get_row_num() const {
            return row_num;
            // if(vid_res_table.get_col_num() == 0 && 
            //    heid_res_table.get_col_num() == 0 &&
            //    float_res_table.get_col_num() == 0 &&
            //    double_res_table.get_col_num() == 0) {
            //     return 0;
            // } else if(vid_res_table.get_col_num() != 0) {
            //     return vid_res_table.get_data_size() / vid_res_table.get_col_num();
            // } else if(heid_res_table.get_col_num() != 0) {
            //     return heid_res_table.get_data_size() / heid_res_table.get_col_num();
            // } else if(float_res_table.get_col_num() != 0) {
            //     return float_res_table.get_data_size() / float_res_table.get_col_num();
            // } else if(double_res_table.get_col_num() != 0) {
            //     return double_res_table.get_data_size() / double_res_table.get_col_num();
            // } else { return 0; }
        }

        sid_t get_row_col(int r, int c) {
            ASSERT(r >= 0 && c >= 0);
            return vid_res_table.get_row_col(r, c);
        }

        heid_t get_row_col_he(int r, int c) {
            ASSERT(r >= 0 && c >= 0);
            return heid_res_table.get_row_col(r, c);
        }

        float get_row_col_float(int r, int c) {
            ASSERT(r >= 0 && c >= 0);
            return float_res_table.get_row_col(r, c);
        }

        double get_row_col_double(int r, int c) {
            ASSERT(r >= 0 && c >= 0);
            return double_res_table.get_row_col(r, c);
        }

        // NORMAL result (i.e., sid)
        void set_col_num(int n, data_type type = SID_t) {
            switch (type) {
            case SID_t:
                vid_res_table.set_col_num(n);
                break;
            case HEID_t:
                heid_res_table.set_col_num(n);
                break;
            case FLOAT_t:
                float_res_table.set_col_num(n);
                break;
            case DOUBLE_t:
                double_res_table.set_col_num(n);
                break;
            }
        }

        void update_nrows() {
            if(vid_res_table.get_col_num() == 0 && 
               heid_res_table.get_col_num() == 0 &&
               float_res_table.get_col_num() == 0 &&
               double_res_table.get_col_num() == 0) {
                row_num = 0;
            } else if(vid_res_table.get_col_num() != 0) {
                row_num = vid_res_table.get_data_size() / vid_res_table.get_col_num();
            } else if(heid_res_table.get_col_num() != 0) {
                row_num = heid_res_table.get_data_size() / heid_res_table.get_col_num();
            } else if(float_res_table.get_col_num() != 0) {
                row_num = float_res_table.get_data_size() / float_res_table.get_col_num();
            } else if(double_res_table.get_col_num() != 0) {
                row_num = double_res_table.get_data_size() / double_res_table.get_col_num();
            }
        }

        void load_data(Result& result) {
            vid_res_table.swap(result.vid_res_table);
            heid_res_table.swap(result.heid_res_table);
            float_res_table.swap(result.float_res_table);
            double_res_table.swap(result.double_res_table);
        }

        void append_res_table_row_to(int r, Result& result) {
            vid_res_table.append_row_to(r, result.vid_res_table);
            heid_res_table.append_row_to(r, result.heid_res_table);
            float_res_table.append_row_to(r, result.float_res_table);
            double_res_table.append_row_to(r, result.double_res_table);
        }

        void dup_result_table(Result& result) {
            vid_res_table.dup_rows(result.vid_res_table);
            heid_res_table.dup_rows(result.heid_res_table);
            float_res_table.dup_rows(result.float_res_table);
            double_res_table.dup_rows(result.double_res_table);
        }

        void set_status_code(int code) {
            status_code = code;
        }

        int get_status_code() { return status_code; }

        void append_result(HyperQuery::Result &r) {
            /// update metadata (i.e., v2c_map, ncols, attr_ncols, and nrows)
            // NOTE: all sub-jobs have the same v2c_map, ncols, and attr_ncols
            v2c_map = r.v2c_map;

            // update date
            vid_res_table.append_result(r.vid_res_table);
            heid_res_table.append_result(r.heid_res_table);
            float_res_table.append_result(r.float_res_table);
            double_res_table.append_result(r.double_res_table);

            update_nrows();
        }
    };

    int qid = -1;   // query id (track engine (sid, tid))
    int pqid = -1;  // parent qid (track the source (proxy or parent query) of query)

    SQState state = SQ_PATTERN;
    HPState pstate = HP_STEP_GET;

    int priority = 0;

    int mt_factor = 1;  // use a single engine (thread) by default
    int mt_tid = 0;     // engine thread number (MT)

    // Pattern
    int pattern_step = 0;
    bool forked = false;    //deciding fork or not
    // ssid_t local_var = 0;   // the local variable

    int limit = -1;
    unsigned offset = 0;
    bool distinct = false;

    // PatternGroup
    PatternGroup pattern_group;
    Result result;

    HyperQuery() { }

    // build a query by existing query template
    HyperQuery(PatternGroup g, int nvars, std::vector<ssid_t> &required_vars)
        : pattern_group(g) {
        result.nvars = nvars;
        result.required_vars = required_vars;
        result.v2c_map.resize(nvars, NO_RESULT_COL);
    }

    // return the current pattern
    Pattern & get_pattern() {
        ASSERT(pattern_step < pattern_group.patterns.size());
        return pattern_group.patterns[pattern_step];
    }

    // return a specific pattern
    Pattern & get_pattern(int step) {
        ASSERT(step < pattern_group.patterns.size());
        return pattern_group.patterns[step];
    }

    void advance_step() {
        this->pattern_step++;
        this->pstate = HP_STEP_GET;
        this->forked = false;

        // clear middle result
        result.candidates.clear();
    }

    int get_pattern_step() {
        return this->pattern_step;
    }

    // shrink the query to reduce communication cost (before sending)
    void shrink() {
        pattern_group.patterns.clear();

        // discard results if does not care
        if (result.blind)
            result.clear(); // clear data but reserve metadata (e.g., #rows, #cols)
    }

    bool has_pattern() { return pattern_group.patterns.size() > 0; }

    bool done(SQState state) {
        switch (state) {
        case SQ_PATTERN:
            return (pattern_step >= pattern_group.patterns.size());
        case SQ_FINAL:
            // FIXME: DEAD CODE currently
            ASSERT(false);
        case SQ_REPLY:
            // FIXME: DEAD CODE currently
            ASSERT(false);
        }
    }

    bool start_from_index() const {
        /*
         * Wukong assumes that its planner will generate a dummy pattern to hint
         * the query should start from a certain index (i.e., predicate or type).
         * For example: ?X __PREDICATE__  ub:undergraduateDegreeFrom
         *
         * NOTE: the graph exploration does not must start from this index,
         * on the contrary, starts from another index would prune bindings MORE efficiently
         * For example, ?X P0 ?Y, ?X P1 ?Z, ...
         *
         * ?X __PREDICATE__ P1 <- // start from index vertex P1
         * ?X P0 ?Y .             // then from ?X's edge with P0
         *
         */
        const Pattern& start_pattern = pattern_group.patterns[0];
        return (start_pattern.type == GE || start_pattern.type == GV);
    }

    void print_hyper_query() {
        logstream(LOG_INFO) << "HyperQuery"
                            << "[ QID=" << qid << " | PQID=" << pqid << " | MT_TID=" << mt_tid << " ]"
                            << LOG_endl;
        pattern_group.print_group();
        logstream(LOG_INFO) << "Result"
                            << "[ row =" << result.get_row_num() << " | col =" << result.get_col_num() << " ]"
                            << LOG_endl;
    }

    void print_SQState() {
        logstream(LOG_INFO) << "HyperQuery"
                            << "[ QID=" << qid << " | PQID=" << pqid << " | MT_TID=" << mt_tid << " ]";
        switch (state) {
        case SQState::SQ_PATTERN: logstream(LOG_INFO) << "\tSQ_PATTERN" << LOG_endl; break;
        case SQState::SQ_REPLY: logstream(LOG_INFO) << "\tSQ_REPLY" << LOG_endl; break;
        case SQState::SQ_FINAL: logstream(LOG_INFO) << "\tSQ_FINAL" << LOG_endl; break;
        default: logstream(LOG_INFO) << "\tUNKNOWN_STATE" << LOG_endl;
        }
    }
};

} // namespace wukong

namespace boost {
namespace serialization {
// char occupied = 0;
// char empty = 1;

template<class Archive>
void save(Archive & ar, const wukong::HyperQuery::Param &t, unsigned int version) {
    ar << t.p_type;
    ar << t.type;
    switch (t.type)
    {
    case wukong::SID_t:
        ar << t.sid;
        break;
    case wukong::HEID_t:
        ar << t.heid;
        break;
    case wukong::INT_t:
        ar << t.num;
        break;
    default:
        ASSERT(false);
    }
}

template<class Archive>
void load(Archive & ar, wukong::HyperQuery::Param &t, unsigned int version) {
    ar >> t.p_type;
    ar >> t.type;
    switch (t.type)
    {
    case wukong::SID_t:
        ar >> t.sid;
        break;
    case wukong::HEID_t:
        ar >> t.heid;
        break;
    case wukong::INT_t:
        ar >> t.num;
        break;
    default:
        ASSERT(false);
    }
}

template<class Archive>
void save(Archive &ar, const wukong::HyperQuery::Pattern &t, unsigned int version) {
    ar << t.type;
    ar << t.input_eids;
    ar << t.input_vids;
    ar << t.input_vars;
    ar << t.output_var;
    ar << t.params;
}

template<class Archive>
void load(Archive &ar, wukong::HyperQuery::Pattern &t, unsigned int version) {
    ar >> t.type;
    ar >> t.input_eids;
    ar >> t.input_vids;
    ar >> t.input_vars;
    ar >> t.output_var;
    ar >> t.params;
}

template<class Archive>
void save(Archive &ar, const wukong::HyperQuery::PatternGroup &t, unsigned int version) {
    ar << t.patterns;
}

template<class Archive>
void load(Archive &ar, wukong::HyperQuery::PatternGroup &t, unsigned int version) {
    ar >> t.patterns;
}

template<class Archive>
void save(Archive &ar, const wukong::HyperQuery::Result &t, unsigned int version) {
    ar << t.row_num;
    ar << t.blind;
    ar << t.status_code;
    ar << t.nvars;
    ar << t.required_vars;
    ar << t.v2c_map;
    ar << t.v2e_middle_map;
    ar << t.e2v_middle_map;
    ar << t.candidates;
    ar << t.vid_res_table.col_num;
    ar << t.heid_res_table.col_num;
    ar << t.float_res_table.col_num;
    ar << t.double_res_table.col_num;
    ar << t.step_latency;
    if(t.vid_res_table.get_col_num() > 0) {
        ar << t.vid_res_table.result_data;
    }
    if(t.heid_res_table.get_col_num() > 0) {
        // printf("saving HyperQuery::Resulttable with %lu items\n", t.heid_res_table.result_data.size());
        ar << t.heid_res_table.result_data;
    }
    if(t.float_res_table.get_col_num() > 0) {
        ar << t.float_res_table.result_data;
    }
    if(t.double_res_table.get_col_num() > 0) {
        ar << t.double_res_table.result_data;
    }
}

template<class Archive>
void load(Archive & ar, wukong::HyperQuery::Result &t, unsigned int version) {
    // printf("loading HyperQuery::Result\n");
    ar >> t.row_num;
    ar >> t.blind;
    ar >> t.status_code;
    ar >> t.nvars;
    ar >> t.required_vars;
    ar >> t.v2c_map;
    ar >> t.v2e_middle_map;
    ar >> t.e2v_middle_map;
    ar >> t.candidates;
    ar >> t.vid_res_table.col_num;
    ar >> t.heid_res_table.col_num;
    ar >> t.float_res_table.col_num;
    ar >> t.double_res_table.col_num;
    ar >> t.step_latency;
    if(t.vid_res_table.get_col_num() > 0) {
        ar >> t.vid_res_table.result_data;
    }
    if(t.heid_res_table.get_col_num() > 0) {
        ar >> t.heid_res_table.result_data;
    }
    if(t.float_res_table.get_col_num() > 0) {
        ar >> t.float_res_table.result_data;
    }
    if(t.double_res_table.get_col_num() > 0) {
        ar >> t.double_res_table.result_data;
    }
}

template<class Archive>
void save(Archive & ar, const wukong::HyperQuery &t, unsigned int version) {
    ar << t.qid;
    ar << t.pqid;
    ar << t.state;
    ar << t.pstate;
    ar << t.forked;
    ar << t.pattern_step;
    ar << t.pattern_group;
    ar << t.result;
}

template<class Archive>
void load(Archive & ar, wukong::HyperQuery &t, unsigned int version) {
    ar >> t.qid;
    ar >> t.pqid;
    ar >> t.state;
    ar >> t.pstate;
    ar >> t.forked;
    ar >> t.pattern_step;
    ar >> t.pattern_group;
    ar >> t.result;
}

}
}

BOOST_SERIALIZATION_SPLIT_FREE(wukong::HyperQuery::Param);
BOOST_SERIALIZATION_SPLIT_FREE(wukong::HyperQuery::Pattern);
BOOST_SERIALIZATION_SPLIT_FREE(wukong::HyperQuery::PatternGroup);
BOOST_SERIALIZATION_SPLIT_FREE(wukong::HyperQuery::Result);
BOOST_SERIALIZATION_SPLIT_FREE(wukong::HyperQuery);

// remove class information at the cost of losing auto versioning,
// which is useless currently because wukong use boost serialization to transmit data
// between endpoints running the same code.
BOOST_CLASS_IMPLEMENTATION(wukong::HyperQuery::Param, boost::serialization::object_serializable);
BOOST_CLASS_IMPLEMENTATION(wukong::HyperQuery::Pattern, boost::serialization::object_serializable);
BOOST_CLASS_IMPLEMENTATION(wukong::HyperQuery::PatternGroup, boost::serialization::object_serializable);
BOOST_CLASS_IMPLEMENTATION(wukong::HyperQuery::Result, boost::serialization::object_serializable);
BOOST_CLASS_IMPLEMENTATION(wukong::HyperQuery, boost::serialization::object_serializable);

// remove object tracking information at the cost of that multiple identical objects
// may be created when an archive is loaded.
// current query data structure does not contain two identical object reference
// with the same pointer
BOOST_CLASS_TRACKING(wukong::HyperQuery::Param, boost::serialization::track_never);
BOOST_CLASS_TRACKING(wukong::HyperQuery::Pattern, boost::serialization::track_never);
BOOST_CLASS_TRACKING(wukong::HyperQuery::PatternGroup, boost::serialization::track_never);
BOOST_CLASS_TRACKING(wukong::HyperQuery::Result, boost::serialization::track_never);
BOOST_CLASS_TRACKING(wukong::HyperQuery, boost::serialization::track_never);