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
#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "client/proxy.hpp"

#include "core/common/errors.hpp"
#include "core/common/global.hpp"
#include "core/common/monitor.hpp"

// utils
#include "utils/math.hpp"
#include "utils/timer.hpp"

namespace wukong {

class ConsoleProxy : public Proxy {
private:
    // Collect candidate constants of all template types in given template query.
    // Result is in ptypes_grp of given template query.
    void fill_template(SPARQLQuery_Template& sqt) {
        sqt.ptypes_grp.resize(sqt.ptypes_str.size());
        for (int i = 0; i < sqt.ptypes_str.size(); i++) {
            std::string type = sqt.ptypes_str[i];  // the Types of random-constant

            // create a TYPE query to collect constants with the certain type
            SPARQLQuery request = SPARQLQuery();
            bool from_predicate = (type.find("fromPredicate") != std::string::npos);

            if (from_predicate) {
                // template types are defined by predicate
                // for example, %fromPredicate takeCourse ?X .
                // create a PREDICATE query to collect constants with the certain predicate
                int pos = sqt.ptypes_pos[i];
                ssid_t p = sqt.pattern_group.patterns[pos / 4].predicate;
                dir_t d;
                switch (pos % 4) {
                case 0:
                    d = (sqt.pattern_group.patterns[pos / 4].direction == OUT) ? IN : OUT;
                    break;
                case 3:
                    d = (sqt.pattern_group.patterns[pos / 4].direction == OUT) ? OUT : IN;
                    break;
                default:
                    ASSERT(false);
                }
                SPARQLQuery::Pattern pattern(p, PREDICATE_ID, d, -1);
                pattern.pred_type = static_cast<char>(SID_t);
                request.pattern_group.patterns.push_back(pattern);

                std::string dir_str = (d == OUT) ? "->" : "<-";
                auto map_result = str_server->id2str(tid, p);
                ASSERT(map_result.first);
                type = "#Predicate [" + map_result.second + " | " + dir_str + "]";
            } else {
                // templates are defined by type
                // for example, %GraduateStudent takeCourse ?X .
                // create a TYPE query to collect constants with the certain type
                auto map_result = str_server->str2id(tid, type);
                ASSERT(map_result.first);
                SPARQLQuery::Pattern pattern(map_result.second, TYPE_ID, IN, -1);
                pattern.pred_type = static_cast<char>(SID_t);
                request.pattern_group.patterns.push_back(pattern);
            }

            request.result.nvars = 1;
            request.result.required_vars.push_back(-1);
            request.result.blind = false;  // must take back the results

            setpid(request);
            send_request(request);

            SPARQLQuery reply = recv_reply();
            std::vector<sid_t> candidates(reply.result.result_table);

            // There is no candidate with the Type for a random-constant in the template
            // TODO: it should report empty for all queries of the template
            ASSERT(candidates.size() > 0);

            sqt.ptypes_grp[i] = candidates;

            logstream(LOG_INFO) << type << " has "
                                << sqt.ptypes_grp[i].size() << " candidates" << LOG_endl;
        }
    }

public:
    Monitor monitor;

    ConsoleProxy(int sid, int tid, StringMapping* str_server, DGraph* graph,
                 Adaptor* adaptor, Stats* stats)
        : Proxy(sid, tid, str_server, graph, adaptor, stats) {}

    Stats* get_stats() { return this->stats; }

    // output result of current query
    void output_result(std::ostream& stream, SPARQLQuery& q, int sz) {
        for (int i = 0; i < sz; i++) {
            stream << i + 1 << ": ";

            // entity
            for (int j = 0; j < q.result.col_num; j++) {
                int id = q.result.get_row_col(i, j);
                auto map_result = str_server->id2str(tid, id);
                if (map_result.first)
                    stream << map_result.second << "\t";
                else
                    stream << id << "\t";
            }

            // attribute
            for (int c = 0; c < q.result.get_attr_col_num(); c++) {
                attr_t tmp = q.result.get_attr_row_col(i, c);
                stream << tmp << "\t";
            }

#ifdef TRDF_MODE
            // timestamp
            for (int j = 0; j < q.result.get_time_col_num(); j++) {
                int64_t time = q.result.get_time_row_col(i, j);
                stream << time_tool::int2str(time) << "\t";
            }
#endif

            stream << std::endl;
        }
    }

    // print result of current query to console
    void print_result(SPARQLQuery& q, int row2prt) {
        logstream(LOG_INFO) << "The first " << row2prt << " rows of results: " << LOG_endl;
        output_result(std::cout, q, row2prt);
    }

    // dump result of current query to specific file
    void dump_result(std::string path, SPARQLQuery& q, int row2prt) {
        if (boost::starts_with(path, "hdfs:")) {
            wukong::hdfs& hdfs = wukong::hdfs::get_hdfs();
            wukong::hdfs::fstream ofs(hdfs, path, true);

            output_result(ofs, q, row2prt);
            ofs.close();
        } else {
            std::ofstream ofs(path);
            if (!ofs.good()) {
                logstream(LOG_INFO) << "Can't open/create output file: " << path << LOG_endl;
                return;
            }

            output_result(ofs, q, row2prt);
            ofs.close();
        }
    }

    int run_single_query(std::istream& is, std::istream& fmt_stream,
                         SPARQLQuery& reply, std::map<std::string, std::string>& params) override {
        // get parameters
        int nopts, mt_factor, cnt, nlines;
        bool snd2gpu;
        std::string ofname = params["ofname"];

        nopts = std::stoi(params["nopts"]);
        mt_factor = std::stoi(params["mt_factor"]);
        cnt = std::stoi(params["cnt"]);
        nlines = std::stoi(params["nlines"]);
        snd2gpu = (params["snd2gpu"] == "true");

        uint64_t start, end;
        SPARQLQuery request;

        // Parse the SPARQL query
        start = timer::get_usec();
        int ret = parser.parse(is, request);
        if (ret) {
            ASSERT_ERROR_CODE(false, ret);
        }
        end = timer::get_usec();
        logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;
        request.mt_factor = std::min(mt_factor, Global::mt_threshold);

        // Generate query plan if SPARQL optimizer is enabled.
        // FIXME: currently, the optimizater only works for standard SPARQL query.
        if (Global::enable_planner) {
            start = timer::get_usec();
            for (int i = 0; i < nopts; i++)
                planner.test_plan(request);
            end = timer::get_usec();
            logstream(LOG_INFO) << "Optimization time: " << (end - start) / nopts << " usec" << LOG_endl;

            // A shortcut for contradictory queries (e.g., empty result)
            if (planner.generate_plan(request) == false) {
                logstream(LOG_INFO) << "Query has no bindings, no need to execute it." << LOG_endl;
                return 0;  // success, skip execution
            }
        } else {
            ASSERT(fmt_stream.good());
            planner.set_plan(request.pattern_group, fmt_stream);
            logstream(LOG_INFO) << "User-defined query plan is enabled" << LOG_endl;
        }

        // Print a WARNING to enable multi-threading for potential (heavy) query
        // TODO: optimizer could recognize the real heavy query
        if (request.start_from_index()  // HINT: start from index
            && !snd2gpu                 // accelerated by GPU
            && (mt_factor == 1 && Global::mt_threshold > 1)) {
            logstream(LOG_EMPH) << "The query starts from an index vertex, "
                                << "you could use option -m to accelerate it."
                                << LOG_endl;
        }

        // GPU-accelerate or not
        if (snd2gpu) {
            request.dev_type = SPARQLQuery::DeviceType::GPU;
            logstream(LOG_INFO) << "Leverage GPU to accelerate query processing." << LOG_endl;
        } else {
            request.dev_type = SPARQLQuery::DeviceType::CPU;
        }

        // Execute the SPARQL query
        monitor.init();
        for (int i = 0; i < cnt; i++) {
            setpid(request);
            // only take back results of the last request if not silent
            request.result.blind = i < (cnt - 1) ? true : Global::silent;

            send_request(request);
            reply = recv_reply();
            logstream(LOG_DEBUG) << "ConsoleProxy recv_reply: got reply qid=" << reply.qid << ", pqid=" << reply.pqid
                                 << ", dev_type=" << (reply.dev_type == SPARQLQuery::DeviceType::GPU ? "GPU" : "CPU")
                                 << ", #rows=" << reply.result.get_row_num() << ", step=" << reply.pattern_step
                                 << ", done: " << reply.done(SPARQLQuery::SQState::SQ_PATTERN) << LOG_endl;
        }
        monitor.finish();

        // Check result status
        if (reply.result.status_code == SUCCESS) {
            if (request.q_type == SPARQLQuery::ASK) {
                std::string result = reply.result.row_num? "True": "False";
                logstream(LOG_INFO) << "(last) result: " << result << LOG_endl;
            } else {
                logstream(LOG_INFO) << "(last) result row num: " << reply.result.row_num
                                    << " , col num:" << reply.result.get_col_num() << LOG_endl;

                // print or dump results
                if (!Global::silent) {
                    if (nlines > 0)
                        print_result(reply, std::min(nlines, reply.result.row_num));
                    if (ofname != "")
                        dump_result(ofname, reply, reply.result.row_num);
                }
            }
        } else {
            logstream(LOG_ERROR)
                << "Query failed [ERRNO " << reply.result.status_code << "]: "
                << ERR_MSG(reply.result.status_code) << LOG_endl;
        }

        // success
        return 0;
    }  // end of run_single_query

    // Run a query emulator for @d seconds. Command is "-b"
    // Warm up for @w firstly, then measure throughput.
    // Latency is evaluated for @d seconds.
    // ConsoleProxy keeps @p queries in flight.
    int run_query_emu(std::istream& is, std::istream& fmt_stream, int d, int w, int p) {
        uint64_t duration = SEC(d);
        uint64_t warmup = SEC(w);
        int parallel_factor = p;
        int try_rounds = 5;  // rounds to try recv reply

        // parse the first line of batch config file
        // [#lights] [#heavies]
        int nlights, nheavies;
        is >> nlights >> nheavies;
        int ntypes = nlights + nheavies;

        if (ntypes <= 0 || nlights < 0 || nheavies < 0) {
            logstream(LOG_ERROR) << "Invalid #lights (" << nlights << " < 0)"
                                 << " or #heavies (" << nheavies << " < 0)!" << LOG_endl;
            return -2;  // parsing failed
        }

        // read plan files according to config file of plans
        std::vector<std::string> fmt_fnames;
        if (!Global::enable_planner) {
            ASSERT(fmt_stream.good());

            fmt_fnames.resize(ntypes);
            for (int i = 0; i < ntypes; i++)
                fmt_stream >> fmt_fnames[i];  // FIXME: incorrect config file (e.g., few plan files)
        }

        std::vector<SPARQLQuery_Template> tpls(nlights);
        std::vector<SPARQLQuery> heavy_reqs(nheavies);

        // parse template queries
        std::vector<int> loads(ntypes);
        for (int i = 0; i < ntypes; i++) {
            // each line is a class of light or heavy query
            // [fname] [#load]
            std::string fname;
            int load;

            is >> fname;
            std::ifstream ifs(fname);
            if (!ifs) {
                logstream(LOG_ERROR) << "Query file not found: " << fname << LOG_endl;
                return -1;  // file not found
            }

            is >> load;
            ASSERT(load > 0);
            loads[i] = load;

            // parse the query
            int ret = i < nlights ? parser.parse_template(ifs, tpls[i]) :  // light query
                               parser.parse(ifs, heavy_reqs[i - nlights]);      // heavy query

            if (ret) {
                logstream(LOG_ERROR) << "Template parsing failed!" << LOG_endl;
                return -ret;  // parsing failed
            }

            // generate a template for each class of light query
            if (i < nlights)
                fill_template(tpls[i]);

            // adapt user-defined plan according
            if (!Global::enable_planner) {
                std::ifstream fs(fmt_fnames[i]);
                if (!fs.good()) {
                    logstream(LOG_ERROR) << "Plan file not found: " << fmt_fnames[i] << LOG_endl;
                    return -1;  // file not found
                }

                if (i < nlights)  // light query
                    planner.set_plan(tpls[i].pattern_group, fs, tpls[i].ptypes_pos);
                else  // heavy query
                    planner.set_plan(heavy_reqs[i - nlights].pattern_group, fs);
            }
        }

        monitor.init(ntypes);

        bool start = false;  // start to measure throughput
        uint64_t send_cnt = 0, recv_cnt = 0, flying_cnt = 0;

        uint64_t init = timer::get_usec();
        // send requeries for duration seconds
        while ((timer::get_usec() - init) < duration) {
            // send requests
            for (int i = 0; i < parallel_factor - flying_cnt; i++) {
                sweep_msgs();  // sweep pending msgs first

                int idx = wukong::math::get_distribution(coder.get_random(), loads);
                SPARQLQuery r = idx < nlights ? tpls[idx].instantiate(coder.get_random()) :  // light query
                                    heavy_reqs[idx - nlights];                               // heavy query

                if (Global::enable_planner)
                    planner.generate_plan(r);

                setpid(r);
                r.result.blind = true;  // always not take back results for emulator

                if (r.start_from_index()) {
#ifdef USE_GPU
                    r.dev_type = SPARQLQuery::DeviceType::GPU;
#else
                    r.mt_factor = Global::mt_threshold;
#endif
                }

                monitor.start_record(r.pqid, idx);
                send_request(r);

                send_cnt++;
            }

            // recieve replies (best of effort)
            for (int i = 0; i < try_rounds; i++) {
                SPARQLQuery r;
                while (tryrecv_reply(r)) {
                    recv_cnt++;
                    monitor.end_record(r.pqid);
                }
            }

            monitor.print_timely_thpt(recv_cnt, sid, tid);  // print throughput

            // start to measure throughput after first warmup seconds
            if (!start && (timer::get_usec() - init) > warmup) {
                monitor.start_thpt(recv_cnt);
                start = true;
            }

            flying_cnt = send_cnt - recv_cnt;
        }

        monitor.end_thpt(recv_cnt);  // finish to measure throughput

        // recieve all replies to calculate the tail latency
        while (recv_cnt < send_cnt) {
            sweep_msgs();  // sweep pending msgs first

            SPARQLQuery r;
            while (tryrecv_reply(r)) {
                recv_cnt++;
                monitor.end_record(r.pqid);
            }

            monitor.print_timely_thpt(recv_cnt, sid, tid);
        }

        monitor.finish();

        return 0;  // success
    }              // end of run_query_emu

    // Run a query emulator for @d seconds without parallel execution. Command is "-b"
    // Warm up for @w firstly, then measure throughput.
    // Latency is evaluated for @d seconds.
    int run_query_emu_serial(std::istream &is, std::istream &fmt_stream, int d, int w, Monitor &monitor) {
        uint64_t duration = SEC(d);
        uint64_t warmup = SEC(w);

        // parse the first line of batch config file
        // [#lights] [#heavies]
        int nlights, nheavies;
        is >> nlights >> nheavies;
        int ntypes = nlights + nheavies;

        if (ntypes <= 0 || nlights < 0 || nheavies < 0) {
            logstream(LOG_ERROR) << "Invalid #lights (" << nlights << " < 0)"
                                 << " or #heavies (" << nheavies << " < 0)!" << LOG_endl;
            return -2; // parsing failed
        }

        // read plan files according to config file of plans
        std::vector<std::string> fmt_fnames;
        if (!Global::enable_planner) {
            ASSERT(fmt_stream.good());

            fmt_fnames.resize(ntypes);
            for (int i = 0; i < ntypes; i ++)
                fmt_stream >> fmt_fnames[i];  // FIXME: incorrect config file (e.g., few plan files)
        }

        std::vector<SPARQLQuery_Template> tpls(nlights);
        std::vector<SPARQLQuery> heavy_reqs(nheavies);

        // parse template queries
        std::vector<int> loads(ntypes);
        for (int i = 0; i < ntypes; i++) {
            // each line is a class of light or heavy query
            // [fname] [#load]
            std::string fname;
            int load;

            is >> fname;
            is >> load;
            std::ifstream ifs(fname);
            ASSERT(load > 0);
            loads[i] = load;

            // parse the query
            int ret = i < nlights ?
                           parser.parse_template(ifs,tpls[i]) : // light query
                           parser.parse(ifs,heavy_reqs[i - nlights]); // heavy query

            if (ret) {
                logstream(LOG_ERROR) << "Template parsing failed!" << LOG_endl;
                return -ret; // parsing failed
            }

            // generate a template for each class of light query
            if (i < nlights)
                fill_template(tpls[i]);

            // adapt user-defined plan according
            if (!Global::enable_planner) {
                std::ifstream fs(fmt_fnames[i]);
                if (!fs.good()) {
                    logstream(LOG_ERROR) << "Plan file not found: " << fmt_fnames[i] << LOG_endl;
                    return -1; // file not found
                }

                if (i < nlights) // light query
                    planner.set_plan(tpls[i].pattern_group, fs, tpls[i].ptypes_pos);
                else // heavy query
                    planner.set_plan(heavy_reqs[i - nlights].pattern_group, fs);
            }
        }

        monitor.init(ntypes);

        bool start = false; // start to measure throughput
        uint64_t send_cnt = 0, recv_cnt = 0;

        uint64_t init = timer::get_usec();
        // send requeries for duration seconds
        while ((timer::get_usec() - init) < duration) {
            // send requests
            sweep_msgs(); // sweep pending msgs first

            int idx = wukong::math::get_distribution(coder.get_random(), loads);
            SPARQLQuery r = idx < nlights ?
                            tpls[idx].instantiate(coder.get_random()) : // light query
                            heavy_reqs[idx - nlights]; // heavy query

            if (Global::enable_planner)
                planner.generate_plan(r);

            setpid(r);
            r.result.blind = true; // always not take back results for emulator

            if (r.start_from_index()) {
#ifdef USE_GPU
                r.dev_type = SPARQLQuery::DeviceType::GPU;
#else
                r.mt_factor = Global::mt_threshold;
#endif
            }

            monitor.start_record(r.pqid, idx);
            send_request(r);

            send_cnt++;

            // recieve replies
            while (true) {
                SPARQLQuery r;
                if (tryrecv_reply(r)) {
                    recv_cnt++;
                    monitor.end_record(r.pqid);
                    break;
                }
            }

            monitor.print_timely_thpt(recv_cnt, sid, tid); // print throughput

            // start to measure throughput after first warmup seconds
            if (!start && (timer::get_usec() - init) > warmup) {
                monitor.start_thpt(recv_cnt);
                start = true;
            }
        }

        monitor.end_thpt(recv_cnt); // finish to measure throughput

        monitor.finish();

        return 0; // success
    } // end of run_query_emu_serial

#ifdef DYNAMIC_GSTORE
    int dynamic_load_data(std::string& dname, RDFLoad& reply, bool& check_dup) override {
        monitor.init();

        RDFLoad request(dname, check_dup);
        setpid(request);
        for (int i = 0; i < Global::num_servers; i++) {
            Bundle bundle(request);
            send(bundle, i);
        }

        int ret = 0;
        for (int i = 0; i < Global::num_servers; i++) {
            Bundle bundle = adaptor->recv();
            ASSERT(bundle.type == DYNAMIC_LOAD);

            reply = bundle.get_rdf_load();
            if (reply.load_ret < 0)
                ret = reply.load_ret;
        }

        monitor.finish();
        return ret;
    }
#endif

    int gstore_check(GStoreCheck& reply, bool i_enable, bool n_enable) {
        monitor.init();

        GStoreCheck request(i_enable, n_enable);
        setpid(request);
        for (int i = 0; i < Global::num_servers; i++) {
            Bundle bundle(request);
            send(bundle, i);
        }

        int ret = 0;
        for (int i = 0; i < Global::num_servers; i++) {
            Bundle bundle = Bundle(adaptor->recv());
            ASSERT(bundle.type == GSTORE_CHECK);

            reply = bundle.get_gstore_check();
            if (reply.check_ret < 0)
                ret = reply.check_ret;
        }

        monitor.finish();
        return ret;
    }
};

}  // namespace wukong
