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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "core/common/hypertype.hpp"

// loader
#include "loader/hypergraph_loader.hpp"

// store
#include "core/store/static_dgraph.hpp"

namespace wukong {

using V2EStore = KVStore<hvkey_t, iptr_t, heid_t>;
using HEStore = KVStore<hekey_t, iptr_t, sid_t>;

/**
 * @brief HyperGraph
 * 
 * Implement the HyperGraph model (e.g., vertices, hyperedges) to KVS model (e.g., key, value)
 *
 *  encoding rules of KVStore
 *  subject/object (vid) >= 2^NBITS_IDX, 2^NBITS_IDX > predicate/type (p/tid) >= 2^1,
 *  TYPE_ID = 1, PREDICATE_ID = 0, OUT = 1, IN = 0
 *
 *  Empty key
 *  (0)   key = [  0 |           0 |     0]  value = [0, 0, ..]  i.e., init
 *  INDEX key/value pair
 *  (1)   key = [  0 |         pid |    IN]  value = [vid0, vid1, ..]  i.e., predicate-index
 *  (2)   key = [  0 |         tid |    IN]  value = [vid0, vid1, ..]  i.e., type-index
 *  NORMAL key/value pair
 *  (6)   key = [vid |         pid | index]  value = [heid0, heid1, ..]  i.e., vid's ngbrs w/ predicate
 *  (7)   key = [vid | VERTEX_TYPE |   OUT]  value = [tid0, tid1, ..]  i.e., vid's all types
 */
class HyperGraph : public StaticDGraph {
protected:
    int sid;
    Mem* mem;
    StringServer* str_server;

    // all hyperedge types
    std::vector<HyperEdgeModel> edge_types;
    // all vertex types
    std::vector<sid_t> vertex_types;

    std::shared_ptr<V2EStore> v2estore;
    std::shared_ptr<HEStore> hestore;

    // TODO: put hyperedge into vector?
    // std::vector<uint64_t> he_offsets; 
    // sid_t* he_store;

    // TODO vtype-index (temp variable)
    // TODO etype-index (temp variable)

    virtual void init_gstore(std::vector<std::vector<HyperEdge>>& hyperedges) = 0;

public:

    HyperGraph(int sid, Mem* mem, StringServer* str_server)
        : StaticDGraph(sid, mem, str_server) {
        // TODO: init v2estore and hestore
    }

    void load(std::string dname) {
        StaticDGraph::load(dname);

        uint64_t start, end;

        std::shared_ptr<HyperGraphBaseLoader> loader;
        // load from hdfs or posix file
        if (boost::starts_with(dname, "hdfs:"))
            loader = std::make_shared<HyperGraphHDFSLoader>(sid, mem, str_server);
        else
            loader = std::make_shared<HyperGraphPosixLoader>(sid, mem, str_server);

        std::vector<std::vector<HyperEdge>> hyper_edges;

        start = timer::get_usec();
        loader->load(dname, hyper_edges);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraphLoader] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for loading hyperedges from disk to memory." << LOG_endl;

        auto count_hyperedge_types = [this](const std::string str_idx_file, bool is_attr = false) {
            std::string edge_type;
            int pid, index_num;
            std::ifstream ifs(str_idx_file.c_str());
            while (ifs >> edge_type >> pid >> index_num) {
                HyperEdgeModel edge_model;
                edge_model.type_id = pid;
                edge_model.index_num = index_num;
                edge_model.index_size.resize(index_num);
                edge_model.index_type_hint.resize(index_num);
                for(int i = 0; i < index_num; i++){
                    ifs >> edge_model.index_size[i];
                }
                this->edge_types.push_back(edge_model);
            }
            ifs.close();
        };

        count_hyperedge_types(dname + "hyper_str_index");
        if (this->edge_types.size() <= 1) {
            logstream(LOG_ERROR) << "Encoding file of predicates should be named as \"str_index\". Graph loading failed. Please quit and try again." << LOG_endl;
        }

        // initiate gstore (kvstore) after loading and exchanging triples (memory reused)
        gstore->refresh();

        start = timer::get_usec();
        // TODO: init v2estore
        end = timer::get_usec();
        logstream(LOG_INFO) << "[RDFGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for initializing v2estore." << LOG_endl;

        start = timer::get_usec();
        // TODO: init hestore
        end = timer::get_usec();
        logstream(LOG_INFO) << "[RDFGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for initializing hestore." << LOG_endl;

        logstream(LOG_INFO) << "[RDFGraph] #" << sid << ": loading HyperGraph is finished" << LOG_endl;

        print_graph_stat();
    }

    virtual ~HyperGraph() {}

    // return total num of edge types
    inline int get_num_edge_types() const { return this->edge_types.size(); }
    // return total num of vertex types
    inline int get_num_vertex_types() const { return this->vertex_types.size(); }
    
    inline std::vector<HyperEdgeModel> get_edge_types() const { return this->edge_types; }
    inline std::vector<sid_t> get_vertex_types() const { return this->vertex_types; }
    
    virtual std::vector<HyperEdge*> get_hyper_edges(int tid, sid_t vid, sid_t edge_type, int index) {

    }

    virtual int dynamic_load_data(std::string dname, bool check_dup) {}
};

}  // namespace wukong
