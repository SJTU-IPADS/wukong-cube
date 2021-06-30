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
#include "core/hypergraph/hypervertex.hpp"

// loader
#include "loader/hypergraph_loader.hpp"

// store
#include "core/store/dgraph.hpp"

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
class HyperGraph : public DGraph {
protected:
    using tbb_hedge_hash_map = tbb::concurrent_hash_map<sid_t, std::vector<heid_t>>;

    const int RDF_RATIO = 60;
    const int HE_RATIO = 20;
    const int V2E_RATIO = 20;

    // all hyperedge types
    std::vector<HyperEdgeModel> edge_types;
    std::map<sid_t, HyperEdgeModel> edge_models;
    // all vertex types
    std::vector<sid_t> vertex_types;

    std::shared_ptr<HEStore> hestore;
    std::shared_ptr<V2EStore> v2estore;

    // TODO: put hyperedge into vector?
    // std::vector<uint64_t> he_offsets;
    // sid_t* he_store;

    // hyperedge-index
    tbb_hedge_hash_map he_map;

    void insert_v2etriple(int tid, std::vector<V2ETriple>& v2etriples) {
        uint64_t s = 0;
        while (s < v2etriples.size()) {
            uint64_t e = s + 1;
            while ((e < v2etriples.size()) &&
                   (v2etriples[s].vid == v2etriples[e].vid) &&
                   (v2etriples[s].edge_type == v2etriples[e].edge_type) &&
                   (v2etriples[s].index == v2etriples[e].index)) {
                e++;
            }

            // allocate entries
            uint64_t off = this->v2estore->alloc_entries(e - s, tid);

            // insert key (vid + edge_type + index)
            uint64_t slot_id = this->v2estore->insert_key(
                hvkey_t(v2etriples[s].vid, v2etriples[s].edge_type, v2etriples[s].index),
                iptr_t(e - s, off));

            // insert eids
            for (uint64_t i = s; i < e; i++) {
                this->v2estore->values[off++] = v2etriples[s].eid;
            }

            // TODO: build index
            // collect_idx_info(this->v2estore->slots[slot_id]);
            s = e;
        }
    }

    // insert hyperedges
    void insert_hyperedge(int64_t tid, std::vector<HyperEdge>& edges) {
        for (auto const& edge : edges) {
            // allocate entries
            uint64_t sz = edge.vertices.size();
            uint64_t off = this->hestore->alloc_entries(sz, tid);

            // insert heid
            uint64_t slot_id = this->hestore->insert_key(
                hekey_t(edge.id),
                iptr_t(sz, off));

            // insert values (vids)
            std::copy(edge.vertices.begin(), edge.vertices.end(), &this->hestore->values[off]);

            // hyperedge-index
            tbb_hedge_hash_map::accessor a;
            he_map.insert(a, edge.edge_type);
            a->second.push_back(edge.id);
        }
    }

    void insert_he_index() {
        for (auto const& e : he_map) {
            // alloc entries
            sid_t edge_type = e.first;
            uint64_t sz = e.second.size();
            uint64_t off = this->v2estore->alloc_entries(sz, 0);

            // insert index key
            uint64_t slot_id = this->v2estore->insert_key(
                hvkey_t(0, edge_type, 0),
                iptr_t(sz, off));

            // insert subjects/objects
            for (auto const& heid : e.second)
                this->v2estore->values[off++] = heid;
        }
    }

    void init_v2estore(std::vector<std::vector<V2ETriple>>& v2etriples) {
        uint64_t start, end;

        start = timer::get_usec();
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int t = 0; t < Global::num_engines; t++) {
            insert_v2etriple(t, v2etriples[t]);

            // release memory
            std::vector<V2ETriple>().swap(v2etriples[t]);
        }
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for inserting v2etriples into v2estore" << LOG_endl;
    }

    void init_hestore(std::vector<std::vector<HyperEdge>>& hyperedges) {
        uint64_t start, end;

        start = timer::get_usec();
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int t = 0; t < Global::num_engines; t++) {
            insert_hyperedge(t, hyperedges[t]);

            // release memory
            std::vector<HyperEdge>().swap(hyperedges[t]);
        }
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for inserting hyperedges into hestore" << LOG_endl;
    }

    void init_gstore(std::vector<std::vector<triple_t>>& triple_pso,
                     std::vector<std::vector<triple_t>>& triple_pos,
                     std::vector<std::vector<triple_attr_t>>& triple_sav) override {
        uint64_t start, end;

        start = timer::get_usec();
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int t = 0; t < Global::num_engines; t++) {
            insert_normal(t, triple_pso[t], triple_pos[t]);

            // release memory
            std::vector<triple_t>().swap(triple_pso[t]);
            std::vector<triple_t>().swap(triple_pos[t]);
        }
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for inserting normal data into gstore" << LOG_endl;

        start = timer::get_usec();
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int t = 0; t < Global::num_engines; t++) {
            insert_attr(triple_sav[t], t);

            // release memory
            std::vector<triple_attr_t>().swap(triple_sav[t]);
        }
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for inserting attributes into gstore" << LOG_endl;

        start = timer::get_usec();
        insert_index();
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for inserting index data into gstore" << LOG_endl;
    }

public:
    HyperGraph(int sid, Mem* mem, StringServer* str_server)
        : DGraph(sid, mem, str_server) {
        char* rdf_addr = mem->kvstore();
        uint64_t rdf_size = mem->kvstore_size() * RDF_RATIO / 100;
        char* he_addr = mem->kvstore() + mem->kvstore_size() * RDF_RATIO / 100;
        uint64_t he_size = mem->kvstore_size() * HE_RATIO / 100;
        char* v2e_addr = mem->kvstore() + mem->kvstore_size() * (RDF_RATIO + HE_RATIO) / 100;
        uint64_t v2e_size = mem->kvstore_size() * V2E_RATIO / 100;
        this->gstore = std::make_shared<StaticKVStore<ikey_t, iptr_t, edge_t>>(sid, mem, rdf_addr, rdf_size);
        this->hestore = std::make_shared<StaticKVStore<hekey_t, iptr_t, sid_t>>(sid, mem, he_addr, he_size);
        this->v2estore = std::make_shared<StaticKVStore<hvkey_t, iptr_t, heid_t>>(sid, mem, v2e_addr, v2e_size);
    }

    void load(std::string dname) override {
        uint64_t start, end;

        std::shared_ptr<BaseLoader> loader;
        // load from hdfs or posix file
        if (boost::starts_with(dname, "hdfs:"))
            loader = std::make_shared<HDFSLoader>(sid, mem, str_server);
        else
            loader = std::make_shared<PosixLoader>(sid, mem, str_server);

        std::vector<std::vector<triple_t>> triple_pso;
        std::vector<std::vector<triple_t>> triple_pos;
        std::vector<std::vector<triple_attr_t>> triple_sav;

        start = timer::get_usec();
        loader->load(dname, triple_pso, triple_pos, triple_sav);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[Loader] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for loading triples from disk to memory." << LOG_endl;

        std::shared_ptr<HyperGraphBaseLoader> hyperloader;
        // load from hdfs or posix file
        if (boost::starts_with(dname, "hdfs:"))
            hyperloader = std::make_shared<HyperGraphHDFSLoader>(sid, mem, str_server);
        else
            hyperloader = std::make_shared<HyperGraphPosixLoader>(sid, mem, str_server);

        auto count_preds = [this](const std::string str_idx_file, bool is_attr = false) {
            std::string pred;
            int pid;
            std::ifstream ifs(str_idx_file.c_str());
            while (ifs >> pred >> pid) {
                if (!is_attr) {
                    this->predicates.push_back(pid);
                } else {
                    this->attributes.push_back(pid);
                    this->attr_type_dim_map.insert(
                        std::make_pair(pid, std::make_pair(SID_t, -1)));
                }
            }
            ifs.close();
        };

        count_preds(dname + "str_index");
        if (this->predicates.size() <= 1) {
            logstream(LOG_ERROR) << "Encoding file of predicates should be named as \"str_index\". Graph loading failed. Please quit and try again." << LOG_endl;
        }
        if (Global::enable_vattr)
            count_preds(dname + "str_attr_index", true);

        std::vector<std::vector<HyperEdge>> hyperedges;
        std::vector<std::vector<V2ETriple>> v2etriples;

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
                for (int i = 0; i < index_num; i++) {
                    ifs >> edge_model.index_size[i];
                }
                this->edge_types.push_back(edge_model);
                this->edge_models[pid] = edge_model;
            }
            ifs.close();
        };

        count_hyperedge_types(dname + "hyper_str_index");
        if (this->edge_types.size() <= 1) {
            logstream(LOG_ERROR) << "Encoding file of predicates should be named as \"str_index\". Graph loading failed. Please quit and try again." << LOG_endl;
        }

        start = timer::get_usec();
        hyperloader->load(dname, edge_models, hyperedges, v2etriples);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperLoader] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for loading hyperedges from disk to memory." << LOG_endl;

        // initiate gstore (kvstore) after loading and exchanging triples (memory reused)
        gstore->refresh();

        start = timer::get_usec();
        init_gstore(triple_pso, triple_pos, triple_sav);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[RDFGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for initializing gstore." << LOG_endl;

        start = timer::get_usec();
        init_v2estore(v2etriples);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for initializing v2estore." << LOG_endl;

        start = timer::get_usec();
        init_hestore(hyperedges);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for initializing hestore." << LOG_endl;

        start = timer::get_usec();
        insert_he_index();
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for inserting index data into v2estore" << LOG_endl;

        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": loading HyperGraph is finished" << LOG_endl;

        print_graph_stat();
    }

    virtual ~HyperGraph() {}

    // return total num of edge types
    inline int get_num_edge_types() const { return this->edge_types.size(); }
    // return total num of vertex types
    inline int get_num_vertex_types() const { return this->vertex_types.size(); }

    inline std::vector<HyperEdgeModel> get_edge_types() const { return this->edge_types; }
    inline std::vector<sid_t> get_vertex_types() const { return this->vertex_types; }

    std::vector<std::pair<sid_t*, uint64_t>> get_edges_by_type(int tid, sid_t edge_type) {
        // index vertex should be 0 and always local
        std::vector<std::pair<sid_t*, uint64_t>> result;
        uint64_t edge_sz;
        heid_t* hyper_edge_ids = v2estore->get_values(tid, this->sid, hvkey_t(0, edge_type, 0), edge_sz);
        result.reserve(edge_sz);
        for (int i = 0; i < edge_sz; i++) {
            uint64_t sz;
            sid_t* vids = hestore->get_values(tid, this->sid, hekey_t(hyper_edge_ids[i]), sz);
            result.push_back(std::make_pair(vids, sz));
        }
        return result;
    }

    heid_t* get_edges_by_vertex(int tid, sid_t vid, sid_t edge_type, int index, uint64_t& sz) {
        return v2estore->get_values(tid, PARTITION(vid), hvkey_t(vid, edge_type, index), sz);
    }

    sid_t* get_edges_by_id(int tid, heid_t eid, uint64_t& sz) {
        return hestore->get_values(tid, PARTITION(eid), hekey_t(eid), sz);
    }

    virtual int dynamic_load_data(std::string dname, bool check_dup) {}

    void print_graph_stat() override {
        DGraph::print_graph_stat();
        hestore->print_mem_usage();
        v2estore->print_mem_usage();
    }
};

}  // namespace wukong
