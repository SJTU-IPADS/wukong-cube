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
 *  (0)   key = [     0 |           0]  value = [0, 0, ..]             i.e., init
 *  INDEX key/value pair
 *  (1)   key = [     0 |        htid]  value = [vid0, vid1, ..]       i.e., HEDGE-vindex
 *  (2)   key = [     0 |        htid]  value = [heid0, heid1, ..]     i.e., HTYPE-index
 *  (3)   key = [     0 |         tid]  value = [vid0, vid1, ..]       i.e., VTYPE-index
 *  NORMAL key/value pair
 *  (6)   key = [   vid |        htid]  value = [heid0, heid1, ..]     i.e., V2E-KV
 *  (6)   key = [                heid]  value = [vid0, vid1, ..]       i.e., HEDGE-KV
 *  (7)   key = [   vid | VERTEX_TYPE]  value = [tid0, tid1, ..]       i.e., vid's all types
 *  (7)   key = [  heid |  HYPER_TYPE]  value = [htid0, htid1, ..]     i.e., heid's all types
 */
class HyperGraph : public DGraph {
protected:
    using tbb_hedge_hash_map = tbb::concurrent_hash_map<sid_t, std::vector<heid_t>>;
    using tbb_hv_hash_map = tbb::concurrent_hash_map<sid_t, std::set<sid_t>>;
    using tbb_ht_hash_map = tbb::concurrent_hash_map<heid_t, std::set<sid_t>>;

    const int HE_RATIO = 50;
    const int V2E_RATIO = 50;

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

    // hyperedge-index (htid -> heids)
    tbb_hedge_hash_map he_map;
    // hyperedge-type-index (htid -> heids)
    tbb_ht_hash_map ht_map;
    // hyperege-vertex-index (htid -> vids)
    tbb_hv_hash_map hv_map;

    void collect_idx_info(RDFStore::slot_t& slot) {
        sid_t vid = slot.key.vid;
        sid_t pid = slot.key.pid;

        uint64_t sz = slot.ptr.size;
        uint64_t off = slot.ptr.off;

        if (slot.key.dir == IN) {
            if (pid == PREDICATE_ID) {
            } else if (pid == TYPE_ID) {
                // (IN) type triples should be skipped
                ASSERT(false);
            } else {  // predicate-index (OUT) vid
                tbb_edge_hash_map::accessor a;
                pidx_out_map.insert(a, pid);
                a->second.push_back(edge_t(vid));
            }
        } else {
            if (pid == PREDICATE_ID) {
            } else if (pid == TYPE_ID) {
                // type-index (IN) vid
                for (uint64_t e = 0; e < sz; e++) {
                    tbb_edge_hash_map::accessor a;
                    tidx_map.insert(a, this->gstore->values[off + e].val);
                    a->second.push_back(edge_t(vid));
                }
            } else {  // predicate-index (IN) vid
                tbb_edge_hash_map::accessor a;
                pidx_in_map.insert(a, pid);
                a->second.push_back(edge_t(vid));
            }
        }
    }

    /// skip all TYPE triples (e.g., <http://www.Department0.University0.edu> rdf:type ub:University)
    /// because Wukong treats all TYPE triples as index vertices. In addition, the triples in triple_pos
    /// has been sorted by the vid of object, and IDs of types are always smaller than normal vertex IDs.
    /// Consequently, all TYPE triples are aggregated at the beggining of triple_pos
    void insert_normal(int tid, std::vector<triple_t>& pso, std::vector<triple_t>& pos) {
        // treat type triples as index vertices
        uint64_t type_triples = 0;
        while (type_triples < pos.size() && is_tpid(pos[type_triples].o))
            type_triples++;

        uint64_t s = 0;
        while (s < pso.size()) {
            // predicate-based key (subject + predicate)
            uint64_t e = s + 1;
            while ((e < pso.size()) && (pso[s].s == pso[e].s) && (pso[s].p == pso[e].p)) {
                e++;
            }

            // allocate entries
            uint64_t off = this->gstore->alloc_entries(e - s, tid);

            // insert subject & predicate
            uint64_t slot_id = this->gstore->insert_key(
                ikey_t(pso[s].s, pso[s].p, OUT), iptr_t(e - s, off));

            // insert objects
            for (uint64_t i = s; i < e; i++) {
                this->gstore->values[off++] = edge_t(pso[i].o);
            }

            collect_idx_info(this->gstore->slots[slot_id]);

            s = e;
        }

        s = type_triples;  // skip type triples
        while (s < pos.size()) {
            // predicate-based key (object + predicate)
            uint64_t e = s + 1;
            while ((e < pos.size()) && (pos[s].o == pos[e].o) && (pos[s].p == pos[e].p)) {
                e++;
            }

            // allocate entries
            uint64_t off = this->gstore->alloc_entries(e - s, tid);

            // insert object
            uint64_t slot_id = this->gstore->insert_key(
                ikey_t(pos[s].o, pos[s].p, IN),
                iptr_t(e - s, off));

            // insert values
            for (uint64_t i = s; i < e; i++) {
                this->gstore->values[off++] = edge_t(pos[i].s);
            }

            collect_idx_info(this->gstore->slots[slot_id]);
            s = e;
        }
    }

    // insert attributes
    void insert_attr(std::vector<triple_attr_t>& attrs, int64_t tid) {
        for (auto const& attr : attrs) {
            // allocate entries
            int type = boost::apply_visitor(variant_type(), attr.v);
            uint64_t sz = (get_sizeof(type) - 1) / sizeof(edge_t) + 1;  // get the ceil size;
            uint64_t off = this->gstore->alloc_entries(sz, tid);

            // insert subject
            uint64_t slot_id = this->gstore->insert_key(
                ikey_t(attr.s, attr.a, OUT),
                iptr_t(sz, off, type));

            // insert values (attributes)
            switch (type) {
            case INT_t:
                *reinterpret_cast<int*>(this->gstore->values + off) = boost::get<int>(attr.v);
                break;
            case FLOAT_t:
                *reinterpret_cast<float*>(this->gstore->values + off) = boost::get<float>(attr.v);
                break;
            case DOUBLE_t:
                *reinterpret_cast<double*>(this->gstore->values + off) = boost::get<double>(attr.v);
                break;
            default:
                logstream(LOG_ERROR) << "Unsupported value type of attribute" << LOG_endl;
            }
        }
    }

    virtual void insert_index() {
        uint64_t t1 = timer::get_usec();

        // insert type-index & predicate-idnex edges in parallel
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int i = 0; i < 3; i++) {
            if (i == 0) insert_index_map(tidx_map, IN);
            if (i == 1) insert_index_map(pidx_in_map, IN);
            if (i == 2) insert_index_map(pidx_out_map, OUT);
        }

        // init edge_predicates and type_predicates
        std::set<sid_t> type_pred_set;
        std::set<sid_t> edge_pred_set;
        for (auto& e : tidx_map) type_pred_set.insert(e.first);
        for (auto& e : pidx_in_map) edge_pred_set.insert(e.first);
        for (auto& e : pidx_out_map) edge_pred_set.insert(e.first);
        edge_pred_set.insert(TYPE_ID);
        this->type_predicates.assign(type_pred_set.begin(), type_pred_set.end());
        this->edge_predicates.assign(edge_pred_set.begin(), edge_pred_set.end());

        tbb_edge_hash_map().swap(pidx_in_map);
        tbb_edge_hash_map().swap(pidx_out_map);
        tbb_edge_hash_map().swap(tidx_map);

        uint64_t t2 = timer::get_usec();
        logstream(LOG_DEBUG) << (t2 - t1) / 1000 << " ms for inserting index data into gstore" << LOG_endl;
    }

    void insert_index_map(tbb_edge_hash_map& map, dir_t d) {
        for (auto const& e : map) {
            // alloc entries
            sid_t pid = e.first;
            uint64_t sz = e.second.size();
            uint64_t off = this->gstore->alloc_entries(sz, 0);

            // insert index key
            uint64_t slot_id = this->gstore->insert_key(
                ikey_t(0, pid, d),
                iptr_t(sz, off));

            // insert subjects/objects
            for (auto const& edge : e.second)
                this->gstore->values[off++] = edge;
        }
    }

    void insert_v2etriple(int tid, std::vector<V2ETriple>& v2etriples) {
        uint64_t s = 0;
        while (s < v2etriples.size()) {
            uint64_t e = s + 1;
            while ((e < v2etriples.size()) &&
                   (v2etriples[s].vid == v2etriples[e].vid) &&
                   (v2etriples[s].edge_type == v2etriples[e].edge_type)) {
                e++;
            }

            // allocate entries
            uint64_t off = this->v2estore->alloc_entries(e - s, tid);

            // insert key (vid + edge_type + index)
            uint64_t slot_id = this->v2estore->insert_key(
                hvkey_t(v2etriples[s].vid, v2etriples[s].edge_type),
                iptr_t(e - s, off));

            // insert eids
            for (uint64_t i = s; i < e; i++) {
                this->v2estore->values[off++] = v2etriples[i].eid;
            }

            // hyperege-vertex-index
            tbb_hv_hash_map::accessor a;
            hv_map.insert(a, v2etriples[s].edge_type);
            a->second.insert(v2etriples[s].vid);

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

            // hyperedge-type-index
            tbb_ht_hash_map::accessor aht;
            ht_map.insert(aht, edge.id);
            aht->second.insert(edge.edge_type);
        }
    }

    void insert_he_index() {
        // insert hyperedge index(htid -> heids)
        for (auto const& e : he_map) {
            // alloc entries
            sid_t edge_type = e.first;
            uint64_t sz = e.second.size();
            uint64_t off = this->v2estore->alloc_entries(sz, 0);

            // insert hyper type as index key
            uint64_t slot_id = this->v2estore->insert_key(
                hvkey_t(0, edge_type),
                iptr_t(sz, off));

            // insert heids as value
            for (auto const& heid : e.second)
                this->v2estore->values[off++] = heid;
        }

        // insert hyperedge type index(heid -> htids)
        for (auto const& e : ht_map) {
            // alloc entries
            heid_t heid = e.first;
            uint64_t sz = e.second.size();
            uint64_t off = this->v2estore->alloc_entries(sz, 0);

            // insert hyper type as index key
            uint64_t slot_id = this->v2estore->insert_key(
                hvkey_t(heid, EDGE_TYPE),
                iptr_t(sz, off));

            // insert heids as value
            for (auto const& htid : e.second)
                this->v2estore->values[off++] = htid;
        }

        // insert hypertype-vertex index(htid -> vids)
        for (auto const& hv : hv_map) {
            // alloc entries
            sid_t edge_type = hv.first;
            uint64_t sz = hv.second.size();
            uint64_t off = this->hestore->alloc_entries(sz, 0);

            // insert hyper type as index key
            uint64_t slot_id = this->hestore->insert_key(
                hekey_t(edge_type),
                iptr_t(sz, off));

            // insert vids as value
            for (auto const& vid : hv.second)
                this->hestore->values[off++] = vid;
        }
   
        // clear maps
        tbb_hedge_hash_map().swap(he_map);
        tbb_ht_hash_map().swap(ht_map);
        tbb_hv_hash_map().swap(hv_map);
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
    HyperGraph(int sid, KVMem kv_mem)
        : DGraph(sid, kv_mem) {
        // KVMem rdf_kv_mem = {
        //     .kvs = kv_mem.kvs, 
        //     .kvs_sz = kv_mem.kvs_sz * RDF_RATIO / 100, 
        //     .rrbuf = kv_mem.rrbuf, 
        //     .rrbuf_sz = kv_mem.rrbuf_sz
        // };
        
        KVMem he_kv_mem = {
            .kvs = kv_mem.kvs, 
            .kvs_sz = kv_mem.kvs_sz * HE_RATIO / 100, 
            .rrbuf = kv_mem.rrbuf, 
            .rrbuf_sz = kv_mem.rrbuf_sz
        };

        KVMem v2e_kv_mem = {
            .kvs = kv_mem.kvs + kv_mem.kvs_sz * HE_RATIO / 100, 
            .kvs_sz = kv_mem.kvs_sz * V2E_RATIO / 100, 
            .rrbuf = kv_mem.rrbuf, 
            .rrbuf_sz = kv_mem.rrbuf_sz
        };
        // this->gstore = std::make_shared<StaticKVStore<ikey_t, iptr_t, edge_t>>(sid, rdf_kv_mem);
        this->hestore = std::make_shared<StaticKVStore<hekey_t, iptr_t, sid_t>>(sid, he_kv_mem);
        this->v2estore = std::make_shared<StaticKVStore<hvkey_t, iptr_t, heid_t>>(sid, v2e_kv_mem);
    }

    void load(std::string dname, StringServer *str_server = NULL) override {
        uint64_t start, end;

        LoaderMem loader_mem = {
            .global_buf = kv_mem.kvs, .global_buf_sz = kv_mem.kvs_sz,
            .local_buf = kv_mem.rrbuf, .local_buf_sz = kv_mem.rrbuf_sz
        };

        // ========== RDF loader ==========
        // load from hdfs or posix file
        // std::shared_ptr<BaseLoader> loader;
        // if (boost::starts_with(dname, "hdfs:"))
        //     loader = std::make_shared<HDFSLoader>(sid, loader_mem);
        // else
        //     loader = std::make_shared<PosixLoader>(sid, loader_mem);

        // std::vector<std::vector<triple_t>> triple_pso;
        // std::vector<std::vector<triple_t>> triple_pos;
        // std::vector<std::vector<triple_attr_t>> triple_sav;

        // start = timer::get_usec();
        // loader->load(dname, triple_pso, triple_pos, triple_sav);
        // end = timer::get_usec();
        // logstream(LOG_INFO) << "[Loader] #" << sid << ": " << (end - start) / 1000 << "ms "
        //                     << "for loading triples from disk to memory." << LOG_endl;

        // check predicates
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
        if (this->predicates.size() <= 1)
            logstream(LOG_ERROR) << "Encoding file of predicates should be named as \"str_index\". Graph loading failed. Please quit and try again." << LOG_endl;
        if (Global::enable_vattr)
            count_preds(dname + "str_attr_index", true);

        // ========== native hyper loader ==========
        std::vector<std::vector<HyperEdge>> hyperedges;
        std::vector<std::vector<V2ETriple>> v2etriples;

        std::shared_ptr<HyperGraphBaseLoader> hyperloader;
        // load from hdfs or posix file
        if (boost::starts_with(dname, "hdfs:"))
            hyperloader = std::make_shared<HyperGraphHDFSLoader>(sid, loader_mem);
        else
            hyperloader = std::make_shared<HyperGraphPosixLoader>(sid, loader_mem);

        // check hyper types
        auto count_hyperedge_types = [this](const std::string str_idx_file) {
            std::string edge_type;
            int htid;
            std::ifstream ifs(str_idx_file.c_str());
            while (ifs >> edge_type >> htid) {
                HyperEdgeModel edge_model;
                edge_model.type_id = htid;
                this->edge_types.push_back(edge_model);
                this->edge_models[htid] = edge_model;
            }
            ifs.close();
        };
        count_hyperedge_types(dname + "hyper_str_index");
        if (this->edge_types.size() <= 1)
            logstream(LOG_ERROR) << "Encoding file of hypertypes should be named as \"hyper_str_index\". Graph loading failed. Please quit and try again." << LOG_endl;

        start = timer::get_usec();
        hyperloader->load(dname, str_server, edge_models, hyperedges, v2etriples);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperLoader] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for loading hyperedges from disk to memory." << LOG_endl;

        // ========== init RDF KV ==========
        // initiate gstore (kvstore) after loading and exchanging triples (memory reused)
        // gstore->refresh();

        // start = timer::get_usec();
        // init_gstore(triple_pso, triple_pos, triple_sav);
        // end = timer::get_usec();
        // logstream(LOG_INFO) << "[RDFGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
        //                     << "for initializing gstore." << LOG_endl;

        // print data information
        size_t v2e_count = 0, he_count = 0;
        for (auto const& v2eVec : v2etriples) v2e_count += v2eVec.size();
        for (auto const& heVec : hyperedges) he_count += heVec.size();
        logstream(LOG_INFO) << "-------" << v2e_count << " v2etriples, " << he_count << " hyperedges-------" << LOG_endl;

        // ========== init V2E KV ==========
        start = timer::get_usec();
        v2estore->refresh();
        init_v2estore(v2etriples);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for initializing v2estore." << LOG_endl;

        // ========== init HyperEdge KV ==========
        start = timer::get_usec();
        hestore->refresh();
        init_hestore(hyperedges);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperGraph] #" << sid << ": " << (end - start) / 1000 << "ms "
                            << "for initializing hestore." << LOG_endl;

        // ========== init index ==========
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

    // return all edge types
    inline std::vector<HyperEdgeModel> get_edge_types() const { return this->edge_types; }
    // return all vertex types
    inline std::vector<sid_t> get_vertex_types() const { return this->vertex_types; }

    // get heid list by vid and hyper type
    heid_t* get_heids_by_vertex_and_type(int tid, sid_t vid, sid_t edge_type, uint64_t& sz) {
        return v2estore->get_values(tid, PARTITION(vid), hvkey_t(vid, edge_type), sz);
    }
   
    // get heid list by hyper type
    heid_t* get_heids_by_type(int tid, sid_t edge_type, uint64_t& sz) {
        return v2estore->get_values(tid, this->sid, hvkey_t(0, edge_type), sz);
    }

    // get hyper edge content by heid
    sid_t* get_edge_by_heid(int tid, heid_t eid, uint64_t& sz) {
        return hestore->get_values(tid, PARTITION(eid), hekey_t(eid), sz);
    }

    // get vids by hyper type
    sid_t* get_vids_by_htype(int tid, sid_t edge_type, uint64_t& sz) {
        return hestore->get_values(tid, this->sid, hekey_t(edge_type), sz);
    }

    // get heid list by vid and hyper type
    std::vector<std::pair<sid_t*, uint64_t>> get_edges_by_type(int tid, sid_t edge_type) {
        // index vertex should be 0 and always local
        std::vector<std::pair<sid_t*, uint64_t>> result;
        uint64_t edge_sz;
        heid_t* hyper_edge_ids = v2estore->get_values(tid, this->sid, hvkey_t(0, edge_type), edge_sz);
        result.reserve(edge_sz);
        for (int i = 0; i < edge_sz; i++) {
            uint64_t sz;
            sid_t* vids = hestore->get_values(tid, this->sid, hekey_t(hyper_edge_ids[i]), sz);
            result.push_back(std::make_pair(vids, sz));
        }
        return result;
    }

    // get the type of a hyperedge
    heid_t* get_type_by_heid(int tid, heid_t eid, uint64_t& sz) {
        return v2estore->get_values(tid, this->sid, hvkey_t(eid, EDGE_TYPE), sz);
    }

    virtual int dynamic_load_data(std::string dname, bool check_dup) {}

    void print_graph_stat() override {
        // DGraph::print_graph_stat();
        logstream(LOG_INFO) << "========== [HyperGraph] hestore ==========" << LOG_endl;
        hestore->print_mem_usage();
        logstream(LOG_INFO) << "========== [HyperGraph] v2estore ==========" << LOG_endl;
        v2estore->print_mem_usage();
    }
};

}  // namespace wukong
