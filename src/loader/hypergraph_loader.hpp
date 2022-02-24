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

#include <dirent.h>
#include <omp.h>
#include <stdio.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <tbb/concurrent_hash_map.h>       // NOLINT

#include <boost/algorithm/string/predicate.hpp>
#include <boost/mpi.hpp>
#include <boost/unordered_map.hpp>

#include "core/common/global.hpp"
#include "core/common/rdma.hpp"
#include "core/common/type.hpp"
#include "core/hypergraph/hypervertex.hpp"

// loader
#include "loader_interface.hpp"

// utils
#include "utils/atomic.hpp"
#include "utils/hdfs.hpp"
#include "utils/assertion.hpp"
#include "utils/math.hpp"
#include "utils/timer.hpp"

namespace wukong {

class HyperGraphBaseLoader : public HyperGraphLoaderInterface {
    using tbb_str_hash_map = tbb::concurrent_hash_map<heid_t, std::string>;
    tbb_str_hash_map id2str;

protected:
    int sid;
    LoaderMem loader_mem;

    std::vector<uint64_t> num_hyperedges;  // record #hyperedges loaded from input data for each server
    std::vector<uint64_t> num_datas;  // record #sids loaded from input data for each server

    uint64_t inline floor(uint64_t original, uint64_t n) {
        ASSERT(n != 0);
        return original - original % n;
    }

    uint64_t inline ceil(uint64_t original, uint64_t n) {
        ASSERT(n != 0);
        if (original % n == 0)
            return original;
        return original - original % n + n;
    }

    heid_t generate_heid(int sid, int tid, int index) {
        // need refine
        static heid_t heid_base = (1 << NBITS_ETYPE);
        heid_t id = Global::num_servers * Global::num_engines * index
                    + tid * Global::num_servers + sid + heid_base;
        return id;
    }

    template<class DataType>
    void dedup_data(std::vector<DataType>& data) {
        if (data.size() <= 1)
            return;

        uint64_t n = 1;
        for (uint64_t i = 1; i < data.size(); i++) {
            if (data[i] == data[i - 1]) {
                continue;
            }

            data[n++] = data[i];
        }
        data.resize(n);
    }

    template<class DataType, class SortFunc>
    void sort_data(std::vector<std::vector<DataType>>& data) {
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int tid = 0; tid < Global::num_engines; tid++) {
            std::sort(data[tid].begin(), data[tid].end(), SortFunc());

            dedup_data<DataType>(data[tid]);

            data[tid].shrink_to_fit();
        }
    }

    template<class DataType>
    void flush_data(int tid, int dst_sid) {
        uint64_t lbuf_partition_sz = floor(loader_mem.local_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(DataType));
        uint64_t* pn = reinterpret_cast<uint64_t*>((loader_mem.local_buf + loader_mem.local_buf_sz * tid) + (lbuf_partition_sz + sizeof(uint64_t)) * dst_sid);
        DataType* lbuf_partition = reinterpret_cast<DataType*>(pn + 1);

        // the 1st uint64_t of buffer records #new-hyperedges
        uint64_t n = *pn;

        // the kvstore is temporally split into #servers pieces.
        // hence, the kvstore can be directly RDMA write in parallel by all servers
        uint64_t gbuf_partition_sz = floor(loader_mem.global_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(DataType));

        // serialize the RDMA WRITEs by multiple threads
        uint64_t exist = __sync_fetch_and_add(&num_datas[dst_sid], n);

        uint64_t cur_sz = (exist + n) * sizeof(DataType);
        if (cur_sz > gbuf_partition_sz) {
            logstream(LOG_ERROR) << "no enough space to store input data!" << LOG_endl;
            logstream(LOG_ERROR) << " kvstore size = " << gbuf_partition_sz
                                 << " #exist-hyperedges = " << exist
                                 << " #new-hyperedges = " << n
                                 << LOG_endl;
            ASSERT(false);
        }

        // send hyperedges and clear the buffer
        uint64_t off = (gbuf_partition_sz + sizeof(uint64_t)) * sid
                        + sizeof(uint64_t)           // reserve the 1st uint64_t as #hyperedges
                        + exist * sizeof(DataType); // skip #exist-data
        
        uint64_t sz = n * sizeof(DataType);        // send #new-data

        if (dst_sid != sid) {
            RDMA& rdma = RDMA::get_rdma();
            rdma.dev->RdmaWrite(tid, dst_sid, reinterpret_cast<char*>(lbuf_partition), sz, off);
        } else {
            memcpy(loader_mem.global_buf + off, reinterpret_cast<char*>(lbuf_partition), sz);
        }

        // clear the buffer
        *pn = 0;
    }

    // send_hyperedge can be safely called by multiple threads,
    // since the buffer is exclusively used by one thread.
    void send_hyperedge(int tid, int dst_sid, HyperEdge& edge) {
        // the RDMA buffer is first split into #threads partitions
        // each partition is further split into #servers pieces
        // each piece: #hyperedges, triple, triple, . . .
        uint64_t lbuf_partition_sz = floor(loader_mem.local_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(sid_t));
        uint64_t* pn = reinterpret_cast<uint64_t*>((loader_mem.local_buf + loader_mem.local_buf_sz * tid) + (lbuf_partition_sz + sizeof(uint64_t)) * dst_sid);
        sid_t* lbuf_partition = reinterpret_cast<sid_t*>(pn + 1);

        // the 1st entry of buffer records #hyperedges (suppose the )
        uint64_t n = *pn;

        int num_ids = edge.get_num_ids();

        // flush buffer if there is no enough space to buffer a new triple
        uint64_t cur_sz = (n + num_ids) * sizeof(sid_t);
        if (cur_sz > lbuf_partition_sz) {
            flush_data<sid_t>(tid, dst_sid);
            n = *pn;  // reset, it should be 0
            ASSERT(n == 0);
        }

        // buffer the hyperedge and update the counter
        lbuf_partition[n] = edge.edge_type;
        lbuf_partition[n + 1] = edge.id;
        lbuf_partition[n + 2] = edge.vertices.size();
        std::copy(edge.vertices.begin(), edge.vertices.end(), &lbuf_partition[n + 3]);
        *pn = (n + num_ids);
    }

    // send_v2e can be safely called by multiple threads,
    // since the buffer is exclusively used by one thread.
    void send_v2e(int tid, int dst_sid, V2ETriple& triple) {
        // the RDMA buffer is first split into #threads partitions
        // each partition is further split into #servers pieces
        // each piece: #hyperedges, tirple, triple, . . .
        uint64_t lbuf_partition_sz = floor(loader_mem.local_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(V2ETriple));
        uint64_t* pn = reinterpret_cast<uint64_t*>((loader_mem.local_buf + loader_mem.local_buf_sz * tid) + (lbuf_partition_sz + sizeof(uint64_t)) * dst_sid);
        V2ETriple* lbuf_partition = reinterpret_cast<V2ETriple*>(pn + 1);

        // the 1st entry of buffer records #hyperedges
        uint64_t n = *pn;

        // flush buffer if there is no enough space to buffer a new triple
        uint64_t cur_sz = (n + 1) * sizeof(V2ETriple);
        if (cur_sz > lbuf_partition_sz) {
            flush_data<V2ETriple>(tid, dst_sid);
            n = *pn;  // reset, it should be 0
            ASSERT(n == 0);
        }

        // buffer the hyperedge and update the counter
        lbuf_partition[n] = triple;
        *pn = (n + 1);
    }

    int read_partial_exchange(std::map<sid_t, HyperEdgeModel>& edge_models, 
                              std::vector<std::string>& fnames) {
        // ensure the file name list has the same order on all servers
        std::sort(fnames.begin(), fnames.end());
        std::vector<uint64_t> edge_index(Global::num_engines, 1);

        auto lambda = [&](std::istream& file, uint64_t &index_start, int localtid) {
            HyperEdge edge;
            int num_ids, vid;
            char line[100], c;
            std::string name;
            // while (file >> edge.edge_type >> num_ids) {
            //     edge.vertices.resize(num_ids);
            //     for(int i = 0; i < num_ids; i++){
            //         file >> edge.vertices[i];
            //     }
            //     // TODO: more balanced partition
            //     int dst_sid = PARTITION(edge.vertices[0]);
            //     send_hyperedge(localtid, dst_sid, edge);
            // }

            while (file >> name >> edge.edge_type >> std::ws) {
                ASSERT(is_htid(edge.edge_type));

                // skip delimer
                file.get(c);
                ASSERT_EQ(c, '|');

                // read vids
                edge.vertices.clear();
                do {
                    file >> vid >> std::ws;
                    ASSERT(is_hvid(vid));
                    edge.vertices.push_back(vid);
                    c = file.peek();
                } while (c != '|');
            
                // skip delimer
                file.get(c);
                ASSERT_EQ(c, '|');
                
                // skip rest
                file.getline(line, 50);

                // generate heid
                edge.id = generate_heid(sid, localtid, index_start++);

                // record string mapping
                tbb_str_hash_map::accessor a;
                id2str.insert(a, edge.id);
                a->second.assign(name);

                // TODO: more balanced partition
                int dst_sid = PARTITION(edge.vertices[0]);
                send_hyperedge(localtid, dst_sid, edge);
            }
        };

        // init #cnt
        for (int s = 0; s < Global::num_servers; s++) {
            for (int t = 0; t < Global::num_engines; t++) {
                uint64_t lbuf_partition_sz = floor(loader_mem.local_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(sid_t));
                uint64_t* pn = reinterpret_cast<uint64_t*>((loader_mem.local_buf + loader_mem.local_buf_sz * t) + (lbuf_partition_sz + sizeof(uint64_t)) * s);
                *pn = 0;
            }
        }

        // load input data and assign to different severs in parallel
        int num_files = fnames.size();
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int i = 0; i < num_files; i++) {
            int localtid = omp_get_thread_num();

            // each server only load a part of files
            if (i % Global::num_servers != sid) continue;

            std::istream* file = init_istream(fnames[i]);
            lambda(*file, edge_index[localtid], localtid);
            close_istream(file);
        }

        // flush the rest hyperedges within each RDMA buffer
        for (int s = 0; s < Global::num_servers; s++)
            for (int t = 0; t < Global::num_engines; t++)
                flush_data<sid_t>(t, s);

        // exchange #hyperedges among all servers
        for (int s = 0; s < Global::num_servers; s++) {
            uint64_t* lbuf_partition = reinterpret_cast<uint64_t*>(loader_mem.local_buf);
            lbuf_partition[0] = num_datas[s];

            uint64_t gbuf_partition_sz = floor(loader_mem.global_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(sid_t));
            uint64_t offset = (gbuf_partition_sz + sizeof(uint64_t)) * sid;
            if (s != sid) {
                RDMA& rdma = RDMA::get_rdma();
                rdma.dev->RdmaWrite(0, s, reinterpret_cast<char*>(lbuf_partition), sizeof(uint64_t), offset);
            } else {
                memcpy(loader_mem.global_buf + offset, reinterpret_cast<char*>(lbuf_partition), sizeof(uint64_t));
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);

        return Global::num_servers;
    }

    int exchange_v2e_triples(std::vector<std::vector<HyperEdge>>& edges) {
        // init #cnt
        for (int s = 0; s < Global::num_servers; s++) {
            for (int t = 0; t < Global::num_engines; t++) {
                uint64_t lbuf_partition_sz = floor(loader_mem.local_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(V2ETriple));
                uint64_t* pn = reinterpret_cast<uint64_t*>((loader_mem.local_buf + loader_mem.local_buf_sz * t) + (lbuf_partition_sz + sizeof(uint64_t)) * s);
                *pn = 0;
            }
        }

        // start from 1: avoid assign edge id to 0
        #pragma omp parallel for num_threads(Global::num_engines)
        for(int i = 0; i < edges.size(); i++) {
            int localtid = omp_get_thread_num();
            // TODO: send v2e triples
            for(int j = 0; j < edges[i].size(); j++){
                for(int k = 0; k < edges[i][j].vertices.size(); k++){
                    int dst_sid = PARTITION(edges[i][j].vertices[k]);
                    V2ETriple triple;
                    triple.eid = edges[i][j].id;
                    triple.vid = edges[i][j].vertices[k];
                    triple.edge_type = edges[i][j].edge_type;
                    send_v2e(localtid, dst_sid, triple);
                }
            }
        }

        // flush the rest hyperedges within each RDMA buffer
        for (int s = 0; s < Global::num_servers; s++)
            for (int t = 0; t < Global::num_engines; t++)
                flush_data<V2ETriple>(t, s);

        // exchange #hyperedges among all servers
        for (int s = 0; s < Global::num_servers; s++) {
            uint64_t* lbuf_partition = reinterpret_cast<uint64_t*>(loader_mem.local_buf);
            lbuf_partition[0] = num_datas[s];

            uint64_t gbuf_partition_sz = floor(loader_mem.global_buf_sz / Global::num_servers - sizeof(uint64_t), sizeof(V2ETriple));
            uint64_t offset = (gbuf_partition_sz + sizeof(uint64_t)) * sid;
            if (s != sid) {
                RDMA& rdma = RDMA::get_rdma();
                rdma.dev->RdmaWrite(0, s, reinterpret_cast<char*>(lbuf_partition), sizeof(uint64_t), offset);
            } else {
                memcpy(loader_mem.global_buf + offset, reinterpret_cast<char*>(lbuf_partition), sizeof(uint64_t));
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);

        return Global::num_servers;
    }

    // selectively load own partitioned data from all files
    int read_all_files(std::map<sid_t, HyperEdgeModel>& edge_models, 
                       std::vector<std::string>& fnames) {
        std::sort(fnames.begin(), fnames.end());
        std::vector<uint64_t> edge_index(Global::num_engines, 1);

        auto lambda = [&](std::istream& file, uint64_t& n, uint64_t &index_start, 
                            uint64_t gbuf_partition_sz, sid_t* kvs, int localtid) {
            HyperEdge edge;
            int num_ids, vid;
            char line[100], c;
            std::string name;
            // while (file >> edge.edge_type >> num_ids) {
            //     edge.vertices.resize(num_ids);
            //     for(int i = 0; i < num_ids; i++){
            //         file >> edge.vertices[i];
            //     }
            //     // TODO: more balanced partition
            //     int dst_sid = PARTITION(edge.vertices[0]);
            //     if (dst_sid == sid) {
            //         ASSERT((n + edge.get_num_ids()) * sizeof(sid_t) <= gbuf_partition_sz);
            //         // buffer the hyperedge and update the counter
            //         kvs[n] = edge.edge_type;
            //         kvs[n+1] = edge.vertices.size();
            //         std::copy(edge.vertices.begin(), edge.vertices.end(), &kvs[n+2]);
            //         n += edge.get_num_ids();
            //     }
            // }

            while (file >> name >> edge.edge_type >> std::ws) {
                ASSERT(is_htid(edge.edge_type));

                // skip delimer
                file.get(c);
                ASSERT_EQ(c, '|');

                // read vids
                do {
                    file >> vid >> std::ws;
                    ASSERT(is_hvid(vid));
                    edge.vertices.push_back(vid);
                    c = file.peek();
                } while (c != '|');
            
                // skip delimer
                file.get(c);
                ASSERT_EQ(c, '|');
                
                // skip rest
                file.getline(line, 50);

                // generate heid
                edge.id = generate_heid(sid, localtid, index_start++);

                // record string mapping
                tbb_str_hash_map::accessor a;
                id2str.insert(a, edge.id);
                a->second.assign(name);

                // TODO: more balanced partition
                int dst_sid = PARTITION(edge.vertices[0]);
                if (dst_sid == sid) {
                    ASSERT((n + edge.get_num_ids()) * sizeof(sid_t) <= gbuf_partition_sz);
                    // buffer the hyperedge and update the counter
                    kvs[n] = edge.edge_type;
                    kvs[n + 1] = edge.id;
                    kvs[n + 2] = edge.vertices.size();
                    std::copy(edge.vertices.begin(), edge.vertices.end(), &kvs[n + 3]);
                    n += edge.get_num_ids();
                }            
            }
        };

        // init #cnt
        for (int t = 0; t < Global::num_engines; t++) {
            uint64_t gbuf_partition_sz = floor(loader_mem.global_buf_sz / Global::num_engines - sizeof(uint64_t), sizeof(sid_t));
            uint64_t* pn = reinterpret_cast<uint64_t*>(loader_mem.global_buf + (gbuf_partition_sz + sizeof(uint64_t)) * t);
            *pn = 0;
        }

        int num_files = fnames.size();
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int i = 0; i < num_files; i++) {
            int localtid = omp_get_thread_num();
            uint64_t gbuf_partition_sz = floor(loader_mem.global_buf_sz / Global::num_engines - sizeof(uint64_t),
                                    sizeof(sid_t));
            uint64_t* pn = reinterpret_cast<uint64_t*>(loader_mem.global_buf + (gbuf_partition_sz + sizeof(uint64_t)) * localtid);
            sid_t* kvs = reinterpret_cast<sid_t*>(pn + 1);

            // the 1st uint64_t of kvs records #hyperedges
            uint64_t n = *pn;

            std::istream* file = init_istream(fnames[i]);
            lambda(*file, n, edge_index[localtid], gbuf_partition_sz, kvs, localtid);
            close_istream(file);

            *pn = n;
        }

        return Global::num_engines;
    }

    void aggregate_hyperedges(int num_partitions,
                        std::vector<std::vector<HyperEdge>>& hyperedges) {
        // calculate #hyperedges on the kvstore from all servers
        uint64_t total = 0;
        uint64_t gbuf_partition_sz = floor(loader_mem.global_buf_sz / num_partitions - sizeof(uint64_t), sizeof(sid_t));
        for (int i = 0; i < num_partitions; i++) {
            uint64_t* pn = reinterpret_cast<uint64_t*>(loader_mem.global_buf + (gbuf_partition_sz + sizeof(uint64_t)) * i);
            total += *pn;  // the 1st uint64_t of kvs records #hyperedges
        }

        // pre-expand to avoid frequent reallocation (maybe imbalance)
        for (int i = 0; i < hyperedges.size(); i++) {
            hyperedges[i].reserve(total / Global::num_engines);
        }

        // each thread will scan all hyperedges (from all servers) and pickup certain hyperedges.
        // It ensures that the hyperedges belong to the same vertex will be stored in the same
        // triple_pso/ops. This will simplify the deduplication and insertion to gstore.
        volatile uint64_t progress = 0;
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int tid = 0; tid < Global::num_engines; tid++) {
            int cnt = 0;  // per thread count for print progress
            for (int id = 0; id < num_partitions; id++) {
                uint64_t* pn = reinterpret_cast<uint64_t*>(loader_mem.global_buf + (gbuf_partition_sz + sizeof(uint64_t)) * id);
                sid_t* kvs = reinterpret_cast<sid_t*>(pn + 1);

                // the 1st uint64_t of kvs records #hyperedges
                uint64_t n = *pn;
                int i = 0;
                while(i < n) {
                    HyperEdge edge;
                    edge.edge_type = kvs[i];
                    edge.id = kvs[i + 1];
                    int num_ids = kvs[i + 2];

                    if ((kvs[i + 3] % Global::num_engines) == tid){
                        edge.vertices.resize(num_ids);
                        std::copy(&kvs[i + 3], &kvs[i + 3 + num_ids], edge.vertices.begin());
                        hyperedges[tid].push_back(edge);
                    }

                    i += (num_ids + 3);
                    cnt += (num_ids + 3);
                    // print the progress (step = 5%) of aggregation
                    if (cnt >= total * 0.05) {
                        uint64_t now = wukong::atomic::add_and_fetch(&progress, 1);
                        if (now % Global::num_engines == 0)
                            logstream(LOG_INFO) << "[HyperLoader] hyperedge already aggregrate "
                                                << (now / Global::num_engines) * 5
                                                << "%" << LOG_endl;
                        cnt = 0;
                    }
                }
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    void aggregate_v2e_triples(int num_partitions,
                        std::vector<std::vector<V2ETriple>>& v2etriples) {
        // calculate #v2etriples on the kvstore from all servers
        uint64_t total = 0;
        uint64_t gbuf_partition_sz = floor(loader_mem.global_buf_sz / num_partitions - sizeof(uint64_t), sizeof(V2ETriple));
        for (int i = 0; i < num_partitions; i++) {
            uint64_t* pn = reinterpret_cast<uint64_t*>(loader_mem.global_buf + (gbuf_partition_sz + sizeof(uint64_t)) * i);
            total += *pn;  // the 1st uint64_t of kvs records #v2etriples
        }

        // pre-expand to avoid frequent reallocation (maybe imbalance)
        for (int i = 0; i < v2etriples.size(); i++) {
            v2etriples[i].reserve(total / Global::num_engines);
        }

        // each thread will scan all v2etriples (from all servers) and pickup certain hyperedges.
        // It ensures that the v2etriples belong to the same vertex will be stored in the same
        // triple_pso/ops. This will simplify the deduplication and insertion to gstore.
        volatile uint64_t progress = 0;
        #pragma omp parallel for num_threads(Global::num_engines)
        for (int tid = 0; tid < Global::num_engines; tid++) {
            int cnt = 0;  // per thread count for print progress
            for (int id = 0; id < num_partitions; id++) {
                uint64_t* pn = reinterpret_cast<uint64_t*>(loader_mem.global_buf + (gbuf_partition_sz + sizeof(uint64_t)) * id);
                V2ETriple* kvs = reinterpret_cast<V2ETriple*>(pn + 1);

                // the 1st uint64_t of kvs records #triples
                uint64_t n = *pn;
                for (uint64_t i = 0; i < n; i++) {
                    V2ETriple triple = kvs[i];

                    if((triple.vid % Global::num_engines) == tid) {
                        v2etriples[tid].push_back(triple);
                    }

                    // print the progress (step = 5%) of aggregation
                    if (++cnt >= total * 0.05) {
                        uint64_t now = wukong::atomic::add_and_fetch(&progress, 1);
                        if (now % Global::num_engines == 0)
                            logstream(LOG_INFO) << "[HyperLoader] V2ETriple already aggregrate "
                                                << (now / Global::num_engines) * 5
                                                << "%" << LOG_endl;
                        cnt = 0;
                    }
                }
            }
        }
    }

    // Load normal hyperedges from all files, using read_partial_exchange or read_all_files.
    void load_hyperedges_from_all(std::vector<std::string>& dfiles,
                               std::map<sid_t, HyperEdgeModel>& edge_models,
                               std::vector<std::vector<HyperEdge>>& edges,
                               std::vector<std::vector<V2ETriple>>& v2etriples) {
        // read_partial_exchange: load partial input files by each server and exchanges hyperedges
        //            according to graph partitioning
        // read_all_files: load all files by each server and select hyperedges
        //                          according to graph partitioning
        //
        // Trade-off: read_all_files avoids network traffic and memory,
        //            but it requires more I/O from distributed FS.
        //
        // Wukong adopts read_all_files for slow network (w/o RDMA) and
        //        adopts read_partial_exchange for fast network (w/ RDMA).
        uint64_t start = timer::get_usec();
        int num_partitons = 0;
        if (Global::use_rdma)
            num_partitons = read_partial_exchange(edge_models, dfiles);
        else
            num_partitons = read_all_files(edge_models, dfiles);
        uint64_t end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperLoader] #" << sid << ": " << (end - start) / 1000 << " ms "
                            << "for loading data files" << LOG_endl;

        // all hyperedges are partitioned and temporarily stored in the kvstore on each server.
        // the kvstore is split into num_partitions partitions, each contains #hyperedges and hyperedges
        //
        // Wukong aggregates all hyperedges before finally inserting them to gstore (kvstore)
        start = timer::get_usec();
        aggregate_hyperedges(num_partitons, edges);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperLoader] #" << sid << ": " << (end - start) / 1000 << " ms "
                            << "for aggregrating hyperedges" << LOG_endl;

        // refresh num_datas
        for(int i = 0; i < num_datas.size(); i++) {
            num_datas[i] = 0;
        }
    
        // Wukong generate a globally unique id for each hyperedge
        // now we need to send all (vid, heid) triples to each machine
        start = timer::get_usec();
        num_partitons = exchange_v2e_triples(edges);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperLoader] #" << sid << ": " << (end - start) / 1000 << " ms "
                            << "for exchange v2e triples" << LOG_endl;

        // all v2e triples are partitioned and temporarily stored in the kvstore on each server.
        // the kvstore is split into num_partitions partitions, each contains #cnt and v2e triples
        //
        // Wukong aggregates all v2e triples before finally inserting them to gstore (kvstore)
        start = timer::get_usec();
        aggregate_v2e_triples(num_partitons, v2etriples);
        end = timer::get_usec();
        logstream(LOG_INFO) << "[HyperLoader] #" << sid << ": " << (end - start) / 1000 << " ms "
                            << "for aggregrating v2e triples" << LOG_endl;
    }

public:
    HyperGraphBaseLoader(int sid, LoaderMem loader_mem)
        : sid(sid), loader_mem(loader_mem) {}

    virtual ~HyperGraphBaseLoader() {}

    virtual std::istream* init_istream(const std::string& src) = 0;
    virtual void close_istream(std::istream* stream) = 0;
    virtual std::vector<std::string> list_files(const std::string& src, std::string prefix) = 0;

    void load(const std::string& src,
              StringServer *str_server,
              std::map<sid_t, HyperEdgeModel>& edge_models,
              std::vector<std::vector<HyperEdge>>& edges,
              std::vector<std::vector<V2ETriple>>& v2etriples) {
        uint64_t start, end;

        num_datas.resize(Global::num_servers, 0);
        edges.resize(Global::num_engines);
        v2etriples.resize(Global::num_engines);

        // ID-format data files
        std::vector<std::string> dfiles(list_files(src, "hyper_id_"));

        if (dfiles.size() == 0) {
            logstream(LOG_WARNING) << "[HyperLoader] no data files found in directory (" << src
                                   << ") at server " << sid << LOG_endl;
        } else {
            logstream(LOG_INFO) << "[HyperLoader] " << dfiles.size() << " files found in directory (" 
                                << src << ") at server " << sid << LOG_endl;
        }

        load_hyperedges_from_all(dfiles, edge_models, edges, v2etriples);

        // add hyperedge name mapping into str_server
        for (auto &&pair : id2str) str_server->add_he(pair.second, pair.first);
        id2str.clear();

        // log meta info
        // for(int i = 0; i < edges.size(); i++) {
        //     std::cout << "#" << sid << " Engine [" << i << "]" << std::endl;
        //     std::cout << "#" << sid << " edge size:" << edges[i].size() << std::endl;
        //     std::cout << "#" << sid << " v2etriple size:" << v2etriples[i].size() << std::endl;
        // }
        // Wukong sorts and dedups all hyperedges before finally inserting them to gstore (kvstore)
        sort_data<HyperEdge, hyperedge_sort>(edges);
        sort_data<V2ETriple, v2etriple_sort>(v2etriples);
    }
};


class HyperGraphPosixLoader : public HyperGraphBaseLoader {
public:
    HyperGraphPosixLoader(int sid, LoaderMem loader_mem)
        : HyperGraphBaseLoader(sid, loader_mem) {}

    ~HyperGraphPosixLoader() {}

    std::istream* init_istream(const std::string& src) {
        return new std::ifstream(src.c_str());
    }

    void close_istream(std::istream* stream) {
        static_cast<std::ifstream*>(stream)->close();
        delete stream;
    }

    std::vector<std::string> list_files(const std::string& src, std::string prefix) {
        DIR* dir = opendir(src.c_str());
        if (dir == NULL) {
            logstream(LOG_ERROR) << "failed to open directory (" << src
                                 << ") at server " << sid << LOG_endl;
            exit(-1);
        }

        std::vector<std::string> files;
        struct dirent* ent;
        while ((ent = readdir(dir)) != NULL) {
            if (ent->d_name[0] == '.')
                continue;

            std::string fname(src + ent->d_name);
            // Assume the fnames (ID-format) start with the prefix.
            if (boost::starts_with(fname, src + prefix))
                files.push_back(fname);
        }
        return files;
    }
};


class HyperGraphHDFSLoader : public HyperGraphBaseLoader {
public:
    HyperGraphHDFSLoader(int sid, LoaderMem loader_mem)
        : HyperGraphBaseLoader(sid, loader_mem) {}

    ~HyperGraphHDFSLoader() {}

    std::istream* init_istream(const std::string& src) {
        wukong::hdfs& hdfs = wukong::hdfs::get_hdfs();
        return new wukong::hdfs::fstream(hdfs, src);
    }

    void close_istream(std::istream* stream) {
        static_cast<wukong::hdfs::fstream*>(stream)->close();
        delete stream;
    }

    std::vector<std::string> list_files(const std::string& src, std::string prefix) {
        if (!wukong::hdfs::has_hadoop()) {
            logstream(LOG_ERROR) << "attempting to load data files from HDFS "
                                 << "but Wukong was built without HDFS." << LOG_endl;
            exit(-1);
        }

        wukong::hdfs& hdfs = wukong::hdfs::get_hdfs();
        return std::vector<std::string>(hdfs.list_files(src, prefix));
    }
};

}  // namespace wukong
