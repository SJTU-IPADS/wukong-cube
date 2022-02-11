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

#include <string>
#include <vector>

#include "core/common/type.hpp"
#include "core/common/hypertype.hpp"
#include "core/common/string_server.hpp"

namespace wukong {

/* Memory region used during loading*/
struct LoaderMem {
    // NOTICE: we assume global_buf is the start of global RDMA memory region
    // aggregate triples from all servers (global_buffer_sz*1)
    char* global_buf;
    uint64_t global_buf_sz;
    // aggregate triples read by each thread (local_buffer_sz*thread_num)
    char* local_buf;
    uint64_t local_buf_sz;
};

class RDFLoaderInterface {
public:
    virtual void load(const std::string& src,
                      std::vector<std::vector<triple_t>>& triple_pso,
                      std::vector<std::vector<triple_t>>& triple_pos,
                      std::vector<std::vector<triple_attr_t>>& triple_sav) = 0;

    virtual ~RDFLoaderInterface() {}
};

class HyperGraphLoaderInterface {
public:
    virtual void load(const std::string& src,
                      StringServer *str_server,
                      std::map<sid_t, HyperEdgeModel>& edge_models,
                      std::vector<std::vector<HyperEdge>>& edges,
                      std::vector<std::vector<V2ETriple>>& v2etriples) = 0;

    virtual ~HyperGraphLoaderInterface() {}
};

}  // namespace wukong
