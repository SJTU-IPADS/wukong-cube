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

#include <stdint.h>
#include <vector>

#include "type.hpp"

namespace wukong {

using heid_t = uint64_t;

class HyperEdgeModel {
public:
    sid_t type_id;
    int index_num;
    std::vector<sid_t> index_type_hint;
    std::vector<int> index_size;
};

struct V2ETriple {
    heid_t eid;
    sid_t vid;
    int index;

    bool operator==(const V2ETriple& triple) const {
        // TODO
        return false;
    }
};

class HyperEdge {
public:
    heid_t id;
    sid_t edge_type; 
    std::vector<sid_t> vertices;
    // TODO: attribue

    int get_num_ids() {
        // edge_type + id_num + ids
        return vertices.size() + 2;
    }

    bool operator==(const HyperEdge& edge) const {
        // TODO
        return false;
    }
};

struct v2etriple_sort {
    inline bool operator()(const V2ETriple &t1, const V2ETriple &t2) {
        // TODO
        return false;
    }
};

struct hyperedge_sort {
    inline bool operator()(const HyperEdge &e1, const HyperEdge &e2) {
        // TODO
        return false;
    }
};

}