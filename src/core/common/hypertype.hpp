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
};

struct V2ETriple {
    heid_t eid;
    sid_t vid;
    sid_t edge_type;

    bool operator==(const V2ETriple& triple) const {
        if ((eid == triple.eid) && 
            (vid == triple.vid) && 
            (edge_type == triple.edge_type)) {
            return true;
        }
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
        // edge_type + edge_id + id_num + ids
        return vertices.size() + 3;
    }

    bool operator==(const HyperEdge& edge) const {
        if ((id == edge.id) && (edge_type == edge.edge_type) && (vertices.size() == edge.vertices.size())) {
            for (int i = 0; i < vertices.size(); i++) {
                if (vertices[i] != edge.vertices[i]) return false;
            }
            return true;
        }
        return false;
    }

    // for debugging
    void print_he(int tid) const {
        printf("[tid %d ]HyperEdge %lu: type = %u, vids = ", tid, id, edge_type);
        for (auto &&vid : vertices) printf("%u ", vid);
        printf("\n");
    }
};

struct v2etriple_sort {
    inline bool operator()(const V2ETriple& t1, const V2ETriple& t2) {
        if (t1.vid < t2.vid)
            return true;

        if (t1.vid == t2.vid && t1.edge_type < t2.edge_type)
            return true;

        if (t1.vid == t2.vid &&
            t1.edge_type == t2.edge_type &&
            t1.eid < t2.eid)
            return true;

        return false;
    }
};

struct hyperedge_sort {
    inline bool operator()(const HyperEdge& e1, const HyperEdge& e2) {
        return e1.id < e2.id;
    }
};

}  // namespace wukong
