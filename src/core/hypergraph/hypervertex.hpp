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
#include <sstream>
#include <string>

#include "core/store/vertex.hpp"

// utils
#include "utils/math.hpp"

namespace wukong {

// Support 8 vertex role in hyperedge 
// (Specfic for normal edge, IN:0/OUT:1)
#define NBITS_IDX   3 
// equal to the size of vertex/edge type
#define NBITS_ETYPE  13 
// 0: index vertex, ID: normal vertex/(hyperedge)
#define NBITS_ID    48

// reserve two special index IDs (predicate and type)
enum { EDGE_TYPE = 0,
       VERTEX_TYPE = 1 };

/**
 * hypergraph-friendly key/value store
 * key: id | type | index
 * value: heid/tid list
 */
struct hvkey_t {
    uint64_t idx  : NBITS_IDX;  // index
    uint64_t type : NBITS_ETYPE;   // vertex/edge type
    uint64_t id   : NBITS_ID;     // vertex/edge id

    hvkey_t() : id(0), type(0), idx(0) {}

    hvkey_t(uint64_t id, uint64_t type, uint64_t idx) : id(id), type(type), idx(idx) {
        assert((this->id == id) && (this->type == type) && (this->idx == idx));  // no key truncate
    }

    hvkey_t& operator=(uint64_t index) {
        id = index;
        return *this;
    }

    uint64_t operator*(uint64_t op_num) {
        return id * op_num;
    }

    uint64_t get_bucket_index() {
        return this->id;
    }

    bool operator==(const hvkey_t& key) const {
        if ((id == key.id) && (type == key.type) && (idx == key.idx))
            return true;
        return false;
    }

    bool operator!=(const hvkey_t& key) const { return !(operator==(key)); }

    bool is_empty() { return ((id == 0) && (type == 0) && (idx == 0)); }

    void print_key() { std::cout << "[" << id << "|" << type << "|" << idx << "]" << std::endl; }

    std::string to_string() {
        std::ostringstream ss;
        ss << "[" << id << "|" << type << "|" << idx << "]";
        return ss.str();
    }

    uint64_t hash() const {
        uint64_t r = 0;
        r += id;
        r <<= NBITS_ETYPE;
        r += type;
        r <<= NBITS_IDX;
        r += idx;
        // the standard hash is too slow
        // (i.e., std::hash<uint64_t>()(r))
        return wukong::math::hash_u64(r);
    }
};

struct hekey_t {
    heid_t id;  // hyperedge id

    hekey_t() : id(0) {}

    hekey_t(heid_t id) : id(id) {}

    hekey_t& operator=(uint64_t index) {
        id = index;
        return *this;
    }

    uint64_t operator*(uint64_t op_num) {
        return id * op_num;
    }

    uint64_t get_bucket_index() {
        return this->id;
    }

    bool operator==(const hekey_t& key) const {
        return (id == key.id);
    }

    bool operator!=(const hekey_t& key) const { return !(operator==(key)); }

    bool is_empty() { return (id == 0); }

    std::string to_string() {
        std::ostringstream ss;
        ss << "[" << id << "]";
        return ss.str();
    }

    uint64_t hash() const {
        // the standard hash is too slow
        // (i.e., std::hash<uint64_t>()(r))
        return wukong::math::hash_u64(id);
    }
};


struct hvkey_Hasher {
    static size_t hash(const hvkey_t& k) {
        return k.hash();
    }

    static bool equal(const hvkey_t& x, const hvkey_t& y) {
        return x.operator==(y);
    }
};

struct hekey_Hasher {
    static size_t hash(const hekey_t& k) {
        return k.hash();
    }

    static bool equal(const hekey_t& x, const hekey_t& y) {
        return x.operator==(y);
    }
};

}  // namespace wukong
