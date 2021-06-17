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

class HyperEdge {
public:
    sid_t edge_type; 
    std::vector<sid_t> vertices;
    // TODO: attribue
};

}