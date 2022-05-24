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

#include <string>
#include <utility>
#include <vector>

#include "core/common/type.hpp"

namespace wukong {

class StringMapping {
public:
    // the data type of predicate/attribute: sid=0, integer=1, float=2, double=3
    boost::unordered_map<sid_t, char> pid2type;

    virtual std::pair<bool, std::string> id2str(int tid, sid_t sid) = 0;
    virtual std::pair<bool, sid_t> str2id(int tid, std::string str) = 0;
    virtual bool add(std::string str, sid_t sid) = 0;

    virtual ~StringMapping() {}
};

}  // namespace wukong
