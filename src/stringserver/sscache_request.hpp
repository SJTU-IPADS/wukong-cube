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
#include <unordered_map>
#include <vector>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

#include "core/common/type.hpp"

namespace wukong {

enum class SSCacheReqType { TRANS_STR = 0,
                            TRANS_ID = 1,
                            LOAD_MAPPING = 2 };

/**
 * String<->ID mapping request
 */
class SSCacheRequest {
private:
    friend class boost::serialization::access;

    template <typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& req_type;
        ar& str;
        ar& vid;
        ar& success;
    }

public:
    SSCacheReqType req_type = SSCacheReqType::TRANS_STR;
    std::string str;
    sid_t vid;
    bool success = false;

    SSCacheRequest() {}

    // used for sscache send TRANS_ID requests
    explicit SSCacheRequest(sid_t vid)
        : req_type(SSCacheReqType::TRANS_ID), vid(vid) {}

    // used for sscache send TRANS_STR requests
    explicit SSCacheRequest(std::string str)
        : req_type(SSCacheReqType::TRANS_STR), str(str) {}
};

}  // namespace wukong

BOOST_CLASS_IMPLEMENTATION(wukong::SSCacheRequest, boost::serialization::object_serializable);
BOOST_CLASS_TRACKING(wukong::SSCacheRequest, boost::serialization::track_never);