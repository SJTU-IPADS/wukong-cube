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

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/string.hpp>

#include "core/common/errors.hpp"
#include "core/common/type.hpp"

#include "core/sparql/query.hpp"

#include "stringserver/sscache_request.hpp"

// utils
#include "utils/assertion.hpp"
#include "utils/logger2.hpp"

namespace wukong {

/**
 * Bundle to be sent by network, with data type labeled
 * Note this class does not use boost serialization
 */
class Bundle {
private:
    friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & type;
        ar & data;
    }

public:
    req_type type;
    std::string data;

    Bundle() { }

    Bundle(const req_type &t, const std::string &d): type(t), data(d) { }

    Bundle(const Bundle &b): type(b.type), data(b.data) { }

    Bundle(const SPARQLQuery &r): type(SPARQL_QUERY) {
        std::stringstream ss;
        boost::archive::binary_oarchive oa(ss);

        oa << r;
        data = ss.str();
    }

    Bundle(const RDFLoad &r): type(DYNAMIC_LOAD) {
        std::stringstream ss;
        boost::archive::binary_oarchive oa(ss);

        oa << r;
        data = ss.str();
    }

    Bundle(const GStoreCheck &r): type(GSTORE_CHECK) {
        std::stringstream ss;
        boost::archive::binary_oarchive oa(ss);

        oa << r;
        data = ss.str();
    }

    Bundle(const SSCacheRequest &r): type(SSCACHE_REQ) {
        std::stringstream ss;
        boost::archive::binary_oarchive oa(ss);

        oa << r;
        data = ss.str();
    }

    Bundle(const std::string str) { init(str); }

    void init(const std::string str) {
        memcpy(&type, str.c_str(), sizeof(req_type));
        std::string d(str, sizeof(req_type), str.length() - sizeof(req_type));
        data = d;
    }

    // SPARQLQuery command
    SPARQLQuery get_sparql_query() const {
        ASSERT(type == SPARQL_QUERY);

        std::stringstream ss;
        ss << data;

        boost::archive::binary_iarchive ia(ss);
        SPARQLQuery result;
        ia >> result;
        return result;
    }

    // RDFLoad command
    RDFLoad get_rdf_load() const {
        ASSERT(type == DYNAMIC_LOAD);

        std::stringstream ss;
        ss << data;

        boost::archive::binary_iarchive ia(ss);
        RDFLoad result;
        ia >> result;
        return result;
    }

    // GStoreCheck command
    GStoreCheck get_gstore_check() const {
        ASSERT(type == GSTORE_CHECK);

        std::stringstream ss;
        ss << data;

        boost::archive::binary_iarchive ia(ss);
        GStoreCheck result;
        ia >> result;
        return result;
    }

    SSCacheRequest get_sscache_req() const {
        ASSERT(type == SSCACHE_REQ);

        std::stringstream ss;
        ss << data;

        boost::archive::binary_iarchive ia(ss);
        SSCacheRequest result;
        ia >> result;
        return result;
    }

    std::string to_str() const {
#if 1 // FIXME
        char *c_str = new char[sizeof(req_type) + data.length()];
        memcpy(c_str, &type, sizeof(req_type));
        memcpy(c_str + sizeof(req_type), data.c_str(), data.length());
        std::string str(c_str, sizeof(req_type) + data.length());
        delete []c_str;
        return str;
#else
        // FIXME: why not work? (Rong)
        return std::string(std::to_string((uint64_t)type) + data);
#endif
    }

};

} // namespace wukong