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
#include <stdio.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/mpi.hpp>
#include <boost/unordered_map.hpp>

#include "core/common/bundle.hpp"
#include "core/common/global.hpp"
#include "core/common/type.hpp"

#include "stringserver/sscache_request.hpp"
#include "stringserver/string_mapping.hpp"

// utils
#include "utils/assertion.hpp"
#include "utils/hdfs.hpp"
#include "utils/timer.hpp"

// #define USE_BITRIE  // use bi-tire to store ID-STR mapping (reduce memory usage)
#ifdef USE_BITRIE
#include "utils/bitrie.hpp"
#endif

namespace wukong {

class StringServer : public StringMapping {
private:
#ifdef USE_BITRIE
    bitrie<char, sid_t> bimap;  // ID-STRING (bi-)map
#else
    boost::unordered_map<std::string, sid_t> simap;  // STRING to ID
    boost::unordered_map<sid_t, std::string> ismap;  // ID to STRING
#endif

public:
    sid_t next_index_id;
    sid_t next_normal_id;

    explicit StringServer(std::string dname) {
        uint64_t start = timer::get_usec();

        next_index_id = 0;
        next_normal_id = 0;

        if (boost::starts_with(dname, "hdfs:")) {
            if (!wukong::hdfs::has_hadoop()) {
                logstream(LOG_ERROR) << "attempting to load ID-mapping files from HDFS "
                                     << "but Wukong was built without HDFS."
                                     << LOG_endl;
                exit(-1);
            }
            load_from_hdfs(dname);
        } else {
            load_from_posixfs(dname);
        }

        uint64_t end = timer::get_usec();
        logstream(LOG_INFO) << "loading string server is finished ("
                            << (end - start) / 1000 << " ms)" << LOG_endl;
    }

#ifdef USE_BITRIE
    std::pair<bool, std::string> id2str(int tid, sid_t vid) override {
        if (bimap.exist(vid)) {
            return std::make_pair(true, bimap[vid]);
        } else {
            return std::make_pair(false, "");
        }
    }

    std::pair<bool, sid_t> str2id(int tid, std::string str) override {
        if (bimap.exist(str)) {
            return std::make_pair(true, bimap[str]);
        } else {
            return std::make_pair(false, 0);
        }
    }

    bool add(std::string str, sid_t vid) override {
        bimap.insert_kv(str, vid);
        return true;
    }

    void shrink() { bimap.storage_resize(); }
#else
    std::pair<bool, std::string> id2str(int tid, sid_t vid) override {
        auto ptr = ismap.find(vid);
        if (ptr != ismap.end()) {
            return std::make_pair(true, ismap[vid]);
        } else {
            return std::make_pair(false, "");
        }
    }

    std::pair<bool, sid_t> str2id(int tid, std::string str) override {
        auto ptr = simap.find(str);
        if (ptr != simap.end()) {
            return std::make_pair(true, simap[str]);
        } else {
            return std::make_pair(false, 0);
        }
    }

    bool add(std::string str, sid_t vid) override {
        simap[str] = vid;
        ismap[vid] = str;
        return true;
    }

    void shrink() {}
#endif

private:
    /* load ID mapping files from a shared filesystem (e.g., NFS) */
    void load_from_posixfs(std::string dname) {
        DIR* dir = opendir(dname.c_str());
        if (dir == NULL) {
            logstream(LOG_ERROR) << "failed to open the directory of ID-mapping files ("
                                 << dname << ")." << LOG_endl;
            exit(-1);
        }

        struct dirent* ent;
        while ((ent = readdir(dir)) != NULL) {
            if (ent->d_name[0] == '.')
                continue;

            std::string fname(dname + ent->d_name);

            if (boost::ends_with(fname, "/str_normal")) {
                logstream(LOG_INFO) << "[StringServer] loading ID-mapping file: " << fname << LOG_endl;
                std::ifstream file(fname.c_str());
                load_normal_or_index_file(file, true);
                file.close();
            }

            if (boost::ends_with(fname, "/str_index")) {
                logstream(LOG_INFO) << "[StringServer] loading string index file: " << fname << LOG_endl;
                std::ifstream file(fname.c_str());
                load_normal_or_index_file(file, false);
                file.close();
            }

            if (boost::ends_with(fname, "/str_attr_index")) {
                std::ifstream file(fname.c_str());
                load_attr_index_file(file);
                file.close();
            }
        }

        shrink();  // save memory
    }

    /* load ID mapping files from HDFS */
    void load_from_hdfs(std::string dname) {
        wukong::hdfs& hdfs = wukong::hdfs::get_hdfs();
        std::vector<std::string> files = hdfs.list_files(dname);

        for (int i = 0; i < files.size(); i++) {
            std::string fname = files[i];
            wukong::hdfs::fstream file(hdfs, fname);
            logstream(LOG_INFO) << "[StringServer] loading ID-mapping file from HDFS: " << fname << LOG_endl;

            // NOTE: users may use a short path (w/o ip:port)
            // e.g., hdfs:/xxx/xxx/
            if (boost::ends_with(fname, "/str_normal")) {
                load_normal_or_index_file(file, true);
            }

            if (boost::ends_with(fname, "/str_index")) {
                load_normal_or_index_file(file, false);
            }

            if (boost::ends_with(fname, "/str_attr_index")) {
                load_attr_index_file(file);
            }
            file.close();
        }

        shrink();  // save memory
    }

    void load_normal_or_index_file(std::istream& file, bool normal) {
        std::string str;
        sid_t id;
        while (file >> str >> id) {
            // add a new ID-STRING (bi-direction) pair
            add(str, id);
            if (!normal) pid2type[id] = static_cast<char>(SID_t);
        }
        if (!normal)
            next_index_id = ++id;
        else
            next_normal_id = ++id;
    }

    /**
     * @brief load the attribute index from the str_attr_index file
     * it contains by (string, predicate-ID, predicate-type)
     * predicate type: SID_t, INT_t, FLOAT_t, DOUBLE_t
     * 
     * NOTE: the predicates/attributes in str_attr_index should be exclusive
     * to the predicates/attributes in str_index
     * 
     * @param file 
     */
    void load_attr_index_file(std::istream& file) {
        std::string str;
        sid_t id;
        char type;
        while (file >> str >> id >> type) {
            // add a new ID-STRING (bi-direction) pair
            add(str, id);
            pid2type[id] = static_cast<char>(type);

            // FIXME: dynamic loading (next_index_id)
            logstream(LOG_INFO) << " attribute[" << id << "] = " << type << LOG_endl;
        }
    }
};

}  // namespace wukong
