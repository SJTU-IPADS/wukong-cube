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
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/unordered_map.hpp>

#include "client/rpc_client.hpp"

#include "stringserver/sscache_request.hpp"
#include "stringserver/string_mapping.hpp"

#include "utils/assertion.hpp"
#include "utils/hdfs.hpp"

#include "core/common/bundle.hpp"

namespace wukong {

class Cache {
private:
    // cache statistics
    uint64_t total_miss;
    uint64_t total_hit;

    // lock for multiple threads r/w cache
    pthread_spinlock_t cache_lock;

    // Cache
    boost::unordered_map<std::string, sid_t> simap;  // STRING to ID
    boost::unordered_map<sid_t, std::string> ismap;  // ID to STRING

    // cache size to be determined
    const uint32_t capacity;

public:
    explicit Cache(uint32_t max_cache_size) : capacity(max_cache_size) {
        pthread_spin_init(&cache_lock, 0);

        total_miss = 0;
        total_hit = 0;

        simap.reserve(capacity);
        ismap.reserve(capacity);
    }
    ~Cache() {}

    bool cached(std::string str) {
        pthread_spin_lock(&cache_lock);
        auto it = simap.find(str);
        bool iscached = (it != simap.end());
        if (iscached) {
            touch(it->second);
        }
        pthread_spin_unlock(&cache_lock);
        return iscached;
    }

    bool cached(sid_t vid) {
        pthread_spin_lock(&cache_lock);
        auto it = ismap.find(vid);
        bool iscached = (it != ismap.end());
        if (iscached) {
            touch(vid);
        }
        pthread_spin_unlock(&cache_lock);
        return iscached;
    }

    // @return true given vid is cached if cached, otherwise false.
    std::pair<bool, std::string> get_if_cached(sid_t vid) {
        std::string get_str;
        pthread_spin_lock(&cache_lock);
        auto it = ismap.find(vid);
        bool iscached = (it != ismap.end());
        if (iscached) {
            get_str = it->second;
            touch(vid);
        }
        pthread_spin_unlock(&cache_lock);
        return std::make_pair(iscached, get_str);
    }

    std::pair<bool, sid_t> get_if_cached(std::string str) {
        sid_t get_sid;
        pthread_spin_lock(&cache_lock);
        auto it = simap.find(str);
        bool iscached = (it != simap.end());
        if (iscached) {
            get_sid = it->second;
            touch(it->second);
        }
        pthread_spin_unlock(&cache_lock);
        return std::make_pair(iscached, get_sid);
    }

    void update(sid_t vid, std::string str) {
        pthread_spin_lock(&cache_lock);
        // don't exsit in cache
        if (ismap.find(vid) == ismap.end()) {
            // cache is full
            if (ismap.size() >= capacity) {
                sid_t evict_vid = evict_old_item();
                simap.erase(ismap[evict_vid]);
                ismap.erase(evict_vid);
                exit(evict_vid);
            }
            simap[str] = vid;
            ismap[vid] = str;
            enter(vid);
        } else {
            // already exsit in cache, so update previous cache item
            simap[str] = vid;
            ismap[vid] = str;
        }
        pthread_spin_unlock(&cache_lock);
    }

    virtual void touch(sid_t vid) = 0;
    virtual void enter(sid_t vid) = 0;
    virtual void exit(sid_t vid) = 0;
    virtual sid_t evict_old_item() = 0;
};

class FIFOCache : public Cache {
private:
    std::list<sid_t> fifo_queue;

public:
    explicit FIFOCache(uint32_t max_cache_size) : Cache(max_cache_size) {}
    ~FIFOCache() {}

    // FIFO strategy cache do noting when item is touched
    void touch(sid_t vid) override {}
    void enter(sid_t vid) override { fifo_queue.emplace_front(vid); }
    void exit(sid_t vid) override { fifo_queue.pop_back(); }
    sid_t evict_old_item() override { return fifo_queue.back(); }
};

class LRUCache : public Cache {
private:
    std::list<sid_t> lru_queue;
    std::unordered_map<sid_t, std::list<sid_t>::iterator> key_finder;

public:
    explicit LRUCache(uint32_t max_cache_size) : Cache(max_cache_size) {}
    ~LRUCache() {}

    void touch(sid_t vid) override {
        // move the touched element at the beginning of the lru_queue
        lru_queue.splice(lru_queue.begin(), lru_queue, key_finder[vid]);
    }
    void enter(sid_t vid) override {
        lru_queue.emplace_front(vid);
        key_finder[vid] = lru_queue.begin();
    }
    void exit(sid_t vid) override {
        // remove the least recently used element
        key_finder.erase(lru_queue.back());
        lru_queue.pop_back();
    }
    sid_t evict_old_item() override { return lru_queue.back(); }
};

class StringRPCClient : public RPCClient {
public:
    StringRPCClient() {}

    ~StringRPCClient() {}

    /**
     * @brief Send String Request to RPC server.
     * 
     * @param req the string request
     * @param timeout RPC timeout
     * 
     * @return request result.
     */
    Status execute_string_request(SSCacheRequest& req, int timeout = ConnectTimeoutMs) {
        if (timeout <= 0) timeout = ConnectTimeoutMs;
        // call info RPC
        Bundle bundle(req);
        std::string reply_msg;
        int ret = cl->call(RPC_CODE::STRING_RPC, reply_msg, timeout, bundle.to_str());
        ASSERT_GE(ret, 0);
        // show cluster info
        std::stringstream ss(reply_msg);
        req_type type;
        ss.read(reinterpret_cast<char*>(&type), sizeof(req_type));
        ASSERT(type == req_type::SSCACHE_REQ);
        boost::archive::binary_iarchive ia(ss);
        ia >> req;
        return Status::OK();
    }
};

class StringCache : public StringMapping {
private:
    // size: Global::num_threads
    std::vector<StringRPCClient> rpc_clients;

    // we store index string map in every machine
    std::unordered_map<std::string, sid_t> index_simap;
    std::unordered_map<sid_t, std::string> index_ismap;

    std::shared_ptr<Cache> cache;

    const sid_t start_normal_id = 1 << NBITS_IDX;


    bool is_index_id(sid_t id) { return id < start_normal_id; }

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
            std::ifstream file(fname.c_str());
            logstream(LOG_INFO) << "[StringCache] loading ID-mapping file: " << fname << LOG_endl;
            
            if (boost::ends_with(fname, "/str_index")) {
                load_index_file(file);
            }

            if (boost::ends_with(fname, "/str_attr_index")) {
                load_attr_index_file(file);
            }
            file.close();
        }
    }

    /* load ID mapping files from HDFS */
    void load_from_hdfs(std::string dname) {
        wukong::hdfs& hdfs = wukong::hdfs::get_hdfs();
        std::vector<std::string> files = hdfs.list_files(dname);

        for (int i = 0; i < files.size(); i++) {
            std::string fname = files[i];
            wukong::hdfs::fstream file(hdfs, fname);
            logstream(LOG_INFO) << "[StringCache] loading ID-mapping file from HDFS: " << fname << LOG_endl;
            // NOTE: users may use a short path (w/o ip:port)
            // e.g., hdfs:/xxx/xxx/
            if (boost::ends_with(fname, "/str_index")) {
                load_index_file(file);
            }

            if (boost::ends_with(fname, "/str_attr_index")) {
                load_attr_index_file(file);
            }
            file.close();
        }
    }

    void load_index_file(std::istream& file) {
        std::string str;
        sid_t id;
        while (file >> str >> id) {
            // add a new ID-STRING (bi-direction) pair
            index_simap[str] = id;
            index_ismap[id] = str;
            pid2type[id] = static_cast<char>(SID_t);
        }
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
            index_simap[str] = id;
            index_ismap[id] = str;
            pid2type[id] = static_cast<char>(type);

            logstream(LOG_INFO) << " attribute[" << id << "] = " << type << LOG_endl;
        }
    }

public:
    static int cache_capacity;

    explicit StringCache(std::string dname)
        : rpc_clients(Global::num_threads) {
        cache = std::make_shared<FIFOCache>(cache_capacity);

        // parse host and port from string server addr
        std::string addr = Global::standalone_str_server_addr;
        std::string host = addr.substr(0, addr.find(":"));
        int port = std::stoi(addr.substr(addr.find(":") + 1));
        std::cout << "connect to string server: " << host << ":" << port << std::endl;
        for (auto& client : rpc_clients) {
            client.connect_to_server(host, port);
        }

        uint64_t start = timer::get_usec();

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
        logstream(LOG_INFO) << "loading string cache is finished ("
                            << (end - start) / 1000 << " ms)" << LOG_endl;
    }

    ~StringCache() {
        for (auto& client : rpc_clients) {
            client.disconnect();
        }
    }

    std::pair<bool, std::string> id2str(int tid, sid_t vid) override {
        if (is_index_id(vid)) {
            if (index_ismap.count(vid) != 0) {
                return std::make_pair(true, index_ismap[vid]);
            } else {
                return std::make_pair(false, "");
            }
        }
        auto cache_result = cache->get_if_cached(vid);
        if (cache_result.first) {
            return std::make_pair(true, cache_result.second);
        } else {
            int dst_tid = rand() % Global::num_threads;
            SSCacheRequest request(vid);

            rpc_clients[tid].execute_string_request(request);
            if (request.success) {
                logstream(LOG_INFO) << "String translation success!!" << LOG_endl;
                cache->update(vid, request.str);
                return std::make_pair(true, request.str);
            } else {
                return std::make_pair(false, "");
            }
        }
    }

    std::pair<bool, sid_t> str2id(int tid, std::string str) override {
        if (index_simap.count(str) != 0) {
            return std::make_pair(true, index_simap[str]);
        }
        auto cache_result = cache->get_if_cached(str);
        if (cache_result.first) {
            logstream(LOG_INFO) << "Found:" << str << " in cache." << LOG_endl;
            return std::make_pair(true, cache_result.second);
        } else {
            logstream(LOG_INFO) << "Not found:" << str << " in cache." << LOG_endl;
            int dst_tid = rand() % Global::num_threads;
            SSCacheRequest request(str);

            rpc_clients[tid].execute_string_request(request);
            if (request.success) {
                logstream(LOG_INFO) << "String translation success!!" << LOG_endl;
                cache->update(request.vid, str);
                return std::make_pair(true, request.vid);
            } else {
                return std::make_pair(false, 0);
            }
        }
    }

    bool add(std::string str, sid_t vid) override {
        logstream(LOG_ERROR) << "Not supported now!" << LOG_endl;
        ASSERT(false);
        return false;
    }
};

    int StringCache::cache_capacity = 10000;

}  // namespace wukong
