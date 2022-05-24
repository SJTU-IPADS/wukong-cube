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

#include "stringserver/string_cache.hpp"
#include "stringserver/sscache_request.hpp"

#include "utils/assertion.hpp"

// string mappings:
// case 1 (<http://www.Department15.University3.edu>, 131073)
// case 2 ("UndergraduateStudent304", 131077)

static void usage(char* fn) {
    std::cout << "usage: " << fn << " <string server port> [options]" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    // parse argv
    int port = atoi(argv[1]);

    // initialize client
    wukong::StringRPCClient client;
    client.connect_to_server("0.0.0.0", port);

    // send STRING request1(id -> str)
    wukong::SSCacheRequest req1(131073);
    wukong::Status result = client.execute_string_request(req1);
    if (!result.ok()) 
        std::cout << "Request1(id -> str) fail." << std::endl;
    else if (req1.str.compare("<http://www.Department15.University3.edu>") == 0)
        std::cout << "Request1(id -> str) success." << std::endl;
    else
        std::cout << "Request1(id -> str) wrong result \"" << req1.str << "\"." << std::endl;

    // send STRING request1(id -> str)
    wukong::SSCacheRequest req2("\"UndergraduateStudent304\"");
    result = client.execute_string_request(req2);
    if (!result.ok()) 
        std::cout << "Request2(str -> id) fail." << std::endl;
    else if (req2.vid == 131077)
        std::cout << "Request2(str -> id) success." << std::endl;
    else
        std::cout << "Request2(str -> id) wrong result ID" << req2.vid << "." << std::endl;

    return 0;
}