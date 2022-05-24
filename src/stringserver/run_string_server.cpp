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

#include <iostream>

#include "stringserver/string_proxy.hpp"
#include "stringserver/string_server.hpp"

static void usage(char* fn) {
    std::cout << "usage: " << fn << " <string server port> <data file directory>[options]" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    // parse argv
    int port = atoi(argv[1]);
    std::string input_folder = std::string(argv[2]);

    // load string server (read-only, shared by all proxies and all engines)
    wukong::StringServer string_server(input_folder);

    // launch string proxy
    wukong::StringProxy proxy(port, &string_server);
    proxy.serve();

    return 0;
}
