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

#include <cstring>
#include <iosfwd>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace wukong {

const int ConnectAttemptsNum = 5;
const int ConnectTimeoutMs = 8000;    // 1000 ms

// enum class RPC_CODE : uint32_t { INFO_RPC = 0, SPARQL_RPC = 1, STRING_RPC = 2, EXIT_RPC = 3 };
enum RPC_CODE {
    INFO_RPC = 0x7001,
    SPARQL_RPC,
    STRING_RPC,
    EXIT_RPC
};

enum StatusCode {
  kOK = 0,
  kInvalid = -1,
  kIOError = -2,
  kAssertionFailed = -3,
  kConnectionFailed = -4,
  kConnectionError = -5,
  kWukongError = -6,
  kUnknownError = -255
};

class Status {
public:
    Status(): code(StatusCode::kOK) {}
    Status(StatusCode code, const std::string& msg): code(code), msg(msg) {}
    Status(int code, const char *cmsg): code(code), msg(std::string(cmsg)) {}

    /// Return a success status
    inline static Status OK() { return Status(); }

    /// Return an error status for invalid data (for example a string that
    /// fails parsing).
    static Status Invalid() { return Status(StatusCode::kInvalid, std::string("")); }

    /// Return an error status for invalid data, with user specified error
    /// message.
    static Status Invalid(std::string const& message) {
        return Status(StatusCode::kInvalid, message);
    }

    /// Return an error status for IO errors (e.g. Failed to open or read from a
    /// file).
    static Status IOError(const std::string& msg = "") {
        return Status(StatusCode::kIOError, msg);
    }

    /// Return an error status when the condition assertion is false.
    static Status AssertionFailed(std::string const& condition) {
        return Status(StatusCode::kAssertionFailed, condition);
    }

    /// Return an error when client failed to connect to wukong proxy.
    static Status ConnectionFailed(std::string const& message = "") {
        return Status(StatusCode::kConnectionFailed,
                    "Failed to connect to wukong proxy: " + message);
    }

    /// Return an error when client losts connection to wukong proxy.
    static Status ConnectionError(std::string const& message = "") {
        return Status(StatusCode::kConnectionError, message);
    }

    /// Return true iff the status indicates success.
    bool ok() const { return code == StatusCode::kOK; }

    int get_code() { return this->code; }

    std::string get_msg() { return this->msg; }

private:
    int code;
    std::string msg;
};

} // namespace wukong