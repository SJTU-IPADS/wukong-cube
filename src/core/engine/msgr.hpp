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

#include <vector>

#include "core/network/adaptor.hpp"

#include "core/common/bundle.hpp"

// utils
#include "utils/logger2.hpp"

namespace wukong {

class Messenger {
private:
    class Message {
    public:
        int sid;
        int tid;
        std::string msg;

        Message(int sid, int tid, std::string &msg)
            : sid(sid), tid(tid), msg(std::move(msg)) { }
    };

    std::vector<Message> pending_msgs;

public:
    int sid;    // server id
    int tid;    // thread id

    Adaptor *adaptor;

    Messenger(int sid, int tid, Adaptor *adaptor) : sid(sid), tid(tid), adaptor(adaptor) { }

    inline void sweep_msgs() {
        if (!pending_msgs.size()) return;

        logstream(LOG_DEBUG) << "#" << tid << " "
                             << pending_msgs.size() << " pending msgs on engine." << LOG_endl;
        for (std::vector<Message>::iterator it = pending_msgs.begin(); it != pending_msgs.end();)
            if (adaptor->send(it->sid, it->tid, it->msg))
                it = pending_msgs.erase(it);
            else
                ++it;
    }

    bool send_msg(Bundle &bundle, int dst_sid, int dst_tid) {
        std::string msg = bundle.to_str();
        if (adaptor->send(dst_sid, dst_tid, msg))
            return true;

        // failed to send, then stash the msg to avoid deadlock
        pending_msgs.push_back(Message(dst_sid, dst_tid, msg));
        return false;
    }

    Bundle recv_msg() { return Bundle(adaptor->recv()); }

    bool tryrecv_msg(Bundle &bundle) {
        std::string msg;
        if (!adaptor->tryrecv(msg)) return false;
        bundle.init(msg);
        return true;
    }

};

} // namespace wukong