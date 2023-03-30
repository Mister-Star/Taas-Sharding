//
// Created by 周慰星 on 23-3-30.
//

#include "message/message.h"
#include "proto/message.pb.h"
namespace Taas {
    // 接受client和peer txn node发来的写集，都放在listen_message_queue中
    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> listen_message_queue;
    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<send_params>>> send_to_server_queue, send_to_client_queue, send_to_storage_queue;
    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Message>>> request_queue, raft_message_queue;

    void InitMessage(Context& ctx) {
        listen_message_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>>();
        send_to_server_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        send_to_client_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        send_to_storage_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        request_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Message>>>();
        raft_message_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Message>>>();
    }

}
