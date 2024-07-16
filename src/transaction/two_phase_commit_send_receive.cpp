//
// Created by user on 24-7-16.
//
#include "transaction/two_phase_commit.h"

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "proto/transaction.pb.h"
#include "storage/redo_loger.h"
#include "storage/mot.h"


namespace Taas {

    // 发送事务给指定applicant、coordinator
    // to_whom 为编号
    ///send to other nodes use send_to_server_queue
    bool TwoPC::Send(uint64_t& to_whom, proto::Transaction& txn,
                     proto::TxnType txn_type) {
        if (to_whom == ctx.taasContext.txn_node_ip_index){
            auto msg = std::make_unique<proto::Message>();
            auto* txn_temp = msg->mutable_txn();
            *(txn_temp) = txn;
            txn_temp->set_txn_type(txn_type);
            auto serialized_txn_str = std::string();
            Gzip(msg.get(), &serialized_txn_str);
            void *data = static_cast<void *>(const_cast<char *>(serialized_txn_str.data()));
            MessageQueue::listen_message_epoch_queue->enqueue(
                    std::make_unique<zmq::message_t>(data, serialized_txn_str.size()));
            return MessageQueue::listen_message_epoch_queue->enqueue(nullptr);
        }
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = txn;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());

        assert(!serialized_txn_str_ptr->empty());
        // printf("send thread  message epoch %d server_id %lu type %d\n", 0, to_whom, txn_type);
        MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(
                to_whom, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(
                to_whom, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }

    // 发送给client abort/commit
    ///send to client use send_to_client_queue
    bool TwoPC::SendToClient(proto::Transaction& txn, proto::TxnType txn_type,
                             proto::TxnState txn_state) {

        if (txn.txn_server_id() != ctx.taasContext.txn_node_ip_index) return true;
        uint64_t currTxnTime = now_to_us() - txn.csn();
        finishedTxnNumber.fetch_add(1);
        lags_.store(totalTxnNumber.load() - finishedTxnNumber.load() + 10);

        if (txn_state == proto::TxnState::Commit){
            successTxnNumber.fetch_add(1);
            successTime.fetch_add(currTxnTime);
            totalTime.fetch_add(currTxnTime);
            if ((successTxnNumber.load() + failedTxnNumber.load()) % 1000 == 0) OUTPUTLOG("============= 2PC + 2PL INFO =============", currTxnTime);
        } else {
            failedTxnNumber.fetch_add(1);
            failedTime.fetch_add(currTxnTime);
            totalTime.fetch_add(currTxnTime);
            if ((successTxnNumber.load() + failedTxnNumber.load()) % 1000 == 0) OUTPUTLOG("============= 2PC + 2PL INFO =============", currTxnTime);
        }

        // only coordinator can send to client

        // 设置txn的状态并创建proto对象
        txn.set_txn_state(txn_state);
        auto msg = std::make_unique<proto::Message>();
        auto rep = msg->mutable_reply_txn_result_to_client();
        rep->set_txn_state(txn_state);
        rep->set_client_txn_id(txn.client_txn_id());

        // 将Transaction使用protobuf进行序列化，序列化的结果在serialized_txn_str_ptr中
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());

        // free txn_state_map and txn_phase_map
        // CleanTxnState(txn_ptr);

        // 将序列化的Transaction放到send_to_client_queue中，等待发送给client
        MessageQueue::send_to_client_queue->enqueue(std::make_unique<send_params>(
                txn.client_txn_id(), txn.csn(), txn.client_ip(), txn.commit_epoch(), txn_type,
                std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_client_queue->enqueue(
                std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
    }

    // 处理接收到的消息
    // change queue to listen_message_epoch_queue
    void TwoPC::HandleClientMessage() {
        while(!EpochManager::IsTimerStop()) {
            ///receive from client use listen_message_txn_queue
            MessageQueue::listen_message_txn_queue->wait_dequeue(message_ptr);
            if (message_ptr == nullptr || message_ptr->empty()) continue;
            LOG(INFO) << message_ptr->size();
            message_string_ptr = std::make_unique<std::string>(
                    static_cast<const char *>(message_ptr->data()), message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
                txn_ptr = std::make_shared<proto::Transaction>(*(msg_ptr->release_txn()));
                SetMessageRelatedCountersInfo();
                HandleReceivedTxn();
            }
            message_ptr.reset();
            message_string_ptr.reset();
            msg_ptr.reset();
            txn_ptr.reset();
            tmp_vector.reset();
            txn_state_struct.reset();
        }
    }

    void TwoPC::HandleReceivedMessage() {
        while(!EpochManager::IsTimerStop()) {
            ///receive from other nodes use listen_message_epoch_queue
            MessageQueue::listen_message_epoch_queue->wait_dequeue(message_ptr);
            if (message_ptr == nullptr || message_ptr->empty()) continue;
            message_string_ptr = std::make_unique<std::string>(
                    static_cast<const char *>(message_ptr->data()), message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
                txn_ptr = std::make_shared<proto::Transaction>(*(msg_ptr->release_txn()));
                SetTxnState(*txn_ptr);
                SetMessageRelatedCountersInfo();
                HandleReceivedTxn();
            }
            message_ptr.reset();
            message_string_ptr.reset();
            msg_ptr.reset();
            txn_ptr.reset();
            tmp_vector.reset();
            txn_state_struct.reset();
        }
    }


    void TwoPC::HandleReceivedTxn() {
        switch (txn_ptr->txn_type()) {
            case proto::TxnType::ClientTxn: {
                ClientTxn_Init();
                break;
            }
            case proto::TxnType::RemoteServerTxn: {
                two_pl_req_progressing.fetch_add(1);
                if (Two_PL_LOCK(*txn_ptr)) {
                    // 发送lock ok
                    auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
                    Send(to_whom, *txn_ptr, proto::TxnType::Lock_ok);
                } else {
                    // 发送lock abort
                    auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
                    Send(to_whom, *txn_ptr, proto::TxnType::Lock_abort);
                }
                key_sorted.clear();
                break;
            }
            case proto::TxnType::Lock_ok: {
                if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
                    tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
                    if(!txn_state_map.getValue(tid, txn_state_struct)) break;
                    txn_state_struct->two_pl_reply.fetch_add(1);
                    txn_state_struct->two_pl_num.fetch_add(1);
                    // 所有应答收到
                    if (txn_state_struct->two_pl_reply.load() == txn_state_struct->txn_shard_num) {
                        two_pl_num_progressing.fetch_add(1);
                        if (Check_2PL_complete(txn_state_struct)) {
                            // 2pl完成，开始2pc prepare阶段
                            if (!txn_phase_map.getValue(tid,tmp_vector)) return;
                            if (tmp_vector->empty()) return;  // if already send to client
                            for (uint64_t i = 0; i < shard_num; i++) {
                                auto to_whom = (*tmp_vector)[i]->shard_id();
                                Send(to_whom, *(*tmp_vector)[i], proto::TxnType::Prepare_req);
                            }
                        } else {
                            AbortTxn();
                        }
                    }
                }
                break;
            }
            case proto::TxnType::Lock_abort: {
                // 直接发送abort
                if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
                    tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
                    if(!txn_state_map.getValue(tid, txn_state_struct)) break;
                    txn_state_struct->two_pl_reply.fetch_add(1);
                    txn_state_struct->two_pl_failed_num.fetch_add(1);
                    if (txn_state_struct->two_pl_reply.load() == txn_state_struct->txn_shard_num) {
                        two_pl_num_progressing.fetch_add(1);
                        AbortTxn();
                    }
                }
                break;
            }
            case proto::TxnType::Prepare_req: {
//          LOG(INFO) << "receive Prepare_req";
                // 日志操作等等，总之返回Prepare_ok
                tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
                auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
                Send(to_whom, *txn_ptr, proto::TxnType::Prepare_ok);
                break;
            }
            case proto::TxnType::Prepare_ok: {
                // 修改元数据
                if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
                    tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
                    if(!txn_state_map.getValue(tid, txn_state_struct)) break;
                    txn_state_struct->two_pc_prepare_reply.fetch_add(1);
                    txn_state_struct->two_pc_prepare_num.fetch_add(1);

                    // 当所有应答已经收到
                    if (txn_state_struct->two_pc_prepare_reply.load() == txn_state_struct->txn_shard_num) {
                        two_pc_prepare_num_progressing.fetch_add(1);
                        if (Check_2PC_Prepare_complete(txn_state_struct)) {
                            if (!txn_phase_map.getValue(tid,tmp_vector)) break;
                            for (uint64_t i = 0; i < shard_num; i++) {
                                auto to_whom = (*tmp_vector)[i]->shard_id();
                                Send(to_whom, *(*tmp_vector)[i], proto::TxnType::Commit_req);
                            }
                        } else {
                            AbortTxn();
                        }
                    }
                }
                break;
            }
            case proto::TxnType::Prepare_abort: {
                ///can not happen in normal cases
//                if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
//                    tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
//                    std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
//                    if(!txn_state_map.getValue(tid, txn_state_struct)) break;
//                    txn_state_struct->two_pc_prepare_reply.fetch_add(1);
//                    txn_state_struct->two_pc_prepare_failed_num.fetch_add(1);
//                    AbortTxn();
//                }
                break;
            }
            case proto::TxnType::Commit_req: {
                Two_PL_UNLOCK(*txn_ptr);    // unlock
                auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
                Send(to_whom, *txn_ptr, proto::TxnType::Commit_ok);
                if (txn_ptr->txn_server_id() != ctx.taasContext.txn_node_ip_index) CleanTxnState(txn_ptr);
                break;
            }
            case proto::TxnType::Commit_ok: {
                if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
                    tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
                    if(!txn_state_map.getValue(tid, txn_state_struct)) break;
                    txn_state_struct->two_pc_commit_reply.fetch_add(1);
                    txn_state_struct->two_pc_commit_num.fetch_add(1);
                    if (txn_state_struct->two_pc_commit_reply.load() == txn_state_struct->txn_shard_num) {
                        two_pc_commit_num_progressing.fetch_add(1);
                        if (Check_2PC_Commit_complete(txn_state_struct)) {
                            CommitTxn();
                        }
                    }
                }
                break;
            }
            case proto::TxnType::Commit_abort: { ///can not happen in normal cases
//                if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
//                    AbortTxn();
//                }
                break;
            }
            case proto::TxnType::Abort_txn: {
                Two_PL_UNLOCK(*txn_ptr);
                CleanTxnState(txn_ptr);
                break;
            }
            default:
                break;
        }
    }

}
