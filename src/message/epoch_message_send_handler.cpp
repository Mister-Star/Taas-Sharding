//
// Created by 周慰星 on 11/9/22.
//
#include "message/epoch_message_send_handler.h"
#include "message/epoch_message_receive_handler.h"
#include "tools/utilities.h"
#include "transaction/merge.h"
#include "transaction/transaction_cache.h"

namespace Taas {

    Context EpochMessageSendHandler::ctx;
    std::atomic<uint64_t> EpochMessageSendHandler::TotalLatency(0), EpochMessageSendHandler::TotalTxnNum(0),
            EpochMessageSendHandler::TotalSuccessTxnNUm(0), EpochMessageSendHandler::TotalSuccessLatency(0);
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> EpochMessageSendHandler::shard_send_epoch,
            EpochMessageSendHandler::backup_send_epoch,
            EpochMessageSendHandler::abort_set_send_epoch,
            EpochMessageSendHandler::insert_set_send_epoch;

    uint64_t EpochMessageSendHandler::shard_sent_epoch = 1, EpochMessageSendHandler::backup_sent_epoch = 1,
            EpochMessageSendHandler::abort_sent_epoch = 1,
            EpochMessageSendHandler::insert_set_sent_epoch = 1, EpochMessageSendHandler::abort_set_sent_epoch = 1;

    void EpochMessageSendHandler::StaticInit(const Context& _ctx) {
        ctx = _ctx;
        shard_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        backup_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        abort_set_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        insert_set_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            backup_send_epoch [i] = std::make_unique<std::atomic<uint64_t>>(1);
            abort_set_send_epoch [i] = std::make_unique<std::atomic<uint64_t>>(1);
            shard_send_epoch[i] = std::make_unique<std::atomic<uint64_t>>(1);
            insert_set_send_epoch[i] = std::make_unique<std::atomic<uint64_t>>(1);
        }
    }

    void EpochMessageSendHandler::StaticClear() {
    }

/**
 * @brief 将txn设置事务状态，并通过protobuf将Reply序列化，将序列化的结果放到send_to_client_queue中，等待发送给客户端
 *
 * @param ctx XML文件的配置信息
 * @param txn 等待回复给client的事务
 * @param txn_state 告诉client此txn的状态(Success or Abort)
 */
bool EpochMessageSendHandler::SendTxnCommitResultToClient(const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnState txn_state) {
        if(txn_ptr->txn_server_id() != ctx.taasContext.txn_node_ip_index) return true;
        txn_ptr->set_txn_state(txn_state);
        auto msg = std::make_unique<proto::Message>();
        auto rep = msg->mutable_reply_txn_result_to_client();
        rep->set_txn_state(txn_state);
        rep->set_client_txn_id(txn_ptr->client_txn_id());
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        auto tim = now_to_us() - txn_ptr->csn();
        TotalLatency.fetch_add(tim);
        TotalTxnNum.fetch_add(1);
        if(txn_state == proto::TxnState::Commit) {
            TotalSuccessLatency.fetch_add(tim);
            TotalSuccessTxnNUm.fetch_add(1);
        }
        MessageQueue::send_to_client_queue->enqueue(std::make_unique<send_params>(txn_ptr->client_txn_id(), txn_ptr->csn(), txn_ptr->client_ip(), txn_ptr->commit_epoch(), proto::TxnType::CommittedTxn, std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_client_queue->enqueue(std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
    }

    bool EpochMessageSendHandler::SendTxnToServer(uint64_t &epoch, uint64_t &to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type) {
        if(ctx.taasContext.kTxnNodeNum > 1) {
            auto pack_param = std::make_unique<pack_params>(to_whom, 0, "", epoch, txn_type, nullptr);
            switch (txn_type) {
                case proto::TxnType::ShardedClientTxn :
                case proto::TxnType::RemoteServerTxn : {
                    return SendRemoteServerTxn(epoch, to_whom, txn_ptr, txn_type);
                }
                case proto::TxnType::BackUpTxn : {
                    return EpochMessageSendHandler::SendBackUpTxn(epoch, txn_ptr, txn_type);
                }
                case proto::TxnType::BackUpACK :
                case proto::TxnType::AbortSetACK :
                case proto::TxnType::InsertSetACK :
                case proto::TxnType::EpochShardACK :
                case proto::EpochRemoteServerACK : {
                    return SendACK(epoch, to_whom, txn_type);
                }
                case proto::TxnType::EpochLogPushDownComplete : {
                    return SendMessageToAll(epoch, txn_type);
                }
                case proto::NullMark:
                case proto::TxnType_INT_MIN_SENTINEL_DO_NOT_USE_:
                case proto::TxnType_INT_MAX_SENTINEL_DO_NOT_USE_:
                case proto::ClientTxn:
                case proto::EpochShardEndFlag:
                case proto::EpochRemoteServerEndFlag:
                case proto::EpochBackUpEndFlag:
                case proto::EpochCommittedTxnEndFlag:
                case proto::CommittedTxn:
                case proto::AbortSet:
                case proto::InsertSet:
                case proto::Lock_ok:
                case proto::Lock_abort:
                case proto::Prepare_req:
                case proto::Prepare_ok:
                case proto::Prepare_abort:
                case proto::Commit_req:
                case proto::Commit_ok:
                case proto::Commit_abort:
                case proto::Abort_txn:
                    break;

            }
        }
        return true;
    }

    bool EpochMessageSendHandler::SendRemoteServerTxn(uint64_t& epoch, uint64_t& to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = *txn_ptr;
        auto shard_id = txn_ptr->shard_id();
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(!serialized_txn_str_ptr->empty());
        if (txn_type == proto::TxnType::ShardedClientTxn) {
            MessageQueue::send_to_server_queue->enqueue(
                    std::make_unique<send_params>(to_whom, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),nullptr));
            return MessageQueue::send_to_server_queue->enqueue(
                    std::make_unique<send_params>(to_whom, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
        } else {
            for(uint64_t j = 0; j < ctx.taasContext.kReplicaNum; j ++ ) {
                auto to_id = (shard_id + ctx.taasContext.kTxnNodeNum - j) % ctx.taasContext.kTxnNodeNum;
                if(to_id == ctx.taasContext.txn_node_ip_index) continue;
                MessageQueue::send_to_server_queue->enqueue(
                        std::make_unique<send_params>(to_id, 0, "", epoch, txn_type,std::move(serialized_txn_str_ptr), nullptr));
                return MessageQueue::send_to_server_queue->enqueue(
                        std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
            }
        }
        return true;
    }

    bool EpochMessageSendHandler::SendBackUpTxn(uint64_t& epoch, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = *txn_ptr;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(!serialized_txn_str_ptr->empty());
        uint64_t to_id;
        for(uint64_t i = 0; i < ctx.taasContext.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
            to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
            if(to_id == (uint64_t)ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
            auto s = std::make_unique<std::string>(*serialized_txn_str_ptr);
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch,proto::TxnType::EpochBackUpEndFlag,std::move(s),nullptr));
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
        }
//        MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),nullptr));
//        return MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "",epoch, proto::TxnType::NullMark,nullptr, nullptr));
        return true;
    }

    bool EpochMessageSendHandler::SendACK(uint64_t &epoch, uint64_t &to_whom, proto::TxnType txn_type) {
        if(to_whom == ctx.taasContext.txn_node_ip_index) return true;
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_txn_server_id(ctx.taasContext.txn_node_ip_index);
        txn_end->set_txn_type(txn_type);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_shard_id(0);
        txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
        std::vector<std::string> keys, values;
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_whom, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),nullptr));
        return MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }

    bool EpochMessageSendHandler::SendMessageToAll(uint64_t& epoch, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_txn_server_id(ctx.taasContext.txn_node_ip_index);
        txn_end->set_txn_type(txn_type);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_shard_id(0);
        txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        MessageQueue::send_to_server_pub_queue->enqueue(
                std::make_unique<send_params>(0, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),
                                              nullptr));
        return MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }

    void EpochMessageSendHandler::CheckAndSendEpochShardEndMessage() {
        uint64_t local_server_id = ctx.taasContext.txn_node_ip_index;
        uint64_t epoch = 1, server_num = ctx.taasContext.kTxnNodeNum;
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()) {
            if (EpochMessageReceiveHandler::IsEpochClientTxnHandleComplete(epoch)) {
                for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
                    if (server_id == local_server_id) continue;
                    auto msg = std::make_unique<proto::Message>();
                    auto *txn_end = msg->mutable_txn();
                    txn_end->set_txn_server_id(local_server_id);
                    txn_end->set_txn_type(proto::TxnType::EpochShardEndFlag);
                    txn_end->set_commit_epoch(epoch);
                    txn_end->set_shard_id(server_id);
                    txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
                    txn_end->set_csn(EpochMessageReceiveHandler::GetAllThreadLocalCountNum(epoch, server_id,
                                                                                           EpochMessageReceiveHandler::shard_should_send_txn_num_local_vec)); /// 不同server由不同的数量
                    auto serialized_txn_str_ptr = std::make_unique<std::string>();
                    Gzip(msg.get(), serialized_txn_str_ptr.get());
                    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(server_id, 0, "", epoch,proto::TxnType::EpochShardEndFlag,std::move(serialized_txn_str_ptr),nullptr));
                    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
                }
                {
                    auto msg = std::make_unique<proto::Message>();
                    auto* txn_end = msg->mutable_txn();
                    txn_end->set_txn_server_id(local_server_id);
                    txn_end->set_txn_type(proto::TxnType::EpochBackUpEndFlag);
                    txn_end->set_commit_epoch(epoch);
                    txn_end->set_shard_id(0);
                    txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
                    txn_end->set_csn(static_cast<uint64_t>(EpochMessageReceiveHandler::GetAllThreadLocalCountNum(epoch, EpochMessageReceiveHandler::backup_should_send_txn_num_local_vec)));
                    auto serialized_txn_str_ptr = std::make_unique<std::string>();
                    Gzip(msg.get(), serialized_txn_str_ptr.get());

                    auto msg_0 = std::make_unique<proto::Message>();
                    auto* txn_end_0 = msg_0->mutable_txn();
                    txn_end_0->set_txn_server_id(local_server_id);
                    txn_end_0->set_txn_type(proto::TxnType::EpochBackUpEndFlag);
                    txn_end_0->set_commit_epoch(epoch);
                    txn_end_0->set_shard_id(0);
                    txn_end_0->set_message_server_id(ctx.taasContext.txn_node_ip_index);
                    txn_end_0->set_csn(0);
                    auto serialized_txn_str_ptr_0 = std::make_unique<std::string>();
                    Gzip(msg_0.get(), serialized_txn_str_ptr_0.get());

                    uint64_t to_id;
                    for(uint64_t i = 0; i < server_num; i ++) {
                        to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
                        if(to_id == (uint64_t)ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
                        if(i < ctx.taasContext.kBackUpNum) { /// send to i+1, i+2...i+kBackNum-1
                            auto s = std::make_unique<std::string>(*serialized_txn_str_ptr);
                            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch,proto::TxnType::EpochBackUpEndFlag,std::move(s),nullptr));
                        }
                        else {
                            auto s = std::make_unique<std::string>(*serialized_txn_str_ptr_0);
                            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch,proto::TxnType::EpochBackUpEndFlag,std::move(s),nullptr));
                        }
                        MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
                    }
                }
                epoch ++;
            }
            else {
                usleep(sleep_time);
            }
        }
    }

    bool EpochMessageSendHandler::SendEpochShardEndMessage(const uint64_t &txn_node_ip_index, const uint64_t epoch, const uint64_t &kTxnNodeNum) {
        for(uint64_t server_id = 0; server_id < kTxnNodeNum; server_id ++) {
            if (server_id == txn_node_ip_index) continue;
            auto msg = std::make_unique<proto::Message>();
            auto *txn_end = msg->mutable_txn();
            txn_end->set_txn_server_id(txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::EpochShardEndFlag);
            txn_end->set_commit_epoch(epoch);
            txn_end->set_shard_id(server_id);
            txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
            txn_end->set_csn(EpochMessageReceiveHandler::GetAllThreadLocalCountNum(epoch, server_id,
                    EpochMessageReceiveHandler::shard_should_send_txn_num_local_vec)); /// 不同server由不同的数量
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            Gzip(msg.get(), serialized_txn_str_ptr.get());
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(server_id, 0, "", epoch,proto::TxnType::EpochShardEndFlag,std::move(serialized_txn_str_ptr),nullptr));
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
        }
        {
            auto msg = std::make_unique<proto::Message>();
            auto* txn_end = msg->mutable_txn();
            txn_end->set_txn_server_id(txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::EpochBackUpEndFlag);
            txn_end->set_commit_epoch(epoch);
            txn_end->set_shard_id(0);
            txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
            txn_end->set_csn(static_cast<uint64_t>(EpochMessageReceiveHandler::GetAllThreadLocalCountNum(epoch, EpochMessageReceiveHandler::backup_should_send_txn_num_local_vec)));
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            Gzip(msg.get(), serialized_txn_str_ptr.get());

            auto msg_0 = std::make_unique<proto::Message>();
            auto* txn_end_0 = msg_0->mutable_txn();
            txn_end_0->set_txn_server_id(txn_node_ip_index);
            txn_end_0->set_txn_type(proto::TxnType::EpochBackUpEndFlag);
            txn_end_0->set_commit_epoch(epoch);
            txn_end_0->set_shard_id(0);
            txn_end_0->set_message_server_id(ctx.taasContext.txn_node_ip_index);
            txn_end_0->set_csn(0);
            auto serialized_txn_str_ptr_0 = std::make_unique<std::string>();
            Gzip(msg_0.get(), serialized_txn_str_ptr_0.get());

            uint64_t to_id;
            for(uint64_t i = 0; i < kTxnNodeNum; i ++) {
                to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
                if(to_id == (uint64_t)ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
                if(i < ctx.taasContext.kBackUpNum) { /// send to i+1, i+2...i+kBackNum-1
                    auto s = std::make_unique<std::string>(*serialized_txn_str_ptr);
                    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch,proto::TxnType::EpochBackUpEndFlag,std::move(s),nullptr));
                }
                else {
                    auto s = std::make_unique<std::string>(*serialized_txn_str_ptr_0);
                    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch,proto::TxnType::EpochBackUpEndFlag,std::move(s),nullptr));
                }
                MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
            }
//            for(uint64_t i = 0; i < ctx.taasContext.kBackUpNum; i ++) {
//                to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
//                if(to_id == (uint64_t)ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
//                auto s = std::make_unique<std::string>(*serialized_txn_str_ptr);
//                MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch,proto::TxnType::EpochBackUpEndFlag,std::move(s),nullptr));
//                MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
//            }
        }
        return true;
    }

    void EpochMessageSendHandler::CheckAndSendEpochRemoteServerEndMessage() {
        uint64_t local_server_id = ctx.taasContext.txn_node_ip_index;
        uint64_t epoch = 1, server_num = ctx.taasContext.kTxnNodeNum;
        while (!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            if(EpochMessageReceiveHandler::CheckEpochShardReceiveComplete(epoch)) {
                for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
                    if (server_id == local_server_id) continue;
                    auto msg = std::make_unique<proto::Message>();
                    auto *txn_end = msg->mutable_txn();
                    txn_end->set_txn_server_id(local_server_id);
                    txn_end->set_txn_type(proto::TxnType::EpochRemoteServerEndFlag);
                    txn_end->set_commit_epoch(epoch);
                    txn_end->set_shard_id(server_id);
                    txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
                    txn_end->set_csn(EpochMessageReceiveHandler::GetAllThreadLocalCountNum(epoch, server_id,
                                                                                           EpochMessageReceiveHandler::remote_server_should_send_txn_num_local_vec)); /// 不同server由不同的数量
                    auto serialized_txn_str_ptr = std::make_unique<std::string>();
                    Gzip(msg.get(), serialized_txn_str_ptr.get());
                    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(server_id, 0, "", epoch,proto::TxnType::EpochRemoteServerEndFlag, std::move(serialized_txn_str_ptr),nullptr));
                    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
                }
                epoch ++;
            }
            else {
                usleep(sleep_time);
            }
        }
    }

    bool EpochMessageSendHandler::SendEpochRemoteServerEndMessage(const uint64_t &txn_node_ip_index, const uint64_t epoch, const uint64_t &kTxnNodeNum) {
        for(uint64_t server_id = 0; server_id < kTxnNodeNum; server_id ++) {
            if (server_id == txn_node_ip_index) continue;
            auto msg = std::make_unique<proto::Message>();
            auto *txn_end = msg->mutable_txn();
            txn_end->set_txn_server_id(txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::EpochRemoteServerEndFlag);
            txn_end->set_commit_epoch(epoch);
            txn_end->set_shard_id(server_id);
            txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
            txn_end->set_csn(EpochMessageReceiveHandler::GetAllThreadLocalCountNum(epoch, server_id,
                  EpochMessageReceiveHandler::remote_server_should_send_txn_num_local_vec)); /// 不同server由不同的数量
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            Gzip(msg.get(), serialized_txn_str_ptr.get());
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(server_id, 0, "", epoch,proto::TxnType::EpochRemoteServerEndFlag, std::move(serialized_txn_str_ptr),nullptr));
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
        }
        return true;
    }

    void EpochMessageSendHandler::CheckAndSendAbortSet() {
        uint64_t local_server_id = ctx.taasContext.txn_node_ip_index;
        uint64_t epoch = 1, server_num = ctx.taasContext.kTxnNodeNum;
        while (!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            if(Merger::CheckEpochMergeComplete(epoch)) {
                auto msg = std::make_unique<proto::Message>();
                auto *txn_end = msg->mutable_txn();
                txn_end->set_txn_server_id(local_server_id);
                txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
                txn_end->set_txn_type(proto::TxnType::AbortSet);
                txn_end->set_commit_epoch(epoch);
                txn_end->set_shard_id(0);
                std::vector<std::string> keys, values;
                TransactionCache::local_epoch_abort_txn_set[epoch % ctx.taasContext.kCacheMaxLength]->getValue(keys, values);
                for (uint64_t i = 0; i < keys.size(); i++) {
                    auto row = txn_end->add_row();
                    row->set_key(keys[i]);
                    row->set_data(values[i]);
                }
                auto serialized_txn_str_ptr = std::make_unique<std::string>();
                Gzip(msg.get(), serialized_txn_str_ptr.get());
                MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::AbortSet, std::move(serialized_txn_str_ptr), nullptr));
                MessageQueue::send_to_server_pub_queue->enqueue( std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));

                epoch ++;
            }
            else {
                usleep(sleep_time);
            }
        }
    }
    bool EpochMessageSendHandler::SendAbortSet(const uint64_t &txn_node_ip_index, const uint64_t epoch, const uint64_t &kCacheMaxLength) {
        auto msg = std::make_unique<proto::Message>();
        auto *txn_end = msg->mutable_txn();
        txn_end->set_txn_server_id(txn_node_ip_index);
        txn_end->set_message_server_id(ctx.taasContext.txn_node_ip_index);
        txn_end->set_txn_type(proto::TxnType::AbortSet);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_shard_id(0);
        std::vector<std::string> keys, values;
        TransactionCache::local_epoch_abort_txn_set[epoch % kCacheMaxLength]->getValue(keys, values);
        for (uint64_t i = 0; i < keys.size(); i++) {
            auto row = txn_end->add_row();
            row->set_key(keys[i]);
            row->set_data(values[i]);
        }
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::AbortSet, std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_server_pub_queue->enqueue( std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }

}