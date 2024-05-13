//
// Created by 周慰星 on 11/9/22.
//
#include <queue>

#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "message/epoch_message_receive_handler.h"
#include "tools/utilities.h"
#include "transaction/merge.h"
#include "transaction/transaction_cache.h"


namespace Taas {

    bool EpochMessageReceiveHandler::Init(const uint64_t &id, const Context& context) {
        ctx = context;
        message_ptr = nullptr;
        txn_ptr.reset();
        thread_id = id;
        server_num = ctx.taasContext.kTxnNodeNum;
        shard_num = ctx.taasContext.kShardNum;
        replica_num = ctx.taasContext.kReplicaNum;
        local_server_id = ctx.taasContext.txn_node_ip_index;
        max_length = ctx.taasContext.kCacheMaxLength;
        ThreadCountersInit(ctx);

        server_num = ctx.taasContext.kTxnNodeNum,
        shard_num = ctx.taasContext.kShardNum,
        replica_num = ctx.taasContext.kReplicaNum,
        local_server_id = ctx.taasContext.txn_node_ip_index,
        max_length = ctx.taasContext.kCacheMaxLength;

        is_local_shard.resize(server_num);
        for(auto &i : is_local_shard) {
            i.resize(shard_num);
        }
        for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
            for(uint64_t i = 0; i < shard_num; i ++) {
                for(uint64_t j = 0; j < replica_num; j ++ ) {
                    if((i + server_num - j) % server_num == server_id) {
                        is_local_shard[server_id][i] = true;
                    }
                }
            }
        }

        return true;
    }

    bool EpochMessageReceiveHandler::StaticInit([[maybe_unused]] const Context& context) {
        return true;
    }

    bool EpochMessageReceiveHandler::StaticClear([[maybe_unused]] uint64_t& epoch) {
        return true;
    }

    void EpochMessageReceiveHandler::ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_read_validate_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->txn_server_id(), 1);
        TransactionCache::epoch_read_validate_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_read_validate_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void EpochMessageReceiveHandler::MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_merge_txn_num_local->IncCount(epoch_mod_temp, txn_ptr->txn_server_id(), 1);
        TransactionCache::epoch_merge_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_merge_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void EpochMessageReceiveHandler::CommitQueueEnqueue(uint64_t& epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_commit_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->txn_server_id(), 1);
        TransactionCache::epoch_commit_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_commit_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void EpochMessageReceiveHandler::RedoLogQueueEnqueue(uint64_t& epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_record_commit_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->txn_server_id(), 1);
        TransactionCache::epoch_redo_log_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_redo_log_queue[epoch_mod_temp]->enqueue(nullptr);
    }

    void EpochMessageReceiveHandler::HandleReceivedMessage() {
        auto safe_length = ctx.taasContext.kCacheMaxLength / 10;
        while(!EpochManager::IsTimerStop()) {
//            while( EpochManager::GetLogicalEpoch() + safe_length > EpochManager::GetPhysicalEpoch() ) {
//                usleep(ctx.taasContext.kEpochSize_us);
//            }
            MessageQueue::listen_message_txn_queue->wait_dequeue(message_ptr);
            if (message_ptr == nullptr || message_ptr->empty()) continue;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()), message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
            HandleReceivedTxn();
            txn_ptr.reset();
        }
    }

    void EpochMessageReceiveHandler::TryHandleReceivedMessage() {
        sleep_flag = true;
        if(MessageQueue::listen_message_txn_queue->try_dequeue(message_ptr)) {
            sleep_flag = false;
            if (message_ptr == nullptr || message_ptr->empty()) return;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
            HandleReceivedTxn();
            txn_ptr.reset();
        }
    }

    void EpochMessageReceiveHandler::HandleReceivedControlMessage() {
        while(!EpochManager::IsTimerStop()) {
            MessageQueue::listen_message_epoch_queue->wait_dequeue(message_ptr);
            if (message_ptr == nullptr || message_ptr->empty()) continue;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
            HandleReceivedTxn();
            txn_ptr.reset();
        }
    }

    void EpochMessageReceiveHandler::TryHandleReceivedControlMessage() {
        sleep_flag = true;
        if(MessageQueue::listen_message_epoch_queue->try_dequeue(message_ptr)) {
            sleep_flag = false;
            if (message_ptr == nullptr || message_ptr->empty()) return;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
            HandleReceivedTxn();
            txn_ptr.reset();
        }
    }








    void EpochMessageReceiveHandler::Shard() {
        auto shard_row_vector = std::make_shared<std::vector<std::shared_ptr<proto::Transaction>>>() ;
        for(uint64_t i = 0; i < shard_num; i ++) {
            shard_row_vector->emplace_back(std::make_shared<proto::Transaction>());
            auto vector_i = &(*((*shard_row_vector)[i]));
            vector_i->set_csn(txn_ptr->csn());
            vector_i->set_commit_epoch(txn_ptr->commit_epoch());
            vector_i->set_txn_server_id(txn_ptr->txn_server_id());
            vector_i->set_client_ip(txn_ptr->client_ip());
            vector_i->set_client_txn_id(txn_ptr->client_txn_id());


            vector_i->set_message_server_id(local_server_id);
            vector_i->set_shard_id(i);
            vector_i->set_txn_type(proto::ShardedClientTxn);
        }
        for(auto i = 0; i < txn_ptr->row_size(); i ++) {
            const auto& row = txn_ptr->row(i);
            auto row_ptr = (*shard_row_vector)[GetHashValue(row.key())]->add_row();
            (*row_ptr) = row;
        }
        for(uint64_t i = 0; i < shard_num; i ++) {
            if(is_local_shard[local_server_id][i]) {
                ReadValidateQueueEnqueue(message_epoch, (*shard_row_vector)[i]);
                MergeQueueEnqueue(message_epoch, (*shard_row_vector)[i]);
                CommitQueueEnqueue(message_epoch, (*shard_row_vector)[i]);
            } else {
                if((*shard_row_vector)[i]->row_size() > 0) {
                    round_robin = round_robin + 1 % replica_num;
                    sent_to = (i + server_num - round_robin) % server_num;
                    shard_should_send_txn_num_local->IncCount(message_epoch, sent_to, 1); //use server_id to send EpochShardEndMessage
                    (*shard_row_vector)[i]->set_shard_server_id(sent_to);
                    EpochMessageSendHandler::SendTxnToServer(message_epoch, sent_to, (*shard_row_vector)[i], proto::TxnType::ShardedClientTxn);
                    shard_send_txn_num_local->IncCount(message_epoch, sent_to, 1);
                }
            }
        }
        backup_should_send_txn_num_local->IncCount(message_epoch, txn_server_id, 1);
        EpochMessageSendHandler::SendTxnToServer(message_epoch, txn_server_id, txn_ptr, proto::TxnType::BackUpTxn);
        RedoLogQueueEnqueue(message_epoch, txn_ptr); /// full txn for redo log
        backup_send_txn_num_local->IncCount(message_epoch, txn_server_id, 1);
    }

    bool EpochMessageReceiveHandler::SetMessageRelatedCountersInfo() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = message_epoch % ctx.taasContext.kCacheMaxLength;
        txn_server_id = txn_ptr->txn_server_id();
        shard_id = txn_ptr->shard_id();
        shard_server_id = txn_ptr->shard_server_id();
        message_server_id = txn_ptr->message_server_id();
        csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        return true;
    }

    bool EpochMessageReceiveHandler::HandleReceivedTxn() {
        SetMessageRelatedCountersInfo();
        switch (txn_ptr->txn_type()) {
            ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
            case proto::TxnType::ClientTxn : {/// sql node --> txn node
                if(ctx.taasContext.taasMode == TaasMode::MultiModel) {
                    HandleMultiModelClientTxn();
                }
                else {
                    message_epoch = EpochManager::GetPhysicalEpoch();
                    message_epoch += 1;
                    shard_should_handle_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
                    txn_ptr->set_commit_epoch(message_epoch);
                    txn_ptr->set_csn(now_to_us());
                    txn_ptr->set_txn_server_id(local_server_id);
                    txn_ptr->set_txn_type(proto::RemoteServerTxn);
                    SetMessageRelatedCountersInfo();
                    Shard();
                    shard_handled_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
                }
                break;
            }
            case proto::TxnType::ShardedClientTxn : {
                shard_should_handle_remote_txn_num_local->IncCount(message_epoch, message_server_id, 1);

                ReadValidateQueueEnqueue(message_epoch, txn_ptr);
                MergeQueueEnqueue(message_epoch, txn_ptr);
                CommitQueueEnqueue(message_epoch, txn_ptr);
                shard_received_txn_num_local->IncCount(message_epoch, message_server_id, 1);
                for(uint64_t j = 0; j < ctx.taasContext.kReplicaNum; j ++ ) { /// use the network for reducing the merge time
                    auto to_id = (shard_id + ctx.taasContext.kTxnNodeNum - j) % ctx.taasContext.kTxnNodeNum;
                    if (to_id == ctx.taasContext.txn_node_ip_index) continue;
                    remote_server_should_send_txn_num_local->IncCount(message_epoch, to_id, 1);
                }
                EpochMessageSendHandler::SendTxnToServer(message_epoch, sent_to, txn_ptr, proto::TxnType::RemoteServerTxn);
                for(uint64_t j = 0; j < ctx.taasContext.kReplicaNum; j ++ ) {
                    auto to_id = (shard_id + ctx.taasContext.kTxnNodeNum - j) % ctx.taasContext.kTxnNodeNum;
                    if (to_id == ctx.taasContext.txn_node_ip_index) continue;
                    remote_server_send_txn_num_local->IncCount(message_epoch, to_id, 1);
                }

//                TransactionCache::epoch_txn_map[message_epoch_mod]->insert(csn_temp, txn_ptr);


                shard_handled_remote_txn_num_local->IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::RemoteServerTxn : {
                remote_server_should_handle_txn_num_local->IncCount(message_epoch, message_server_id, 1);
//                TransactionCache::epoch_txn_map[message_epoch_mod]->insert(csn_temp, txn_ptr);
                MergeQueueEnqueue(message_epoch, txn_ptr);
                CommitQueueEnqueue(message_epoch, txn_ptr);
                remote_server_received_txn_num_local->IncCount(message_epoch, message_server_id, 1);
                remote_server_handled_txn_num_local->IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpTxn : {
//                TransactionCache::epoch_back_txn_map[message_epoch_mod]->insert(csn_temp, txn_ptr);
                backup_received_txn_num_local->IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochShardEndFlag : {
//                LOG(INFO) << "Receive Handle EpochShardEndFlag epoch " << message_epoch;
                shard_should_receive_txn_num.IncCount(message_epoch, message_server_id,txn_ptr->csn());
                shard_received_pack_num.IncCount(message_epoch, message_server_id, 1);
                CheckEpochShardReceiveComplete(message_epoch);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::EpochShardACK);
                break;
            }
            case proto::EpochRemoteServerEndFlag : {
//                LOG(INFO) << "Receive Handle EpochRemoteServerEndFlag epoch " << message_epoch;
                remote_server_should_receive_txn_num.IncCount(message_epoch, message_server_id,txn_ptr->csn());
                remote_server_received_pack_num.IncCount(message_epoch, message_server_id, 1);
                CheckEpochRemoteServerReceiveComplete(message_epoch);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::EpochRemoteServerACK);
                break;
            }
            case proto::TxnType::EpochBackUpEndFlag : {
//                LOG(INFO) << "Receive Handle EpochBackUpEndFlag epoch " << message_epoch;
                backup_should_receive_txn_num.IncCount(message_epoch, message_server_id, txn_ptr->csn());
                backup_received_pack_num.IncCount(message_epoch, message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::BackUpACK);
                break;
            }
            case proto::EpochCommittedTxnEndFlag : {
//                LOG(INFO) << "Receive Handle EpochCommittedTxnEndFlag epoch " << message_epoch;
                /// do nothing
                break;
            }
            case proto::TxnType::AbortSet : {
//                LOG(INFO) << "Receive AbortSet epoch " << message_epoch;
                UpdateEpochAbortSet();
//                LOG(INFO) << "Receive ==Handled== AbortSet epoch " << message_epoch;
                abort_set_received_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::AbortSetACK);
                break;
            }
            case proto::TxnType::InsertSet : {
//                LOG(INFO) << "Receive Handle InsertSet epoch " << message_epoch;
                insert_set_received_num.IncCount(message_epoch, message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::InsertSetACK);
                break;
            }
            case proto::TxnType::EpochShardACK : {
//                LOG(INFO) << "Receive Handle EpochShardACK epoch " << message_epoch;
                shard_received_ack_num.IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::EpochRemoteServerACK : {
//                LOG(INFO) << "Receive Handle EpochRemoteServerACK epoch " << message_epoch;
                remote_server_received_ack_num.IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpACK : {
//                LOG(INFO) << "Receive Handle BackUpACK epoch " << message_epoch;
                backup_received_ack_num.IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::AbortSetACK : {
//                LOG(INFO) << "Receive Handle AbortSetACK epoch " << message_epoch;
                abort_set_received_ack_num.IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::InsertSetACK : {
//                LOG(INFO) << "Receive Handle InsertSetACK epoch " << message_epoch;
                insert_set_received_ack_num.IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochLogPushDownComplete : {
//                LOG(INFO) << "Receive Handle EpochLogPushDownComplete epoch " << message_epoch;
                redo_log_push_down_ack_num.IncCount(message_epoch, message_server_id, 1);
            break;
        }
        case proto::NullMark:
            case proto::TxnType_INT_MIN_SENTINEL_DO_NOT_USE_:
            case proto::TxnType_INT_MAX_SENTINEL_DO_NOT_USE_:
            case proto::CommittedTxn:
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
        return true;
    }

    bool EpochMessageReceiveHandler::UpdateEpochAbortSet() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.taasContext.kCacheMaxLength;
        for(int i = 0; i < txn_ptr->row_size(); i ++) {
            TransactionCache::epoch_abort_txn_set[message_epoch_mod]->insert(txn_ptr->row(i).key(), txn_ptr->row(i).data());
        }
        return true;
    }






    uint64_t EpochMessageReceiveHandler::getMultiModelTxnId() {
        for(auto i = 0; i < txn_ptr->row_size(); i ++) {
            const auto &row = txn_ptr->row(i);
            if (row.op_type() == proto::OpType::Read) {
                continue;
            }
            std::string tempData = txn_ptr->row(0).data();
            std::string tempKey = txn_ptr->row(0).key();
            uint64_t index;
            if (!tempData.empty()) {
                index = tempData.find("tid:");
                if (index < tempData.length()) {
                    auto tid = std::strtoull(&tempData.at(index), nullptr, 10);
                    return tid;
                }
            }
        }
        return 0;
    }

    void EpochMessageReceiveHandler::HandleMultiModelClientSubTxn(const uint64_t& txn_id) {
        shard_should_handle_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
        txn_ptr->set_commit_epoch(message_epoch);
        txn_ptr->set_csn(txn_id);
        txn_ptr->set_txn_server_id(local_server_id);
        SetMessageRelatedCountersInfo();
        ReadValidateQueueEnqueue(message_epoch, txn_ptr);
        CommitQueueEnqueue(message_epoch, txn_ptr);
        shard_handled_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
    }

    bool EpochMessageReceiveHandler::HandleMultiModelClientTxn() {
        std::shared_ptr<MultiModelTxn> multiModelTxn;
        uint64_t txn_id;
        if(txn_ptr->storage_type() == "mot" || txn_ptr->storage_type() == "nebula") {
            txn_id = getMultiModelTxnId();
        }
        else {
            txn_id = txn_ptr->client_txn_id();
        }
        TransactionCache::MultiModelTxnMap.getValue(std::to_string(txn_id), multiModelTxn);
        if(txn_ptr->storage_type() == "kv") {
            multiModelTxn->total_txn_num = txn_ptr->csn(); // total sub txn num
        }
        multiModelTxn->received_txn_num += 1;
        if(multiModelTxn->total_txn_num == multiModelTxn->received_txn_num) {
            message_epoch = EpochManager::GetPhysicalEpoch();
            shard_should_handle_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
            txn_ptr = multiModelTxn->kv;
            HandleMultiModelClientSubTxn(txn_id);
            if(multiModelTxn->sql != nullptr) {
                txn_ptr = multiModelTxn->sql;
                HandleMultiModelClientSubTxn(txn_id);
            }
            if(multiModelTxn->gql != nullptr) {
                txn_ptr = multiModelTxn->gql;
                HandleMultiModelClientSubTxn(txn_id);
            }
            shard_handled_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
        }
        return true;
    }

}