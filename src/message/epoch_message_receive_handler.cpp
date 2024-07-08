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
        local_server_id = ctx.taasContext.txn_node_ip_index;
        max_length = ctx.taasContext.kCacheMaxLength;
        ThreadCountersInit(ctx);
        return true;
    }

    bool EpochMessageReceiveHandler::StaticInit(const Context& context) {
        return true;
    }

    bool EpochMessageReceiveHandler::StaticClear(uint64_t& epoch) {
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
        while(!EpochManager::IsTimerStop()) {
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
        for (int i = 0; i < (int)ctx.taasContext.kHandleTxnMessageNumOfEachTraversal; ++i) {
            if (MessageQueue::listen_message_txn_queue->try_dequeue(message_ptr)) {
                sleep_flag = false;
                if (message_ptr == nullptr) return;
                if (message_ptr->empty()) continue;
                message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                   message_ptr->size());
                msg_ptr = std::make_unique<proto::Message>();
                res = UnGzip(msg_ptr.get(), message_string_ptr.get());
                assert(res);
                txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
                HandleReceivedTxn();
                txn_ptr.reset();
            }
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
        for (int i = 0; i < (int)ctx.taasContext.kHandleEpochMessageNumOfEachTraversal; ++i) {
            if(MessageQueue::listen_message_epoch_queue->try_dequeue(message_ptr)) {
                sleep_flag = false;
                if (message_ptr == nullptr) return;
                if(message_ptr->empty()) continue;
                message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),message_ptr->size());
                msg_ptr = std::make_unique<proto::Message>();
                res = UnGzip(msg_ptr.get(), message_string_ptr.get());
                assert(res);
                txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
                HandleReceivedTxn();
                txn_ptr.reset();
            }
        }
    }

    bool EpochMessageReceiveHandler::SetMessageRelatedCountersInfo() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = message_epoch % ctx.taasContext.kCacheMaxLength;
        message_server_id = txn_ptr->txn_server_id();
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
                    shard_should_handle_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
                    txn_ptr->set_commit_epoch(message_epoch);
                    txn_ptr->set_csn(now_to_us());
                    txn_ptr->set_txn_server_id(ctx.taasContext.txn_node_ip_index);
                    txn_ptr->set_txn_type(proto::RemoteServerTxn);/// change to server txn, then, use server_id to check
                    SetMessageRelatedCountersInfo();
                    ReadValidateQueueEnqueue(message_epoch, txn_ptr);
                    shard_handled_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
                }
                break;
            }
            case proto::TxnType::ShardedClientTxn :
            case proto::TxnType::EpochShardEndFlag : {
                break;
            }
            case proto::TxnType::RemoteServerTxn : {
                remote_server_should_handle_txn_num_local->IncCount(message_epoch, local_server_id, 1);
                TransactionCache::epoch_txn_map[message_epoch_mod]->insert(csn_temp, txn_ptr);
                MergeQueueEnqueue(message_epoch, txn_ptr);
                CommitQueueEnqueue(message_epoch, txn_ptr);
                remote_server_received_txn_num_local->IncCount(message_epoch,message_server_id, 1);
                remote_server_handled_txn_num_local->IncCount(message_epoch, local_server_id, 1);
                break;
            }
            case proto::TxnType::EpochRemoteServerEndFlag : {
//                LOG(INFO) << "EpochRemoteServerEndFlag";
                remote_server_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                remote_server_received_pack_num.IncCount(message_epoch, message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::EpochRemoteServerACK);
                break;
            }
            case proto::TxnType::BackUpTxn : {
                TransactionCache::epoch_back_txn_map[message_epoch_mod]->insert(csn_temp, txn_ptr);
                backup_received_txn_num_local->IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochBackUpEndFlag : {
                backup_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                backup_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::BackUpACK);
                break;
            }
            case proto::TxnType::AbortSet : {
                UpdateEpochAbortSet();
                abort_set_received_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::AbortSetACK);
                break;
            }
            case proto::TxnType::InsertSet : {
                insert_set_received_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::InsertSetACK);
                break;
            }
            case proto::TxnType::EpochShardACK : {
                shard_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::EpochRemoteServerACK: {
//                LOG(INFO) << "EpochRemoteServerACK";
                remote_server_received_ack_num.IncCount(message_epoch,message_server_id, 1);
            }
            case proto::TxnType::BackUpACK : {
                backup_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::AbortSetACK : {
                abort_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::InsertSetACK : {
                insert_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochLogPushDownComplete : {
                redo_log_push_down_ack_num.IncCount(message_epoch,message_server_id, 1);
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
            case proto::EpochCommittedTxnEndFlag:
                break;
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
        txn_ptr->set_txn_server_id(ctx.taasContext.txn_node_ip_index);
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



    bool EpochMessageReceiveHandler::UpdateEpochAbortSet() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.taasContext.kCacheMaxLength;
        for(int i = 0; i < txn_ptr->row_size(); i ++) {
            TransactionCache::epoch_abort_txn_set[message_epoch_mod]->insert(txn_ptr->row(i).key(), txn_ptr->row(i).data());
        }
        return true;
    }



}