//
// Created by 周慰星 on 11/15/22.
//

#include <utility>
#include "transaction/merge.h"
#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "storage/redo_loger.h"
#include "transaction/transaction_cache.h"

namespace Taas {

    Context Merger::ctx;

    void Merger::Init(uint64_t id_) {
        txn_ptr.reset();
        message_ptr = nullptr;
        sharding_num = ctx.taasContext.kTxnNodeNum;
        local_server_id = ctx.taasContext.txn_node_ip_index;
    }

    void Merger::StaticInit(const Context &ctx_) {
        ctx = ctx_;
    }


    void Merger::ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_read_validate_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->server_id(), 1);
        TransactionCache::epoch_read_validate_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_read_validate_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void Merger::MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_merge_txn_num_local->IncCount(epoch_mod_temp, txn_ptr->server_id(), 1);
        TransactionCache::epoch_merge_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_merge_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void Merger::CommitQueueEnqueue(uint64_t& epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_commit_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->server_id(), 1);
        TransactionCache::epoch_commit_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_commit_queue[epoch_mod_temp]->enqueue(nullptr);
    }

    bool Merger::MergeQueueTryDequeue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        ///not use for now
        return false;
    }
    bool Merger::CommitQueueTryDequeue(uint64_t& epoch_, std::shared_ptr<proto::Transaction> txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        return TransactionCache::epoch_commit_queue[epoch_mod_temp]->try_dequeue(txn_ptr_);
    }




    void Merger::Send() {
        if(ctx.taasContext.taasMode == TaasMode::Sharding) {
            /// already send
        }
        else if(ctx.taasContext.taasMode == TaasMode::MultiMaster && txn_ptr->server_id() == ctx.taasContext.txn_node_ip_index) {
            write_set = std::make_shared<proto::Transaction>();
            write_set->set_csn(txn_ptr->csn());
            write_set->set_commit_epoch(txn_ptr->commit_epoch());
            write_set->set_server_id(txn_ptr->server_id());
            write_set->set_client_ip(txn_ptr->client_ip());
            write_set->set_client_txn_id(txn_ptr->client_txn_id());
            write_set->set_sharding_id(ctx.taasContext.txn_node_ip_index);
            write_set->set_txn_type(proto::RemoteServerTxn);
            for(auto i = 0; i < txn_ptr->row_size(); i ++) { /// for SI isolation only seng the wriet set.
                const auto& row = txn_ptr->row(i);
                if(row.op_type() == proto::OpType::Read) continue;
                auto row_ptr = write_set->add_row();
                (*row_ptr) = row;
            }
            /// only local txn send its write set or complete txn
            /// SI only send write set
            /// if you want to achieve SER, you need to send the complete txn
            sharding_should_send_txn_num_local->IncCount(message_epoch, message_server_id, 1);
            backup_should_send_txn_num_local->IncCount(message_epoch, message_server_id, 1);
            EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, write_set, proto::TxnType::RemoteServerTxn);
            EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, txn_ptr, proto::TxnType::BackUpTxn);
            sharding_send_txn_num_local->IncCount(message_epoch, message_server_id, 1);
            backup_send_txn_num_local->IncCount(message_epoch, message_server_id, 1);
            MergeQueueEnqueue(message_epoch, txn_ptr);
            CommitQueueEnqueue(message_epoch, txn_ptr);
        }
    }

    void Merger::ReadValidate() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = message_epoch % ctx.taasContext.kCacheMaxLength;
        message_server_id = txn_ptr->server_id();
        if (CRDTMerge::ValidateReadSet(txn_ptr)) {
            Send();
        }
        else {
            total_read_version_check_failed_txn_num_local.fetch_add(1);
            csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
            TransactionCache::epoch_abort_txn_set[message_epoch_mod]->insert(csn_temp, csn_temp);
            if(ctx.taasContext.taasMode == TaasMode::MultiMaster && txn_ptr->server_id() == ctx.taasContext.txn_node_ip_index)
                EpochMessageSendHandler::SendTxnCommitResultToClient(txn_ptr, proto::TxnState::Abort);
        }
        epoch_read_validated_txn_num_local->IncCount(message_epoch, message_server_id, 1);
    }

    void Merger::Merge() {
        auto time1 = now_to_us();
        epoch = txn_ptr->commit_epoch();
        CRDTMerge::MultiMasterCRDTMerge(txn_ptr);
        total_merge_txn_num_local.fetch_add(1);
        total_merge_latency_local.fetch_add(now_to_us() - time1);
        epoch_merged_txn_num_local->IncCount(epoch, txn_server_id, 1);
    }

    void Merger::Commit() {
        auto time1 = now_to_us();
        ///validation phase
        if (!CRDTMerge::ValidateWriteSet(txn_ptr)) {
            auto key = std::to_string(txn_ptr->client_txn_id());
            total_failed_txn_num_local.fetch_add(1);
            EpochMessageSendHandler::SendTxnCommitResultToClient(txn_ptr, proto::TxnState::Abort);
        } else {
            epoch_record_commit_txn_num_local->IncCount(epoch, txn_ptr->server_id(), 1);
            CRDTMerge::Commit(txn_ptr);
            if(txn_ptr->server_id() == ctx.taasContext.txn_node_ip_index) { /// only local txn do redo log
                RedoLoger::RedoLog(txn_ptr);
            }
            epoch_record_committed_txn_num_local->IncCount(epoch, txn_ptr->server_id(), 1);
            success_commit_txn_num_local.fetch_add(1);
            success_commit_latency_local.fetch_add(now_to_us() - time1);
            EpochMessageSendHandler::SendTxnCommitResultToClient(txn_ptr, proto::TxnState::Commit);
        }
        total_commit_txn_num_local.fetch_add(1);
        total_commit_latency_local.fetch_add(now_to_us() - time1);
        epoch_committed_txn_num_local->IncCount(epoch, txn_ptr->server_id(), 1);
    }

    void Merger::EpochMerge() {
        epoch = EpochManager::GetLogicalEpoch();
        std::mutex mtx;
        std::unique_lock lck(mtx);
        while (!EpochManager::IsTimerStop()) {
            sleep_flag = true;
            epoch = EpochManager::GetLogicalEpoch();
            epoch_mod = epoch % ctx.taasContext.kCacheMaxLength;

            while(TransactionCache::epoch_read_validate_queue[epoch_mod]->try_dequeue(txn_ptr)) { /// only local txn do this procedure
                if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                    ReadValidate();
                    txn_ptr.reset();
                    sleep_flag = false;
                }
            }

            if(!EpochManager::IsShardingMergeComplete(epoch)) {
                while (TransactionCache::epoch_merge_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                        Merge();
                        txn_ptr.reset();
                        sleep_flag = false;
                    }
                }
            }

            if(EpochManager::IsAbortSetMergeComplete(epoch) && !EpochManager::IsCommitComplete(epoch)) {
                while (!EpochManager::IsCommitComplete(epoch) && TransactionCache::epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                        Commit();
                        txn_ptr.reset();
                        sleep_flag = false;
                    }
                }
            }
            if(sleep_flag)
                usleep(merge_sleep_time);
        }
    }
}