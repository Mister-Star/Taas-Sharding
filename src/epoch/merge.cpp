//
// Created by 周慰星 on 11/15/22.
//

#include <utility>
#include "epoch/merge.h"
#include "epoch/epoch_manager.h"
#include "tools/utilities.h"

namespace Taas {

    void Merger::Init(uint64_t id_, Context ctx_) {
//        message_ptr = nullptr;
//        pack_param = nullptr;
//        thread_id = 0;
//        epoch = 0;
//        res = false, sleep_flag = false;

        txn_ptr = nullptr;
        thread_id = id_;
        ctx = std::move(ctx_);
        message_handler.Init(thread_id, ctx);
    }

    bool Merger::EpochMerge() {
        sleep_flag = false;
        while (merge_queue->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
            res = true;
            epoch = txn_ptr->commit_epoch();
            if (!CRDTMerge::ValidateReadSet(ctx, *(txn_ptr))){
                res = false;
            }
            if (!CRDTMerge::MultiMasterCRDTMerge(ctx, *(txn_ptr))) {
                res = false;
            }
            if(res) {
                EpochManager::should_commit_txn_num.IncCount(epoch, thread_id, 1);
                epoch_commit_queue[epoch % ctx.kCacheMaxLength]->enqueue(std::move(txn_ptr));
                epoch_commit_queue[epoch % ctx.kCacheMaxLength]->enqueue(nullptr);
            }
            EpochManager::merged_txn_num.IncCount(epoch, thread_id, 1);

            sleep_flag = true;
        }
        return sleep_flag;
    }

    ///日志存储为整个事务模式
    bool Merger::EpochCommit_RedoLog_TxnMode() {
        sleep_flag = false;
        //RC or RR Isolation
        epoch = EpochManager::GetLogicalEpoch();
        for(; epoch < EpochManager::GetPhysicalEpoch(); epoch ++) {
            auto epoch_mod = epoch % ctx.kCacheMaxLength;
            if(EpochManager::IsCommitComplete(epoch)) continue;
            if(EpochManager::IsShardingMergeComplete(epoch)) {
                while (epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
                    //不对分片事务进行commit处理
                    epoch = txn_ptr->commit_epoch();
                    EpochManager::committed_txn_num.IncCount(epoch, thread_id, 1);
                    sleep_flag = true;
                }
                ///validation phase
                while (epoch_local_txn_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
                    epoch = txn_ptr->commit_epoch();
                    if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
                        auto key = std::to_string(txn_ptr->client_txn_id());
                        EpochManager::abort_txn_set.insert(key,key);
                        MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
                    }
                    else {
                        EpochManager::record_commit_txn_num.IncCount(epoch, thread_id, 1);
                        CRDTMerge::Commit(ctx, *(txn_ptr));
                        CRDTMerge::RedoLog(ctx, *(txn_ptr));
                        EpochManager::record_committed_txn_num.IncCount(epoch, thread_id, 1);
                        MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Commit);
                    }
                    EpochManager::committed_txn_num.IncCount(epoch, thread_id, 1);
                    sleep_flag = true;
                }
            }
        }
        //SI Isolation

        //SER Isolation

        return sleep_flag;
    }

    ///日志存储为分片模式
    bool Merger::EpochCommit_RedoLog_ShardingMode() {
        sleep_flag = false;
        //RC or RR Isolation
        epoch = EpochManager::GetLogicalEpoch();
        for(; epoch < EpochManager::GetPhysicalEpoch(); epoch ++) {
            auto epoch_mod = epoch % ctx.kCacheMaxLength;
            if(EpochManager::IsCommitComplete(epoch)) continue;

            if(EpochManager::IsShardingMergeComplete(epoch)) {
                while (epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
                    if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
                        //do nothing
                    } else {
                        EpochManager::record_commit_txn_num.IncCount(epoch, thread_id, 1);
                        CRDTMerge::Commit(ctx, *(txn_ptr));
                        CRDTMerge::RedoLog(ctx, *(txn_ptr));
                        EpochManager::record_committed_txn_num.IncCount(epoch, thread_id, 1);
                    }
                    EpochManager::committed_txn_num.IncCount(epoch, thread_id, 1);
                    sleep_flag = true;
                }
                while (epoch_local_txn_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
                    if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
                        auto key = std::to_string(txn_ptr->client_txn_id());
                        EpochManager::abort_txn_set.insert(key,key);
                        MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
                    } else {
                        MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Commit);
                    }
                    EpochManager::committed_txn_num.IncCount(epoch, thread_id, 1);
                    sleep_flag = true;
                }
            }
        }
        return sleep_flag;
    }


    bool Merger::Run(uint64_t id_, Context ctx_) {
        Init(id_, std::move(ctx_));
        while(!EpochManager::IsTimerStop()) {
            sleep_flag = false;

            sleep_flag = sleep_flag | EpochCommit_RedoLog_TxnMode();

            sleep_flag = sleep_flag | EpochMerge();

            if(!sleep_flag) usleep(200);
        }
        return true;
    }
}