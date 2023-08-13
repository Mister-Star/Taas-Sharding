//
// Created by zwx on 23-7-3.
//

#include "epoch/epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "message/handler_receive.h"
#include "storage/redo_loger.h"
#include "transaction/merge.h"

#include "string"

namespace Taas {

    bool ShardingEpochManager::CheckEpochMergeState(const Context& ctx) {
        auto res = false;
        while (EpochManager::IsShardingMergeComplete(merge_epoch.load()) &&
               merge_epoch.load() < EpochManager::GetPhysicalEpoch()) {
            merge_epoch.fetch_add(1);
        }
        auto i = merge_epoch.load();
        while(i < EpochManager::GetPhysicalEpoch() &&
              (ctx.kTxnNodeNum == 1 ||
               (MessageReceiveHandler::CheckEpochShardingSendComplete(ctx, i) &&
                MessageReceiveHandler::CheckEpochShardingReceiveComplete(ctx, i) &&
                MessageReceiveHandler::CheckEpochBackUpComplete(ctx, i))
              ) &&
              Merger::CheckEpochMergeComplete(ctx, i)
                ) {
            EpochManager::SetShardingMergeComplete(i, true);
            merge_epoch.fetch_add(1);
            LOG(INFO) << "**** Finished Epoch Merge Epoch : " << i << "****\n";
            i ++;
            res = true;
        }
        return res;
    }

    bool ShardingEpochManager::CheckEpochAbortMergeState(const Context& ctx) {
        auto i = abort_set_epoch.load();
        if(i >= merge_epoch.load() && commit_epoch.load() >= abort_set_epoch.load()) return false;
        if(EpochManager::IsAbortSetMergeComplete(i)) return true;
        if( i < merge_epoch.load()  && EpochManager::IsShardingMergeComplete(i) &&
            (ctx.kTxnNodeNum == 1 || MessageReceiveHandler::CheckEpochAbortSetMergeComplete(ctx, i))) {

            EpochManager::SetAbortSetMergeComplete(i, true);
            abort_set_epoch.fetch_add(1);
            LOG(INFO) << "******** Finished Abort Set Merge Epoch : " << i << "********\n";
            i ++;
            return true;
        }
        return false;
    }

    static uint64_t last_total_commit_txn_num = 0;
    bool ShardingEpochManager::CheckEpochCommitState(const Context& ctx) {
        if(commit_epoch.load() >= abort_set_epoch.load()) return false;
        auto i = commit_epoch.load();
        if( i < abort_set_epoch.load() && EpochManager::IsShardingMergeComplete(i) &&
            EpochManager::IsAbortSetMergeComplete(i) &&
            Merger::CheckEpochCommitComplete(ctx, i)
                ) {
            EpochManager::SetCommitComplete(i, true);
            auto epoch_commit_success_txn_num = Merger::epoch_record_committed_txn_num.GetCount(i);
            total_commit_txn_num += epoch_commit_success_txn_num;///success
            LOG(INFO) << PrintfToString("************ 完成一个Epoch的合并 Epoch: %lu, EpochSuccessCommitTxnNum: %lu, EpochCommitTxnNum: %lu ************\n",
                                        i, epoch_commit_success_txn_num, MessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num);
            if(i % ctx.print_mode_size == 0) {
                LOG(INFO) << PrintfToString("Epoch: %lu ClearEpoch: %lu, SuccessTxnNumber %lu, ToTalSuccessLatency %lu, SuccessAvgLatency %lf, TotalCommitTxnNum %lu, TotalCommitlatency %lu, TotalCommitAvglatency %lf ************\n",
                                            i, clear_epoch.load(),
                                            MessageSendHandler::TotalSuccessTxnNUm.load(), MessageSendHandler::TotalSuccessLatency.load(),
                                            (((double)MessageSendHandler::TotalSuccessLatency.load()) / ((double)MessageSendHandler::TotalSuccessTxnNUm.load())),
                                            MessageSendHandler::TotalTxnNum.load(),///receive from client
                                            MessageSendHandler::TotalLatency.load(),
                                            (((double)MessageSendHandler::TotalLatency.load()) / ((double)MessageSendHandler::TotalTxnNum.load())));
            }
            last_total_commit_txn_num = MessageSendHandler::TotalTxnNum.load();
            i ++;
            commit_epoch.fetch_add(1);
            EpochManager::AddLogicalEpoch();
            return true;
        }
        return false;
    }
}

