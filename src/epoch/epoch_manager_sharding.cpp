//
// Created by zwx on 23-7-3.
//

#include "epoch/epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "message/epoch_message_receive_handler.h"
#include "transaction/merge.h"

#include "string"
#include "tools/thread_pool_light.h"

namespace Taas {
//
//    bool ShardEpochManager::CheckEpochMergeState(const Context& ctx) {
//        auto res = false;
//        while (EpochManager::IsShardMergeComplete(merge_epoch.load()) &&
//               merge_epoch.load() < EpochManager::GetPhysicalEpoch()) {
//            merge_epoch.fetch_add(1);
//        }
//        auto i = merge_epoch.load();
//        while(i < EpochManager::GetPhysicalEpoch() &&
//              (ctx.taasContext.kTxnNodeNum == 1 ||
//               (EpochMessageReceiveHandler::CheckEpochShardSendComplete(i) &&
//                       EpochMessageReceiveHandler::CheckEpochShardReceiveComplete(i) &&
//                       EpochMessageReceiveHandler::CheckEpochBackUpComplete(i))
//              ) &&
//              Merger::CheckEpochMergeComplete(i)
//                ) {
//            EpochManager::SetShardMergeComplete(i, true);
//            merge_epoch.fetch_add(1);
//            LOG(INFO) << "**** Finished Epoch Merge Epoch : " << i << "****\n";
//            i ++;
//            res = true;
//        }
//        return res;
//    }
//
//    bool ShardEpochManager::CheckEpochAbortMergeState(const Context& ctx) {
//        auto i = abort_set_epoch.load();
//        if(i >= merge_epoch.load() && commit_epoch.load() >= abort_set_epoch.load()) return false;
//        if(EpochManager::IsAbortSetMergeComplete(i)) return true;
//        if( i < merge_epoch.load()  && EpochManager::IsShardMergeComplete(i) &&
//            (ctx.taasContext.kTxnNodeNum == 1 || EpochMessageReceiveHandler::CheckEpochAbortSetMergeComplete(i))) {
//
//            EpochManager::SetAbortSetMergeComplete(i, true);
//            abort_set_epoch.fetch_add(1);
//            LOG(INFO) << "******** Finished Abort Set Merge Epoch : " << i << "********\n";
//            i ++;
//            return true;
//        }
//        return false;
//    }

    static uint64_t last_total_commit_txn_num = 0;
//    bool ShardEpochManager::CheckEpochCommitState(const Context& ctx) {
//        if(commit_epoch.load() >= abort_set_epoch.load()) return false;
//        auto i = commit_epoch.load();
//        if( i < abort_set_epoch.load() && EpochManager::IsShardMergeComplete(i) &&
//            EpochManager::IsAbortSetMergeComplete(i) &&
//            Merger::CheckEpochCommitComplete(i)
//                ) {
//            EpochManager::SetCommitComplete(i, true);
//            auto epoch_commit_success_txn_num = ThreadCounters::GetAllThreadLocalCountNum(i,
//                                                           ThreadCounters::epoch_record_committed_txn_num_local_vec);
//            total_commit_txn_num += epoch_commit_success_txn_num;///success
//            if(i % ctx.taasContext.print_mode_size == 0) {
//                LOG(INFO) << PrintfToString("************ 完成一个Epoch的合并 Epoch: %lu, EpochSuccessCommitTxnNum: %lu, EpochCommitTxnNum: %lu ************\n",
//                                        i, epoch_commit_success_txn_num, EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num);
//                LOG(INFO) << PrintfToString("Epoch: %lu ClearEpoch: %lu, SuccessTxnNumber %lu, ToTalSuccessLatency %lu, SuccessAvgLatency %lf, TotalCommitTxnNum %lu, TotalCommitlatency %lu, TotalCommitAvglatency %lf ************\n",
//                                            i, clear_epoch.load(),
//                                            EpochMessageSendHandler::TotalSuccessTxnNUm.load(), EpochMessageSendHandler::TotalSuccessLatency.load(),
//                                            (((double)EpochMessageSendHandler::TotalSuccessLatency.load()) / ((double)EpochMessageSendHandler::TotalSuccessTxnNUm.load())),
//                                            EpochMessageSendHandler::TotalTxnNum.load(),///receive from client
//                                            EpochMessageSendHandler::TotalLatency.load(),
//                                            (((double)EpochMessageSendHandler::TotalLatency.load()) / ((double)EpochMessageSendHandler::TotalTxnNum.load())));
//            }
//            last_total_commit_txn_num = EpochMessageSendHandler::TotalTxnNum.load();
//            commit_epoch.fetch_add(1);
//            EpochManager::AddLogicalEpoch();
//            return true;
//        }
//        return false;
//    }


    void ShardEpochManager::EpochLogicalTimerManagerThreadMain(const Context& ctx) {
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        uint64_t epoch = 1;
        OUTPUTLOG("===== Start Epoch的合并 ===== ", epoch);
        util::thread_pool_light workers(5);
//        while(!EpochManager::IsInitOK() || EpochManager::GetPhysicalEpoch() < 10) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){

            while(epoch >= EpochManager::GetPhysicalEpoch()) usleep(logical_sleep_timme);
            auto time1 = now_to_us();
//                LOG(INFO) << "**** Start Epoch Merge Epoch : " << epoch << "****\n";
            while(!EpochMessageReceiveHandler::CheckEpochClientTxnHandleComplete(epoch)) usleep(logical_sleep_timme);
//            workers.push_emergency_task([epoch, &ctx] () {
//                EpochMessageSendHandler::SendEpochShardEndMessage(ctx.taasContext.txn_node_ip_index, epoch, ctx.taasContext.kTxnNodeNum);
//            });
//            while(!EpochMessageReceiveHandler::IsShardSendFinish(epoch)) usleep(logical_sleep_timme);
//            LOG(INFO) << "**** finished IsShardSendFinish : " << epoch << "****\n";

//            while(!EpochMessageReceiveHandler::IsShardACKReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsShardACKReceiveComplete : " << epoch << "****\n";
//            while(!EpochMessageReceiveHandler::CheckEpochShardSendComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished CheckEpochShardSendComplete : " << epoch << "****\n";


//            while(!EpochMessageReceiveHandler::IsShardPackReceiveComplete(epoch)) usleep(logical_sleep_timme);
//            LOG(INFO) << "**** finished IsShardPackReceiveComplete : " << epoch << "****\n";
//            while(!EpochMessageReceiveHandler::IsShardTxnReceiveComplete(epoch)) usleep(logical_sleep_timme);
//            LOG(INFO) << "**** finished ShardTxn : " << epoch << "****\n";
            while(!EpochMessageReceiveHandler::CheckEpochShardReceiveComplete(epoch)) usleep(logical_sleep_timme);

//            auto time2 = now_to_us();
//            LOG(INFO) << "**** Finished CheckEpochShardReceiveComplete Epoch : " << epoch << ",time cost : " << time2 - time1 << "****\n";

            while(!EpochMessageReceiveHandler::CheckEpochShardTxnHandleComplete(epoch)) usleep(logical_sleep_timme);
//            LOG(INFO) << "**** finished IsEpochShardTxnHandleComplete : " << epoch << "****\n";
//            workers.push_emergency_task([epoch, &ctx] () {
//                EpochMessageSendHandler::SendEpochRemoteServerEndMessage(ctx.taasContext.txn_node_ip_index, epoch, ctx.taasContext.kTxnNodeNum);
//            });
//            LOG(INFO) << "**** finished SendEpochRemoteServerEndMessage : " << epoch << "****\n";
//            while(!EpochMessageReceiveHandler::IsRemoteServerSendFinish(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsRemoteServerSendFinish : " << epoch << "****\n";
//            while(!EpochMessageReceiveHandler::IsRemoteServerACKReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsRemoteServerACKReceiveComplete : " << epoch << "****\n";

//            while(!EpochMessageReceiveHandler::CheckEpochRemoteServerSendComplete(epoch)) usleep(logical_sleep_timme);
//            LOG(INFO) << "**** finished RemoteServerTxn : " << epoch << "****\n";
//                auto time3 = now_to_us();
//                LOG(INFO) << "**** Finished CheckEpochRemoteServerSendComplete Epoch : " << epoch << ",time cost : " << time3 - time2 << "****\n";

//            while(!EpochMessageReceiveHandler::IsBackUpACKReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsBackUpACKReceiveComplete : " << epoch << "****\n";
//            while(!EpochMessageReceiveHandler::IsBackUpSendFinish(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsBackUpSendFinish : " << epoch << "****\n";
            while(!EpochMessageReceiveHandler::CheckEpochBackUpComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** Finished CheckEpochBackUpComplete Epoch : " << epoch << "****\n";

//            while(!EpochMessageReceiveHandler::IsRemoteServerPackReceiveComplete(epoch)) usleep(logical_sleep_timme);
////                LOG(INFO) << "**** finished IsRemoteServerReceiveComplete : " << epoch << "****\n";
//            while(!EpochMessageReceiveHandler::IsRemoteServerTxnReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsRemoteServerTxnReceiveComplete : " << epoch << "****\n";
            while(!EpochMessageReceiveHandler::CheckEpochRemoteServerReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** Finished CheckEpochRemoteServerReceiveComplete Epoch : " << epoch << "****\n";
//            auto time4 = now_to_us();
            while(!Merger::CheckEpochMergeComplete(epoch)) usleep(logical_sleep_timme);
            EpochManager::SetEpochMergeComplete(epoch, true);
            workers.push_emergency_task([epoch, &ctx] () {
                EpochMessageSendHandler::SendAbortSet(ctx.taasContext.txn_node_ip_index, epoch, ctx.taasContext.kTxnNodeNum);
            });
            merge_epoch.fetch_add(1);
            auto time5 = now_to_us();
//                LOG(INFO) << "**** Finished Epoch Merge Epoch : " << epoch << ",time cost : " << time5 - time1 << "****\n";

//            while(!EpochMessageReceiveHandler::IsAbortSetACKReceiveComplete(epoch)) usleep(logical_sleep_timme);
////                LOG(INFO) << "**** finished IsAbortSetACKReceiveComplete : " << epoch << "****\n";
//            while(!EpochMessageReceiveHandler::IsAbortSetReceiveComplete(epoch)) usleep(logical_sleep_timme);
////                LOG(INFO) << "**** finished IsAbortSetReceiveComplete : " << epoch << "****\n";
            while(!EpochMessageReceiveHandler::CheckEpochAbortSetMergeComplete(epoch)) usleep(logical_sleep_timme);
            EpochManager::SetAbortSetMergeComplete(epoch, true);
            abort_set_epoch.fetch_add(1);
            auto time6 = now_to_us();
//            LOG(INFO) << "******* Finished Abort Set Merge Epoch : " << epoch << "Time cost :" << time6 - time5 << "********\n";


            while(!Merger::CheckEpochCommitComplete(epoch)) usleep(logical_sleep_timme);
            EpochManager::SetCommitComplete(epoch, true);
//            LOG(INFO) << "******* Finished CommitComplete Epoch : " << epoch << "********\n";
            while(!Merger::CheckEpochRecordCommitted(epoch)) usleep(logical_sleep_timme);
            EpochManager::SetRecordCommitted(epoch, true);
//            LOG(INFO) << "******* Finished RecordCommitted Epoch : " << epoch << "********\n";
            commit_epoch.fetch_add(1);
            EpochManager::AddLogicalEpoch();
            auto time7 = now_to_us();
            auto epoch_commit_success_txn_num = ThreadCounters::GetAllThreadLocalCountNum(epoch,
                                               ThreadCounters::epoch_record_committed_txn_num_local_vec);
            total_commit_txn_num += epoch_commit_success_txn_num;///success
            if(epoch % ctx.taasContext.print_mode_size == 0) {
                LOG(INFO) << PrintfToString(
                        "************ 完成一个Epoch的合并 Physical Epoch %lu, Logical Epoch: %lu, Local EpochSuccessCommitTxnNum: %lu,TotalSuccessTxnNum: %lu, EpochCommitTxnNum: %lu ",
                        EpochManager::GetPhysicalEpoch(), epoch, epoch_commit_success_txn_num, total_commit_txn_num,
                        EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num)
                          << "\n Time Cost  Epoch: " << epoch
                          << ",Merge time cost : " << time5 - time1
                          << ",Abort Set Merge time cost : " << time6 - time5
                          << ",Commit time cost : " << time7 - time6
                          << "Total Time Cost ****" << time7 - time1
                          << "****\n";
                OUTPUTLOG("===== Logical Start Epoch的合并 ===== ", epoch);
            }
            epoch ++;
            last_total_commit_txn_num = EpochMessageSendHandler::TotalTxnNum.load();
        }
    }
}