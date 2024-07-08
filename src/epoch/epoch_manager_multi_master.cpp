//
// Created by zwx on 23-7-3.
//
#include "epoch/epoch_manager.h"
#include "epoch/epoch_manager_multi_master.h"
#include "message/epoch_message_receive_handler.h"
#include "transaction/merge.h"
//
#include "string"
#include "tools/thread_pool_light.h"

namespace Taas {


    static uint64_t last_total_commit_txn_num = 0;

    void MultiMasterEpochManager::EpochLogicalTimerManagerThreadMain(const Context& ctx) {
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        uint64_t epoch = 1;
        OUTPUTLOG("===== Logical Start Epoch的合并 ===== ", epoch);
        util::thread_pool_light workers(ctx.taasContext.kMergeThreadNum);
        while(!EpochManager::IsInitOK()) usleep(logical_sleep_timme);
        if(ctx.taasContext.kTxnNodeNum > 1) {
            while(!EpochManager::IsTimerStop()){
                auto time1 = now_to_us();
                while(epoch >= EpochManager::GetPhysicalEpoch()) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** Start Epoch Merge Epoch : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::CheckEpochClientTxnHandleComplete(epoch)) usleep(logical_sleep_timme);
                while(!Merger::CheckEpochReadValidateComplete(epoch)) usleep(logical_sleep_timme);
                workers.push_emergency_task([&epoch, &ctx] () {
                    EpochMessageSendHandler::SendEpochShardEndMessage(ctx.taasContext.txn_node_ip_index, epoch, ctx.taasContext.kTxnNodeNum);
                });
                while(!EpochMessageReceiveHandler::IsShardSendFinish(epoch)) usleep(logical_sleep_timme);

//                LOG(INFO) << "**** finished IsShardSendFinish : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::IsShardACKReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsShardACKReceiveComplete : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::CheckEpochShardSendComplete(epoch)) usleep(logical_sleep_timme);
//                auto time2 = now_to_us();
//                LOG(INFO) << "**** Finished CheckEpochShardSendComplete Epoch : " << epoch << ",time cost : " << time2 - time1 << "****\n";

                while(!EpochMessageReceiveHandler::IsShardPackReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsShardPackReceiveComplete : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::IsShardTxnReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsShardTxnReceiveComplete : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::CheckEpochShardReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                auto time3 = now_to_us();
//                LOG(INFO) << "**** Finished CheckEpochShardReceiveComplete Epoch : " << epoch << ",time cost : " << time3 - time2 << "****\n";

                while(!EpochMessageReceiveHandler::IsBackUpACKReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsBackUpACKReceiveComplete : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::IsBackUpSendFinish(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsBackUpSendFinish : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::CheckEpochBackUpComplete(epoch)) usleep(logical_sleep_timme);
//                auto time4 = now_to_us();
//                LOG(INFO) << "**** Finished CheckEpochBackUpComplete Epoch : " << epoch << ",time cost : " << time4 - time3 << "****\n";

                while(!Merger::CheckEpochMergeComplete(epoch)) usleep(logical_sleep_timme);
                EpochManager::SetEpochMergeComplete(epoch, true);
                merge_epoch.fetch_add(1);
                auto time5 = now_to_us();
//                LOG(INFO) << "**** Finished Epoch Merge Epoch : " << epoch << ",time cost : " << time5 - time1 << ",rest time cost : " << time5 - time4 << "****\n";


                /// in multi master mode, there is no need to send and merge shard abort set
//                while(!EpochMessageReceiveHandler::IsAbortSetACKReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsAbortSetACKReceiveComplete : " << epoch << "****\n";
//                while(!EpochMessageReceiveHandler::IsAbortSetReceiveComplete(epoch)) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** finished IsAbortSetReceiveComplete : " << epoch << "****\n";
//                while(!EpochMessageReceiveHandler::CheckEpochAbortSetMergeComplete(epoch)) usleep(logical_sleep_timme);
                EpochManager::SetAbortSetMergeComplete(epoch, true);
                abort_set_epoch.fetch_add(1);
                auto time6 = now_to_us();
//                LOG(INFO) << "******* Finished Abort Set Merge Epoch : " << epoch << ",time cost : " << time6 - time5 << "********\n";


                while(!Merger::CheckEpochCommitComplete(epoch)) usleep(logical_sleep_timme);
                EpochManager::SetCommitComplete(epoch, true);
                commit_epoch.fetch_add(1);
                EpochManager::AddLogicalEpoch();
                auto time7 = now_to_us();
                auto epoch_commit_success_txn_num = ThreadCounters::GetAllThreadLocalCountNum(epoch,
                                                ThreadCounters::epoch_record_committed_txn_num_local_vec);
                total_commit_txn_num += epoch_commit_success_txn_num;///success
//                if(epoch % ctx.taasContext.print_mode_size == 0)
//                    LOG(INFO) << PrintfToString("************ 完成一个Epoch的合并 Physical Epoch %lu, Logical Epoch: %lu, Local EpochSuccessCommitTxnNum: %lu,TotalSuccessTxnNum: %lu, EpochCommitTxnNum: %lu ",
//                                                EpochManager::GetPhysicalEpoch(), epoch, epoch_commit_success_txn_num, total_commit_txn_num,
//                                                EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num)
//                    << ",Time Cost  Epoch: " << epoch
//                    << ",Merge time cost : " << time5 - time1
//                    << ",Abort Set Merge time cost : " << time6 - time5
//                    << ",Commit time cost : " << time7 - time6
//                    << "Total Time Cost ****" << time7 - time1
//                    << "****\n";
//                    OUTPUTLOG("===== Logical Start Epoch的合并 ===== ", epoch);
                if(epoch % ctx.taasContext.print_mode_size == 0) {
                    LOG(INFO) << PrintfToString(
                            "************ 完成一个Epoch的合并 Physical Epoch %lu, Logical Epoch: %lu, Local EpochSuccessCommitTxnNum: %lu,TotalSuccessTxnNum: %lu, EpochCommitTxnNum: %lu ",
                            EpochManager::GetPhysicalEpoch(), epoch, epoch_commit_success_txn_num, total_commit_txn_num,
                            EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num)
                              << ",Time Cost  Epoch: " << epoch
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
        else {
            while(!EpochManager::IsTimerStop()){
                auto time1 = now_to_us();
                while(epoch >= EpochManager::GetPhysicalEpoch()) usleep(logical_sleep_timme);
//                LOG(INFO) << "**** Start Epoch Merge Epoch : " << epoch << "****\n";
                while(!EpochMessageReceiveHandler::CheckEpochClientTxnHandleComplete(epoch)) usleep(logical_sleep_timme);
                while(!Merger::CheckEpochReadValidateComplete(epoch)) usleep(logical_sleep_timme);
//                workers.push_emergency_task([epoch, &ctx] () {
//                    EpochMessageSendHandler::SendEpochEndMessage(ctx.taasContext.txn_node_ip_index, epoch, ctx.taasContext.kTxnNodeNum);
//                });
                while(!Merger::CheckEpochMergeComplete(epoch)) usleep(logical_sleep_timme);
                EpochManager::SetEpochMergeComplete(epoch, true);
                merge_epoch.fetch_add(1);
                auto time5 = now_to_us();
//                LOG(INFO) << "**** Finished Epoch Merge Epoch : " << epoch << ",time cost : " << time5 - time1 << "****\n";
                EpochManager::SetAbortSetMergeComplete(epoch, true);
                abort_set_epoch.fetch_add(1);
                auto time6 = now_to_us();
//                LOG(INFO) << "******* Finished Abort Set Merge Epoch : " << epoch << ",time cost : " << time6 - time5 << "********\n";
                while(!Merger::CheckEpochCommitComplete(epoch)) usleep(logical_sleep_timme);
                EpochManager::SetCommitComplete(epoch, true);
                commit_epoch.fetch_add(1);
                EpochManager::AddLogicalEpoch();
                auto time7 = now_to_us();
                auto epoch_commit_success_txn_num = ThreadCounters::GetAllThreadLocalCountNum(epoch,
                                                           ThreadCounters::epoch_record_committed_txn_num_local_vec);
                total_commit_txn_num += epoch_commit_success_txn_num;///success
//                if(epoch % ctx.taasContext.print_mode_size == 0)
//                    LOG(INFO) << PrintfToString("************ 完成一个Epoch的合并 Physical Epoch %lu, Logical Epoch: %lu, Local EpochSuccessCommitTxnNum: %lu,TotalSuccessTxnNum: %lu, EpochCommitTxnNum: %lu ",
//                                                EpochManager::GetPhysicalEpoch(), epoch, epoch_commit_success_txn_num, total_commit_txn_num,
//                                            EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num)
//                          << ",Time Cost  Epoch: " << epoch
//                          << ",Merge time cost : " << time5 - time1
//                          << ",Abort Set Merge time cost : " << time6 - time5
//                          << ",Commit time cost : " << time7 - time6
//                          << "Total Time Cost ****" << time7 - time1
//                          << "****\n";
                if(epoch % ctx.taasContext.print_mode_size == 0) {
                    LOG(INFO) << PrintfToString(
                            "************ 完成一个Epoch的合并 Physical Epoch %lu, Logical Epoch: %lu, Local EpochSuccessCommitTxnNum: %lu,TotalSuccessTxnNum: %lu, EpochCommitTxnNum: %lu ",
                            EpochManager::GetPhysicalEpoch(), epoch, epoch_commit_success_txn_num, total_commit_txn_num,
                            EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num)
                              << ",Time Cost  Epoch: " << epoch
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
}

