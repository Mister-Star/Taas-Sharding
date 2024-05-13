//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "epoch/epoch_manager_multi_master.h"
#include "epoch/two_phase_commit.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"

namespace Taas {

    void WorkerForPhysicalThreadMain(const Context &ctx) {
        std::string name = "EpochPhysical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        EpochPhysicalTimerManagerThreadMain(ctx);
   }

    void WorkerForLogicalThreadMain(const Context& ctx) {
        std::string name = "EpochLogical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ShardEpochManager::EpochLogicalTimerManagerThreadMain(ctx);
    }

    void WorkerForEpochControlMessageThreadMain(const Context& ctx) {
        SetCPU();
        while(!EpochManager::IsInitOK() || EpochManager::GetPhysicalEpoch() < 100) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taasContext.taasMode) {
                case TaasMode::MultiModel :
                case TaasMode::MultiMaster :
                case TaasMode::Shard : {
                    uint64_t local_server_id = ctx.taasContext.txn_node_ip_index;
                    uint64_t shard_epoch = 1, remote_server_epoch = 1, abort_send_epoch = 1, server_num = ctx.taasContext.kTxnNodeNum;
                    bool sleep_flag;
                    while(!EpochManager::IsInitOK()) usleep(sleep_time);
                    while(!EpochManager::IsTimerStop()) {
                        sleep_flag = true;
                        if (shard_epoch <= EpochManager::GetPhysicalEpoch() && EpochMessageReceiveHandler::CheckEpochClientTxnHandleComplete(shard_epoch)) {
                            EpochMessageSendHandler::SendEpochShardEndMessage(local_server_id, shard_epoch, server_num);
//                            LOG(INFO) << "Send EpochShardEndFlag epoch " << shard_epoch;
                            shard_epoch ++;
                            sleep_flag = false;
                        }

                        if(remote_server_epoch < shard_epoch &&
                            EpochMessageReceiveHandler::CheckEpochShardReceiveComplete(remote_server_epoch) &&
                            EpochMessageReceiveHandler::CheckEpochShardTxnHandleComplete(remote_server_epoch)) {
                            EpochMessageSendHandler::SendEpochRemoteServerEndMessage(local_server_id, remote_server_epoch, server_num);
//                            LOG(INFO) << "Send SendEpochRemoteServerEndMessage epoch " << remote_server_epoch;
                            remote_server_epoch ++;
                            sleep_flag = false;
                        }

//                        if(abort_send_epoch < remote_server_epoch && EpochManager::IsEpochMergeComplete(abort_send_epoch)) {
//                            EpochMessageSendHandler::SendAbortSet(local_server_id, abort_send_epoch, server_num);
////                            LOG(INFO) << "Send SendAbortSet epoch " << abort_send_epoch;
//                            abort_send_epoch ++;
//                            sleep_flag = false;
//                        }

//
//                        if(EpochManager::IsEpochMergeComplete(abort_send_epoch)) {
//                            EpochMessageSendHandler::SendAbortSet(local_server_id, abort_send_epoch, ctx.taasContext.kCacheMaxLength);
//                            abort_send_epoch ++;
//                            sleep_flag = false;
//                        }
                        if(sleep_flag) usleep(200);
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    //
                    break;
                }
            }
        }
    }

    void WorkerForLogicalRedoLogPushDownCheckThreadMain(const Context& ctx) {
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taasContext.taasMode) {
                case TaasMode::MultiModel :
                case TaasMode::MultiMaster :
                case TaasMode::Shard : {
                    CheckRedoLogPushDownState(ctx);
                    break;
                }
                case TaasMode::TwoPC : {
//                    TwoPhaseCommitManager::TwoPhaseCommitManagerThreadMain(ctx);
                }
            }
//            CheckRedoLogPushDownState(ctx);
        }
    }

}

