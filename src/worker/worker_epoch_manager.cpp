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

