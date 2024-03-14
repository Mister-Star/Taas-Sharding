//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_merge.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "storage/mot.h"

namespace Taas {

    void WorkerFroMergeThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochMerge-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        while(init_ok_num.load() < 1) usleep(sleep_time);
        merger.Init(id);
        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        auto txn_ptr = std::make_shared<proto::Transaction>();
        switch(ctx.taasContext.taasMode) {
            case TaasMode::MultiModel :
            case TaasMode::MultiMaster :
            case TaasMode::Sharding : {
                while(!EpochManager::IsTimerStop()) {
                    merger.EpochMerge();
                }
                break;
            }
            case TaasMode::TwoPC : {
                break;
            }
        }
    }


}

