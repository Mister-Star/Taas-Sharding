//
// Created by 周慰星 on 2022/9/14.
//
#include "transaction/two_phase_commit.h"
#include "worker/worker_epoch_merge.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "message/message.h"
#include "transaction/transaction_cache.h"

namespace Taas {

    void WorkerFroMergeThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochMerge-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        while(init_ok_num.load() < 1) usleep(sleep_time);
        merger.MergeInit(id, ctx);
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

    void EpochWorkerThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochWorker-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        EpochMessageReceiveHandler receiveHandler;
        class TwoPC two_pc;
        while(init_ok_num.load() < 1) usleep(sleep_time);
        merger.MergeInit(id, ctx);
        receiveHandler.Init(id, ctx);
        Taas::TwoPC::Init(ctx, id);
        auto sleep_flag = true;
        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        switch(ctx.taasContext.taasMode) {
            case TaasMode::MultiModel :
            case TaasMode::MultiMaster :
            case TaasMode::Sharding : {
                while(!EpochManager::IsTimerStop()) {
                    sleep_flag = false;

                    merger.epoch = EpochManager::GetLogicalEpoch();
                    merger.epoch_mod = merger.epoch % ctx.taasContext.kCacheMaxLength;
                    while(TransactionCache::epoch_read_validate_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) { /// only local txn do this procedure
                        if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                            merger.ReadValidate();
                            merger.txn_ptr.reset();
                            sleep_flag = false;
                        }
                    }


                    if(!EpochManager::IsShardingMergeComplete(merger.epoch)) {
                        while (TransactionCache::epoch_merge_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
                            if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                                merger.Merge();
                                merger.txn_ptr.reset();
                                sleep_flag = false;
                            }
                        }
                    }

                    if(EpochManager::IsAbortSetMergeComplete(merger.epoch) && !EpochManager::IsCommitComplete(merger.epoch)) {
                        while (!EpochManager::IsCommitComplete(merger.epoch) && TransactionCache::epoch_commit_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
                            if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                                merger.Commit();
                                merger.txn_ptr.reset();
                                sleep_flag = false;
                            }
                        }
                    }

                    receiveHandler.TryHandleReceivedControlMessage();
                    receiveHandler.TryHandleReceivedMessage();

                    sleep_flag = sleep_flag & receiveHandler.sleep_flag;

                    if(sleep_flag) usleep(merge_sleep_time);
                }
                break;
            }
            case TaasMode::TwoPC : {
                two_pc.HandleClientMessage();
                break;
            }
        }
    }
}


