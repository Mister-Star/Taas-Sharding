//
// Created by 周慰星 on 2022/9/14.
//
#include "transaction/two_phase_commit.h"
#include "worker/worker_epoch_merge.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "transaction/transaction_cache.h"

namespace Taas {

    void WorkerFroMergeThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochMerge-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        while(init_ok_num.load() < 5) usleep(sleep_time);
        merger.MergeInit(id, ctx);
        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        auto txn_ptr = std::make_shared<proto::Transaction>();
        switch(ctx.taasContext.taasMode) {
            case TaasMode::MultiModel :
            case TaasMode::MultiMaster :
            case TaasMode::Shard : {
//                while(!EpochManager::IsTimerStop()) {
//                    merger.EpochMerge();
//                }
                break;
            }
            case TaasMode::TwoPC : {
                break;
            }
        }
    }

    void EpochWorkerThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "TaaSMerger-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        EpochMessageReceiveHandler receiveHandler;
        class TwoPC two_pc;
        while(init_ok_num.load() < 5) usleep(sleep_time);
//        LOG(INFO) << "start worker init" << id;
        merger.MergeInit(id, ctx);
        receiveHandler.Init(id, ctx);
        Taas::TwoPC::Init(ctx, id);
        bool sleep_flag;
        auto safe_length = 500 * 1000/ ctx.taasContext.kEpochSize_us;
        if(safe_length < 10) safe_length = 10;
        init_ok_num.fetch_add(1);
//        LOG(INFO) << "finish worker init" << id;
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        switch(ctx.taasContext.taasMode) {
            case TaasMode::MultiModel :
            case TaasMode::MultiMaster :
            case TaasMode::Shard : {
//                if(id < (ctx.taasContext.kMergeThreadNum * 3) / 4) {
//                    while(!EpochManager::IsTimerStop()) {
//                        merger.epoch = EpochManager::GetLogicalEpoch();
//                        merger.epoch_mod = merger.epoch % ctx.taasContext.kCacheMaxLength;
//                        while(TransactionCache::epoch_read_validate_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
//                            if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
//                                merger.ReadValidate();
//                            }
//                        }
//                        std::this_thread::yield();
//
//                        if(!EpochManager::IsEpochMergeComplete(merger.epoch)) {
//                            while (TransactionCache::epoch_merge_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
//                                if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
//                                    merger.Merge();
//                                }
//                            }
//                        }
//                        else {
//                            std::this_thread::yield();
//                        }
//
//                        if(EpochManager::IsAbortSetMergeComplete(merger.epoch) && !EpochManager::IsCommitComplete(merger.epoch)) {
//                            while (!EpochManager::IsCommitComplete(merger.epoch) && TransactionCache::epoch_commit_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
//                                if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
//                                    merger.Commit();
//                                }
//                            }
//                        }
//                        else {
//                            std::this_thread::yield();
//                        }
//
//                        if(EpochManager::IsAbortSetMergeComplete(merger.epoch) && !EpochManager::IsRecordCommitted(merger.epoch)) {
//                            while (!EpochManager::IsRecordCommitted(merger.epoch) && TransactionCache::epoch_redo_log_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
//                                if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
//                                    merger.RedoLog();
//                                }
//                            }
//                        }
//                        else {
//                            std::this_thread::yield();
//                        }
//
//                        receiveHandler.TryHandleReceivedControlMessage();
//                        if( EpochManager::GetLogicalEpoch() + safe_length > EpochManager::GetPhysicalEpoch() )
//                            receiveHandler.TryHandleReceivedMessage();
//                    }
//                }
//                else {
                    while(!EpochManager::IsTimerStop()) {
                        sleep_flag = true;

                        merger.epoch = EpochManager::GetLogicalEpoch();
                        merger.epoch_mod = merger.epoch % ctx.taasContext.kCacheMaxLength;
                        while (TransactionCache::epoch_read_validate_queue[merger.epoch_mod]->try_dequeue(
                                merger.txn_ptr)) {
                            if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                                merger.ReadValidate();
                                merger.txn_ptr.reset();
                                sleep_flag = false;
                            }
                        }

                        if (!EpochManager::IsEpochMergeComplete(merger.epoch)) {
                            while (TransactionCache::epoch_merge_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
                                if (merger.txn_ptr != nullptr &&
                                    merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                                    merger.Merge();
                                    merger.txn_ptr.reset();
                                    sleep_flag = false;
                                }
                            }
                        }

                        if (EpochManager::IsAbortSetMergeComplete(merger.epoch) &&
                            !EpochManager::IsCommitComplete(merger.epoch)) {
                            while (!EpochManager::IsCommitComplete(merger.epoch) &&
                                   TransactionCache::epoch_commit_queue[merger.epoch_mod]->try_dequeue(
                                           merger.txn_ptr)) {
                                if (merger.txn_ptr != nullptr &&
                                    merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                                    merger.Commit();
                                    merger.txn_ptr.reset();
                                    sleep_flag = false;
                                }
                            }
                        }

                        if (EpochManager::IsAbortSetMergeComplete(merger.epoch) &&
                            !EpochManager::IsRecordCommitted(merger.epoch)) {
                            while (!EpochManager::IsRecordCommitted(merger.epoch) &&
                                   TransactionCache::epoch_redo_log_queue[merger.epoch_mod]->try_dequeue(
                                           merger.txn_ptr)) {
                                if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() !=
                                                                 proto::TxnType::NullMark) { /// only local txn do redo log
                                    merger.RedoLog();
                                    merger.txn_ptr.reset();
                                    sleep_flag = false;
                                }
                            }
                        }

                        receiveHandler.TryHandleReceivedControlMessage();
                        if (EpochManager::GetLogicalEpoch() + safe_length >
                            EpochManager::GetPhysicalEpoch()) /// avoid task backlogs, stop handling txn comes from the client
                            receiveHandler.TryHandleReceivedMessage();

                        sleep_flag = sleep_flag & receiveHandler.sleep_flag;

                        if (sleep_flag) std::this_thread::yield();
                    }
//                }
                break;
            }
            case TaasMode::TwoPC : {
                two_pc.HandleClientMessage();
                break;
            }
        }
    }
}


