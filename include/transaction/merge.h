//
// Created by 周慰星 on 11/15/22.
//

#ifndef TAAS_MERGE_H
#define TAAS_MERGE_H

#pragma once

#include "transaction/crdt_merge.h"
#include "message/handler_send.h"
#include "message/handler_receive.h"

#include "zmq.hpp"
#include "proto/message.pb.h"

#include <cstdint>

namespace Taas {

    class Merger {

    public:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, epoch = 0, epoch_mod = 0, txn_server_id = 0;
        bool res, sleep_flag;
        Context ctx;
        CRDTMerge merger;
        MessageSendHandler message_transmitter;
        MessageReceiveHandler message_handler;

        ///epoch
        static AtomicCounters_Cache
                epoch_should_merge_txn_num, epoch_merged_txn_num,
                epoch_should_commit_txn_num, epoch_committed_txn_num,
                epoch_record_commit_txn_num, epoch_record_committed_txn_num;
        static std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
                epoch_merge_map,
                local_epoch_abort_txn_set,
                epoch_abort_txn_set;
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::string>>> epoch_insert_set;

        /// whole server state
        static concurrent_unordered_map<std::string, std::string> read_version_map_data, read_version_map_csn, insert_set, abort_txn_set;

        ///queues
        static std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> task_queue;
        static std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> merge_queue;///merge_queue 存放需要进行merge的子事务 不区分epoch
        static std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> commit_queue;
        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
                epoch_merge_queue,///merge_queue 存放需要进行merge的子事务 不区分epoch
                epoch_local_txn_queue, ///epoch_local_txn_queue 本地接收到的完整的事务  txn receive from client (used to push log down to storage)
                epoch_commit_queue;///epoch_commit_queue 当前epoch的涉及当前分片的要进行validate和commit的子事务 receive from servers and local sharding txn, wait to validate

        static std::vector<std::unique_ptr<std::atomic<bool>>>
                epoch_merge_complete,
                epoch_commit_complete;


        static void StaticInit(const Context& ctx);
        static void ClearMergerEpochState(const Context& ctx, uint64_t &epoch);

        void Init(const Context& ctx_, uint64_t id);

        static bool EpochMerge(const Context& ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr);
        void EpochMerge_Usleep();
        void EpochMerge_Block();
        void EpochCommit_Usleep();
        void EpochCommit_Block();

        static void MergeQueueEnqueue(const Context& ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr);
        static bool MergeQueueTryDequeue(const Context& ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr);
        static void CommitQueueEnqueue(const Context& ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr);
        static bool CommitQueueTryDequeue(const Context& ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr);
        static void LocalTxnCommitQueueEnqueue(const Context& ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr);
        static bool LocalTxnCommitQueueTryDequeue(const Context& ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr);




        static bool CheckEpochMergeComplete(const Context &ctx, uint64_t& epoch) {
            if(epoch_merge_complete[epoch % ctx.kCacheMaxLength]->load()) {
                return true;
            }
            if (epoch < EpochManager::GetPhysicalEpoch() && IsMergeComplete(ctx, epoch)) {
                epoch_merge_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }
        static bool IsEpochMergeComplete(const Context &ctx, uint64_t& epoch) {
            return epoch_merge_complete[epoch % ctx.kCacheMaxLength]->load();
        }

        static bool CheckEpochCommitComplete(const Context &ctx, uint64_t& epoch) {
            if (epoch_commit_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
            if (epoch < EpochManager::GetPhysicalEpoch() && IsCommitComplete(ctx, epoch)) {
                epoch_commit_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }
        static bool IsEpochCommitComplete(const Context &ctx, uint64_t& epoch) {
            return epoch_commit_complete[epoch % ctx.kCacheMaxLength]->load();
        }



        static bool IsMergeComplete(const Context& ctx, uint64_t& epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i++) {
                if (epoch_should_merge_txn_num.GetCount(epoch, i) > epoch_merged_txn_num.GetCount(epoch, i))
                    return false;
            }
            return true;
        }
        static bool IsMergeComplete(uint64_t epoch, uint64_t server_id) {
            return epoch_should_merge_txn_num.GetCount(epoch, server_id) <= epoch_merged_txn_num.GetCount(epoch, server_id);
        }
        static bool IsCommitComplete(const Context& ctx, uint64_t& epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i++) {
                if (epoch_should_commit_txn_num.GetCount(epoch, i) > epoch_committed_txn_num.GetCount(epoch, i))
                    return false;
            }
            return true;
        }
        static bool IsCommitComplete(uint64_t epoch, uint64_t server_id) {
            return epoch_should_commit_txn_num.GetCount(epoch, server_id) <= epoch_committed_txn_num.GetCount(epoch, server_id);
        }

        static bool IsRedoLogComplete(const Context& ctx, uint64_t& epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i++) {
                if (epoch_record_commit_txn_num.GetCount(epoch, i) > epoch_record_committed_txn_num.GetCount(epoch, i))
                    return false;
            }
            return true;
        }
        static bool IsRedoLogComplete(uint64_t epoch, uint64_t server_id) {
            return epoch_record_commit_txn_num.GetCount(epoch, server_id) <= epoch_record_committed_txn_num.GetCount(epoch, server_id);
        }


    };
}

#endif //TAAS_MERGE_H
