//
// Created by 周慰星 on 11/15/22.
//

#ifndef TAAS_MERGE_H
#define TAAS_MERGE_H

#pragma once

#include "transaction/crdt_merge.h"
#include "message/epoch_message_send_handler.h"
#include "message/epoch_message_receive_handler.h"

#include "zmq.hpp"
#include "proto/message.pb.h"
#include "tools/thread_local_counters.h"

#include <cstdint>

namespace Taas {

    class Merger : public ThreadLocalCounters {

    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::shared_ptr<proto::Transaction> txn_ptr, write_set, backup_txn, full_txn;
        std::shared_ptr<std::vector<std::shared_ptr<proto::Transaction>>> sharding_row_vector;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, local_server_id,
                server_dequeue_id = 0, epoch_mod = 0, epoch = 0, max_length = 0, sharding_num = 0,///cache check
        message_epoch = 0, message_epoch_mod = 0, message_sharding_id = 0, message_server_id = 0, ///message epoch info
        server_reply_ack_id = 0,
                cache_clear_epoch_num = 0, cache_clear_epoch_num_mod = 0,
                redo_log_push_down_reply = 1, txn_server_id = 0;;

        bool res, sleep_flag;
        std::shared_ptr<proto::Transaction> empty_txn_ptr;
        std::hash<std::string> _hash;

//        CRDTMerge merger;
//        EpochMessageSendHandler message_transmitter;
//        EpochMessageReceiveHandler message_handler;

        std::shared_ptr<AtomicCounters_Cache>///epoch, server_id, value
            sharding_should_send_txn_num_local,
            sharding_send_txn_num_local,
            sharding_should_handle_local_txn_num_local,
            sharding_handled_local_txn_num_local,

            sharding_should_handle_remote_txn_num_local,
            sharding_handled_remote_txn_num_local,
            sharding_received_txn_num_local,

            backup_should_send_txn_num_local,
            backup_send_txn_num_local,
            backup_received_txn_num_local;

        std::shared_ptr<AtomicCounters_Cache>
            epoch_should_read_validate_txn_num_local, epoch_read_validated_txn_num_local,
            epoch_should_merge_txn_num_local, epoch_merged_txn_num_local,
            epoch_should_commit_txn_num_local, epoch_committed_txn_num_local,
            epoch_record_commit_txn_num_local, epoch_record_committed_txn_num_local;

        std::atomic<uint64_t> total_merge_txn_num_local, total_merge_latency_local,
            total_commit_txn_num_local, total_commit_latency_local, success_commit_txn_num_local,
            success_commit_latency_local, total_read_version_check_failed_txn_num_local,
            total_failed_txn_num_local;


        void Init(uint64_t id_);

        void ReadValidate();
        void Send();
        void Merge();
        void Commit();
        void EpochMerge();

        void ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction> &txn_ptr_);
        void MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void CommitQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);

    public:

        static void clearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
            for(const auto& i : vec) {
                if(i != nullptr)
                    i->Clear(epoch);
            }
        }

        static uint64_t getAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
            uint64_t ans = 0;
            for(const auto& i : vec) {
                if(i != nullptr)
                    ans += i->GetCount(epoch);
            }
            return ans;
        }
        static uint64_t getAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &sharding_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
            uint64_t ans = 0;
            for(const auto& i : vec) {
                if(i != nullptr)
                    ans += i->GetCount(epoch, sharding_id);
            }
            return ans;
        }



        static std::vector<std::shared_ptr<AtomicCounters_Cache>>
                sharding_should_send_txn_num_local_vec,
                sharding_send_txn_num_local_vec,
                sharding_should_handle_local_txn_num_local_vec,
                sharding_handled_local_txn_num_local_vec,

                sharding_should_handle_remote_txn_num_local_vec,
                sharding_handled_remote_txn_num_local_vec,
                sharding_received_txn_num_local_vec,

                backup_should_send_txn_num_local_vec,
                backup_send_txn_num_local_vec,
                backup_received_txn_num_local_vec;

        static std::vector<std::shared_ptr<AtomicCounters_Cache>>
                epoch_should_read_validate_txn_num_local_vec, epoch_read_validated_txn_num_local_vec,
                epoch_should_merge_txn_num_local_vec, epoch_merged_txn_num_local_vec,
                epoch_should_commit_txn_num_local_vec, epoch_committed_txn_num_local_vec,
                epoch_record_commit_txn_num_local_vec, epoch_record_committed_txn_num_local_vec;


        ///epoch
        static Context ctx;
        static AtomicCounters_Cache
                epoch_should_read_validate_txn_num, epoch_read_validated_txn_num,
                epoch_should_merge_txn_num, epoch_merged_txn_num,
                epoch_should_commit_txn_num, epoch_committed_txn_num,
                epoch_record_commit_txn_num, epoch_record_committed_txn_num;
        static std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
                epoch_merge_map,
                local_epoch_abort_txn_set,
                epoch_abort_txn_set;
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>>
                epoch_txn_map, epoch_write_set_map, epoch_back_txn_map;
        /// whole server state
        static concurrent_unordered_map<std::string, std::string> read_version_map_data, read_version_map_csn, insert_set;

        ///queues
        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
                epoch_read_validate_queue,///validate_queue 存放需要进行validate的事务 区分epoch
                epoch_merge_queue,///merge_queue 存放需要进行merge的事务 区分epoch
                epoch_commit_queue;///epoch_commit_queue 当前epoch的涉及当前分片的要进行commit validate和commit的子事务 receive from servers and local sharding txn, wait to validate

        static std::vector<std::unique_ptr<std::atomic<bool>>>
                epoch_read_validate_complete,
                epoch_merge_complete,
                epoch_commit_complete;

        static std::atomic<uint64_t> total_merge_txn_num, total_merge_latency, total_commit_txn_num, total_commit_latency, success_commit_txn_num, success_commit_latency,
            total_read_version_check_failed_txn_num, total_failed_txn_num;

        static std::condition_variable merge_cv, commit_cv;

        static void StaticInit(const Context& ctx_);
        static void ClearMergerEpochState(uint64_t &epoch);

        [[nodiscard]] uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % sharding_num;
        }

        static bool MergeQueueTryDequeue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        static bool CommitQueueTryDequeue(uint64_t &epoch_, std::shared_ptr<proto::Transaction> txn_ptr_);

        static bool CheckEpochReadValidateComplete(const uint64_t& epoch);
        static bool CheckEpochMergeComplete(const uint64_t& epoch);
        static bool IsEpochMergeComplete(const uint64_t& epoch);
        static bool CheckEpochCommitComplete(const uint64_t& epoch);
        static bool IsEpochCommitComplete(const uint64_t& epoch);





        static bool IsReadValidateComplete(const uint64_t& epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
                if(getAllThreadLocalCountNum(epoch,i, epoch_should_read_validate_txn_num_local_vec) >
                        getAllThreadLocalCountNum(epoch,i, epoch_read_validated_txn_num_local_vec))
                    return false;
//                if (epoch_should_read_validate_txn_num.GetCount(epoch, i) > epoch_read_validated_txn_num.GetCount(epoch, i))
//                    return false;
            }
            return true;
        }
        static bool IsMergeComplete(const uint64_t& epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
//                if (epoch_should_read_validate_txn_num.GetCount(epoch, i) > epoch_read_validated_txn_num.GetCount(epoch, i))
//                    return false;
//                if (epoch_should_merge_txn_num.GetCount(epoch, i) > epoch_merged_txn_num.GetCount(epoch, i))
//                    return false;
                if(getAllThreadLocalCountNum(epoch,i, epoch_should_read_validate_txn_num_local_vec) >
                   getAllThreadLocalCountNum(epoch,i, epoch_read_validated_txn_num_local_vec))
                    return false;
                if(getAllThreadLocalCountNum(epoch,i, epoch_should_merge_txn_num_local_vec) >
                   getAllThreadLocalCountNum(epoch,i, epoch_merged_txn_num_local_vec))
                    return false;
            }
            return true;
        }
        static bool IsCommitComplete(const uint64_t & epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
                if(getAllThreadLocalCountNum(epoch,i, epoch_should_commit_txn_num_local_vec) >
                   getAllThreadLocalCountNum(epoch,i, epoch_committed_txn_num_local_vec))
                    return false;
//                if (epoch_should_commit_txn_num.GetCount(epoch, i) > epoch_committed_txn_num.GetCount(epoch, i))
//                    return false;
            }
            return true;
        }
        static bool IsRedoLogComplete(const uint64_t & epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
                if(getAllThreadLocalCountNum(epoch,i, epoch_record_commit_txn_num_local_vec) >
                   getAllThreadLocalCountNum(epoch,i, epoch_record_committed_txn_num_local_vec))
                    return false;
//                if (epoch_record_commit_txn_num.GetCount(epoch, i) > epoch_record_committed_txn_num.GetCount(epoch, i))
//                    return false;
            }
            return true;
        }

        static bool IsShardingSendFinish(const uint64_t &epoch, const uint64_t &sharding_id) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                   getAllThreadLocalCountNum(epoch, sharding_id, sharding_send_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_id, sharding_should_send_txn_num_local_vec) &&
                   getAllThreadLocalCountNum(epoch, sharding_id, sharding_handled_local_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_id, sharding_should_handle_local_txn_num_local_vec);
        }
        static bool IsShardingSendFinish(const uint64_t &epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                   getAllThreadLocalCountNum(epoch, sharding_send_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_should_send_txn_num_local_vec) &&
                   getAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec);
        }
        static bool IsBackUpSendFinish(const uint64_t &epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                   getAllThreadLocalCountNum(epoch, backup_send_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, backup_should_send_txn_num_local_vec) &&
                   getAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec);
        }


        static bool IsEpochLocalTxnHandleComplete(const uint64_t &epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                   getAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec);
        }
        static bool IsEpochTxnHandleComplete(const uint64_t &epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                   getAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec) &&
                   getAllThreadLocalCountNum(epoch, sharding_handled_remote_txn_num_local_vec) >=
                   getAllThreadLocalCountNum(epoch, sharding_should_handle_remote_txn_num_local_vec);
        }

    };
}

#endif //TAAS_MERGE_H
