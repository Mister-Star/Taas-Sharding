//
// Created by zwx on 24-3-14.
//

#ifndef TAAS_THTEAD_LOCAL_COUNTERS_H
#define TAAS_THTEAD_LOCAL_COUNTERS_H

#pragma once

#include "context.h"
#include "atomic_counters.h"
#include "atomic_counters_cache.h"

namespace Taas {

    class ThreadLocalCounters{
    private:

        uint64_t thread_id = 0, max_length = 0, sharding_num = 0, local_server_id;
        static std::atomic<uint64_t> inc_id;

    public:
        static Context ctx;

    ///message handling
    public:
        std::shared_ptr<AtomicCounters_Cache>
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
    public:
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

        static std::vector<uint64_t>
                sharding_send_ack_epoch_num,
                backup_send_ack_epoch_num,
                backup_insert_set_send_ack_epoch_num,
                abort_set_send_ack_epoch_num; /// check and reply ack

        static std::vector<std::unique_ptr<std::atomic<bool>>>
                epoch_sharding_send_complete,
                epoch_sharding_receive_complete,
                epoch_back_up_complete,
                epoch_abort_set_merge_complete,
                epoch_insert_set_complete;

        static AtomicCounters_Cache
            sharding_should_receive_pack_num,
            sharding_received_pack_num,
            sharding_should_receive_txn_num,
            sharding_received_txn_num,
            sharding_received_ack_num,

            backup_should_send_txn_num,
            backup_send_txn_num,
            backup_should_receive_pack_num,
            backup_received_pack_num,
            backup_should_receive_txn_num,
            backup_received_txn_num,
            backup_received_ack_num,

            insert_set_should_receive_num,
            insert_set_received_num,
            insert_set_received_ack_num,

            abort_set_should_receive_num,
            abort_set_received_num,
            abort_set_received_ack_num,

            redo_log_push_down_ack_num,
            redo_log_push_down_local_epoch;

        static bool CheckEpochShardingSendComplete(const uint64_t& epoch) ;
        static bool CheckEpochShardingReceiveComplete(uint64_t& epoch) ;
        static bool CheckEpochBackUpComplete(uint64_t& epoch) ;
        static bool CheckEpochAbortSetMergeComplete(uint64_t& epoch) ;
        static bool CheckEpochInsertSetMergeComplete(uint64_t& epoch) ;

        static bool IsShardingSendFinish(const uint64_t &epoch, const uint64_t &sharding_id) ;
        static bool IsShardingSendFinish(const uint64_t &epoch) ;
        static bool IsBackUpSendFinish(const uint64_t &epoch) ;


        static bool IsEpochLocalTxnHandleComplete(const uint64_t &epoch) ;
        static bool IsEpochTxnHandleComplete(const uint64_t &epoch) ;
        static bool IsShardingTxnReceiveComplete(const uint64_t &epoch) ;
        static bool IsShardingTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsShardingPackReceiveComplete(const uint64_t &epoch) ;
        static bool IsShardingPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsBackUpTxnReceiveComplete(const uint64_t &epoch) ;
        static bool IsBackUpTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsBackUpPackReceiveComplete(const uint64_t &epoch) ;
        static bool IsBackUpPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;

        static bool IsAbortSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsAbortSetReceiveComplete(const uint64_t &epoch);
        static bool IsInsertSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsInsertSetReceiveComplete(const uint64_t &epoch) ;


        static bool IsShardingACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsBackUpACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsAbortSetACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsInsertSetACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsRedoLogPushDownACKReceiveComplete(const uint64_t &epoch) ;



    ///Merge
    public:
        std::shared_ptr<AtomicCounters_Cache>
                epoch_should_read_validate_txn_num_local,
                epoch_read_validated_txn_num_local,
                epoch_should_merge_txn_num_local,
                epoch_merged_txn_num_local,
                epoch_should_commit_txn_num_local,
                epoch_committed_txn_num_local,
                epoch_record_commit_txn_num_local,
                epoch_record_committed_txn_num_local;

        std::atomic<uint64_t>
                total_merge_txn_num_local,
                total_merge_latency_local,
                total_commit_txn_num_local,
                total_commit_latency_local,
                success_commit_txn_num_local,
                success_commit_latency_local,
                total_read_version_check_failed_txn_num_local,
                total_failed_txn_num_local;
    public:
        static std::vector<std::shared_ptr<AtomicCounters_Cache>>
                epoch_should_read_validate_txn_num_local_vec,
                epoch_read_validated_txn_num_local_vec,
                epoch_should_merge_txn_num_local_vec,
                epoch_merged_txn_num_local_vec,
                epoch_should_commit_txn_num_local_vec,
                epoch_committed_txn_num_local_vec,
                epoch_record_commit_txn_num_local_vec,
                epoch_record_committed_txn_num_local_vec;

        static std::vector<std::unique_ptr<std::atomic<bool>>>
                epoch_read_validate_complete,
                epoch_merge_complete,
                epoch_commit_complete;

        static std::atomic<uint64_t>
                total_merge_txn_num,
                total_merge_latency,
                total_commit_txn_num,
                total_commit_latency,
                success_commit_txn_num,
                success_commit_latency,
                total_read_version_check_failed_txn_num,
                total_failed_txn_num;


        static bool CheckEpochReadValidateComplete(const uint64_t& epoch);
        static bool CheckEpochMergeComplete(const uint64_t& epoch) ;
        static bool IsEpochMergeComplete(const uint64_t& epoch) ;
        static bool CheckEpochCommitComplete(const uint64_t& epoch) ;
        static bool IsEpochCommitComplete(const uint64_t& epoch) ;
        static bool IsReadValidateComplete(const uint64_t& epoch) ;
        static bool IsMergeComplete(const uint64_t& epoch) ;
        static bool IsCommitComplete(const uint64_t & epoch) ;
        static bool IsRedoLogComplete(const uint64_t & epoch) ;



    public:

        void Init(const uint64_t &id_, const Context& context);
        static bool StaticInit(const Context& context);
        static bool StaticClear(uint64_t& epoch);

        static void ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) ;
        static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) ;
        static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &sharding_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec);

    };

}

#endif //TAAS_THTEAD_LOCAL_COUNTERS_H
