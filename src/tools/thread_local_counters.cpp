//
// Created by user on 24-3-14.
//
#include "tools/thread_local_counters.h"
#include "epoch/epoch_manager.h"

namespace Taas{

    std::atomic<uint64_t> ThreadLocalCounters::inc_id(0);
    Context ThreadLocalCounters::ctx;

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            ThreadLocalCounters::sharding_should_send_txn_num_local_vec,
            ThreadLocalCounters::sharding_send_txn_num_local_vec,
            ThreadLocalCounters::sharding_should_handle_local_txn_num_local_vec,
            ThreadLocalCounters::sharding_handled_local_txn_num_local_vec,

            ThreadLocalCounters::sharding_should_handle_remote_txn_num_local_vec,
            ThreadLocalCounters::sharding_handled_remote_txn_num_local_vec,
            ThreadLocalCounters::sharding_received_txn_num_local_vec,

            ThreadLocalCounters::backup_should_send_txn_num_local_vec,
            ThreadLocalCounters::backup_send_txn_num_local_vec,
            ThreadLocalCounters::backup_received_txn_num_local_vec;

    std::vector<uint64_t>
            ThreadLocalCounters::sharding_send_ack_epoch_num,
            ThreadLocalCounters::backup_send_ack_epoch_num,
            ThreadLocalCounters::backup_insert_set_send_ack_epoch_num,
            ThreadLocalCounters::abort_set_send_ack_epoch_num;

    std::vector<std::unique_ptr<std::atomic<bool>>>
            ThreadLocalCounters::epoch_sharding_send_complete,
            ThreadLocalCounters::epoch_sharding_receive_complete,
            ThreadLocalCounters::epoch_back_up_complete,
            ThreadLocalCounters::epoch_abort_set_merge_complete,
            ThreadLocalCounters::epoch_insert_set_complete;

    AtomicCounters_Cache
            ThreadLocalCounters::sharding_should_receive_pack_num(10, 1),
            ThreadLocalCounters::sharding_received_pack_num(10, 1),
            ThreadLocalCounters::sharding_should_receive_txn_num(10, 1),
            ThreadLocalCounters::sharding_received_txn_num(10, 1),
            ThreadLocalCounters::sharding_received_ack_num(10, 1),

            ThreadLocalCounters::backup_should_send_txn_num(10, 1),
            ThreadLocalCounters::backup_send_txn_num(10, 1),
            ThreadLocalCounters::backup_should_receive_pack_num(10, 1),
            ThreadLocalCounters::backup_received_pack_num(10, 1),
            ThreadLocalCounters::backup_should_receive_txn_num(10, 1),
            ThreadLocalCounters::backup_received_txn_num(10, 1),

            ThreadLocalCounters::backup_received_ack_num(10, 1),

            ThreadLocalCounters::insert_set_should_receive_num(10, 1),
            ThreadLocalCounters::insert_set_received_num(10, 1),
            ThreadLocalCounters::insert_set_received_ack_num(10, 1),

            ThreadLocalCounters::abort_set_should_receive_num(10, 1),
            ThreadLocalCounters::abort_set_received_num(10, 1),
            ThreadLocalCounters::abort_set_received_ack_num(10, 1),

            ThreadLocalCounters::redo_log_push_down_ack_num(10, 1),
            ThreadLocalCounters::redo_log_push_down_local_epoch(10, 1);






    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            ThreadLocalCounters::epoch_should_read_validate_txn_num_local_vec,
            ThreadLocalCounters::epoch_read_validated_txn_num_local_vec,
            ThreadLocalCounters::epoch_should_merge_txn_num_local_vec,
            ThreadLocalCounters::epoch_merged_txn_num_local_vec,
            ThreadLocalCounters::epoch_should_commit_txn_num_local_vec,
            ThreadLocalCounters::epoch_committed_txn_num_local_vec,
            ThreadLocalCounters::epoch_record_commit_txn_num_local_vec,
            ThreadLocalCounters::epoch_record_committed_txn_num_local_vec;

    std::vector<std::unique_ptr<std::atomic<bool>>>
            ThreadLocalCounters::epoch_read_validate_complete,
            ThreadLocalCounters::epoch_merge_complete,
            ThreadLocalCounters::epoch_commit_complete;

    std::atomic<uint64_t>
            ThreadLocalCounters::total_merge_txn_num(0),
            ThreadLocalCounters::total_merge_latency(0),
            ThreadLocalCounters::total_commit_txn_num(0),
            ThreadLocalCounters::total_commit_latency(0),
            ThreadLocalCounters::success_commit_txn_num(0),
            ThreadLocalCounters::success_commit_latency(0),
            ThreadLocalCounters::total_read_version_check_failed_txn_num(0),
            ThreadLocalCounters::total_failed_txn_num(0);







    void ThreadLocalCounters::ThreadLocalCountersInit(const Context& context) {
        thread_id = inc_id.fetch_add(1);
        sharding_num = context.taasContext.kTxnNodeNum;
        max_length = context.taasContext.kCacheMaxLength;
        local_server_id = context.taasContext.txn_node_ip_index;

        sharding_should_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        sharding_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num);
        sharding_should_handle_local_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        sharding_handled_local_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        sharding_should_handle_remote_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        sharding_handled_remote_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        sharding_received_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        backup_should_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        backup_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        backup_received_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num);

        sharding_should_send_txn_num_local_vec[thread_id] = sharding_should_send_txn_num_local;
        sharding_send_txn_num_local_vec[thread_id] = sharding_send_txn_num_local;
        sharding_should_handle_local_txn_num_local_vec[thread_id] = sharding_should_handle_local_txn_num_local;
        sharding_handled_local_txn_num_local_vec[thread_id] = sharding_handled_local_txn_num_local;
        sharding_should_handle_remote_txn_num_local_vec[thread_id] = sharding_should_handle_remote_txn_num_local;
        sharding_handled_remote_txn_num_local_vec[thread_id] = sharding_handled_remote_txn_num_local;
        sharding_received_txn_num_local_vec[thread_id] = sharding_received_txn_num_local;
        backup_should_send_txn_num_local_vec[thread_id] = backup_should_send_txn_num_local;
        backup_send_txn_num_local_vec[thread_id] = backup_send_txn_num_local;
        backup_received_txn_num_local_vec[thread_id] = backup_received_txn_num_local;


        epoch_should_read_validate_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        epoch_read_validated_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num);
        epoch_should_merge_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        epoch_merged_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        epoch_should_commit_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        epoch_committed_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        epoch_record_commit_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num),
        epoch_record_committed_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, sharding_num);

        epoch_should_read_validate_txn_num_local_vec[thread_id] = epoch_should_read_validate_txn_num_local;
        epoch_read_validated_txn_num_local_vec[thread_id] = epoch_read_validated_txn_num_local;
        epoch_should_merge_txn_num_local_vec[thread_id] = epoch_should_merge_txn_num_local;
        epoch_merged_txn_num_local_vec[thread_id] = epoch_merged_txn_num_local;
        epoch_should_commit_txn_num_local_vec[thread_id] = epoch_should_commit_txn_num_local;
        epoch_committed_txn_num_local_vec[thread_id] = epoch_committed_txn_num_local;
        epoch_record_commit_txn_num_local_vec[thread_id] = epoch_record_commit_txn_num_local;
        epoch_record_committed_txn_num_local_vec[thread_id] = epoch_record_committed_txn_num_local;

    }

    bool ThreadLocalCounters::StaticInit(const Context& context) {
        ctx = context;
        auto thread_total_num = ctx.taasContext.kMergeThreadNum + ctx.taasContext.kEpochMessageThreadNum + ctx.taasContext.kEpochTxnThreadNum;
        auto max_length = context.taasContext.kCacheMaxLength;
        auto sharding_num = context.taasContext.kTxnNodeNum;

        sharding_should_send_txn_num_local_vec.resize(thread_total_num);
        sharding_send_txn_num_local_vec.resize(thread_total_num);
        sharding_should_handle_local_txn_num_local_vec.resize(thread_total_num);
        sharding_handled_local_txn_num_local_vec.resize(thread_total_num);
        sharding_should_handle_remote_txn_num_local_vec.resize(thread_total_num);
        sharding_handled_remote_txn_num_local_vec.resize(thread_total_num);
        sharding_received_txn_num_local_vec.resize(thread_total_num);
        backup_should_send_txn_num_local_vec.resize(thread_total_num);
        backup_send_txn_num_local_vec.resize(thread_total_num);
        backup_received_txn_num_local_vec.resize(thread_total_num);

        sharding_send_ack_epoch_num.resize(sharding_num + 1);
        backup_send_ack_epoch_num.resize(sharding_num + 1);
        backup_insert_set_send_ack_epoch_num.resize(sharding_num + 1);
        abort_set_send_ack_epoch_num.resize(sharding_num + 1);
        for(int i = 0; i <= (int) sharding_num; i ++ ) { /// start at 1, not 0
            sharding_send_ack_epoch_num[i] = 1;
            backup_send_ack_epoch_num[i] = 1;
            backup_insert_set_send_ack_epoch_num[i] = 1;
            abort_set_send_ack_epoch_num[i] = 1;
        }

        epoch_sharding_send_complete.resize(max_length);
        epoch_sharding_receive_complete.resize(max_length);
        epoch_back_up_complete.resize(max_length);
        epoch_abort_set_merge_complete.resize(max_length);
        epoch_insert_set_complete.resize(max_length);
        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            epoch_sharding_send_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_sharding_receive_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_back_up_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_abort_set_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_insert_set_complete[i] = std::make_unique<std::atomic<bool>>(false);
        }

        sharding_should_receive_pack_num.Init(max_length, sharding_num, 1),
        sharding_received_pack_num.Init(max_length, sharding_num),
        sharding_should_receive_txn_num.Init(max_length, sharding_num, 0),
        sharding_received_txn_num.Init(max_length, sharding_num),
        sharding_received_ack_num.Init(max_length, sharding_num),

        backup_should_send_txn_num.Init(max_length, sharding_num),
        backup_send_txn_num.Init(max_length, sharding_num),
        backup_should_receive_pack_num.Init(max_length, sharding_num, 1),
        backup_received_pack_num.Init(max_length, sharding_num),
        backup_should_receive_txn_num.Init(max_length, sharding_num, 0),
        backup_received_txn_num.Init(max_length, sharding_num),
        backup_received_ack_num.Init(max_length, sharding_num),

        insert_set_should_receive_num.Init(max_length, sharding_num, 1),
        insert_set_received_num.Init(max_length, sharding_num),
        insert_set_received_ack_num.Init(max_length, sharding_num),

        abort_set_should_receive_num.Init(max_length, sharding_num, 1),
        abort_set_received_num.Init(max_length, sharding_num);
        abort_set_received_ack_num.Init(max_length, sharding_num);

        redo_log_push_down_ack_num.Init(max_length, sharding_num);
        redo_log_push_down_local_epoch.Init(max_length, sharding_num);





        ///Merge
        epoch_should_read_validate_txn_num_local_vec.resize(thread_total_num);
        epoch_read_validated_txn_num_local_vec.resize(thread_total_num);
        epoch_should_merge_txn_num_local_vec.resize(thread_total_num);
        epoch_merged_txn_num_local_vec.resize(thread_total_num);
        epoch_should_commit_txn_num_local_vec.resize(thread_total_num);
        epoch_committed_txn_num_local_vec.resize(thread_total_num);
        epoch_record_commit_txn_num_local_vec.resize(thread_total_num);
        epoch_record_committed_txn_num_local_vec.resize(thread_total_num);

        ///epoch merge state
        epoch_read_validate_complete.resize(ctx.taasContext.kCacheMaxLength);
        epoch_merge_complete.resize(ctx.taasContext.kCacheMaxLength);
        epoch_commit_complete.resize(ctx.taasContext.kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(ctx.taasContext.kCacheMaxLength); i ++) {
            epoch_read_validate_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_commit_complete[i] = std::make_unique<std::atomic<bool>>(false);
        }
        return true;
    }

    bool ThreadLocalCounters::StaticClear(uint64_t& epoch) {
        auto epoch_mod_temp = epoch % ctx.taasContext.kCacheMaxLength;
        auto cache_clear_epoch_num_mod = epoch % ctx.taasContext.kCacheMaxLength;

        ///Message handle
        sharding_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        sharding_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_received_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_should_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        backup_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_received_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),
        insert_set_should_receive_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        insert_set_received_num.Clear(cache_clear_epoch_num_mod, 0),
        insert_set_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),
        abort_set_should_receive_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        abort_set_received_num.Clear(cache_clear_epoch_num_mod, 0);
        abort_set_received_ack_num.Clear(cache_clear_epoch_num_mod, 0);
        redo_log_push_down_ack_num.Clear(cache_clear_epoch_num_mod, 0);
        redo_log_push_down_local_epoch.Clear(cache_clear_epoch_num_mod, 0);
        epoch_sharding_send_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_sharding_receive_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_back_up_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_abort_set_merge_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_insert_set_complete[cache_clear_epoch_num_mod]->store(false);


        ///Merge
        epoch_read_validate_complete[epoch_mod_temp]->store(false);
        epoch_merge_complete[epoch_mod_temp]->store(false);
        epoch_commit_complete[epoch_mod_temp]->store(false);

        ClearAllThreadLocalCountNum(epoch, sharding_should_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, sharding_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec);

        ClearAllThreadLocalCountNum(epoch, sharding_should_handle_remote_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, sharding_handled_remote_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, sharding_received_txn_num_local_vec);

        ClearAllThreadLocalCountNum(epoch, backup_should_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, backup_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, backup_received_txn_num_local_vec);

        ClearAllThreadLocalCountNum(epoch, epoch_should_read_validate_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_read_validated_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_should_merge_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_merged_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_should_commit_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_committed_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_record_commit_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_record_committed_txn_num_local_vec);

        return true;
    }




    bool ThreadLocalCounters::CheckEpochShardingSendComplete(const uint64_t& epoch) {
        if(epoch_sharding_send_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) {
            return true;
        }
        if (epoch < EpochManager::GetPhysicalEpoch() &&
            IsShardingACKReceiveComplete(epoch) &&
            IsShardingSendFinish(epoch)
                ) {
            epoch_sharding_send_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadLocalCounters::CheckEpochShardingReceiveComplete(uint64_t& epoch) {
        if (epoch_sharding_receive_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() &&
            IsShardingPackReceiveComplete(epoch) &&
            IsShardingTxnReceiveComplete(epoch)) {
            epoch_sharding_receive_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadLocalCounters::CheckEpochBackUpComplete(uint64_t& epoch) {
        if (epoch_back_up_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetPhysicalEpoch() && IsBackUpACKReceiveComplete(epoch)
           &&IsBackUpSendFinish(epoch)) {
            epoch_back_up_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadLocalCounters::CheckEpochAbortSetMergeComplete(uint64_t& epoch) {
        if(epoch_abort_set_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetPhysicalEpoch() &&
           IsAbortSetACKReceiveComplete(epoch) &&
           IsAbortSetReceiveComplete(epoch)
                ) {
            epoch_abort_set_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadLocalCounters::CheckEpochInsertSetMergeComplete(uint64_t& epoch) {
        if(epoch_insert_set_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetPhysicalEpoch() &&
           IsInsertSetACKReceiveComplete(epoch) &&
           IsInsertSetReceiveComplete(epoch)
                ) {
            epoch_insert_set_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }

    bool ThreadLocalCounters::IsShardingSendFinish(const uint64_t &epoch, const uint64_t &sharding_id) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, sharding_id, sharding_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_id, sharding_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, sharding_id, sharding_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_id, sharding_should_handle_local_txn_num_local_vec)
                ;
    }
    bool ThreadLocalCounters::IsShardingSendFinish(const uint64_t &epoch) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, sharding_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec)
                ;
    }
    bool ThreadLocalCounters::IsBackUpSendFinish(const uint64_t &epoch) {
        return epoch < EpochManager::GetPhysicalEpoch() &&
               GetAllThreadLocalCountNum(epoch, backup_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, backup_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec)
                ;
    }


    bool ThreadLocalCounters::IsEpochLocalTxnHandleComplete(const uint64_t &epoch) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec);
    }
    bool ThreadLocalCounters::IsEpochTxnHandleComplete(const uint64_t &epoch) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, sharding_handled_remote_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, sharding_should_handle_remote_txn_num_local_vec);
    }
    bool ThreadLocalCounters::IsShardingTxnReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(GetAllThreadLocalCountNum(epoch, i, sharding_received_txn_num_local_vec) < sharding_should_receive_txn_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsShardingTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return GetAllThreadLocalCountNum(epoch, id, sharding_received_txn_num_local_vec) >= sharding_should_receive_txn_num.GetCount(epoch, id);
    }
    bool ThreadLocalCounters::IsShardingPackReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(sharding_received_pack_num.GetCount(epoch, i) < sharding_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsShardingPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return sharding_received_pack_num.GetCount(epoch, id) >= sharding_should_receive_pack_num.GetCount(epoch, id);
    }
    bool ThreadLocalCounters::IsBackUpTxnReceiveComplete(const uint64_t &epoch) {
        uint64_t to_id;
        for(uint64_t i = 0; i < ctx.taasContext.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
            to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
            if(to_id == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
            if(GetAllThreadLocalCountNum(epoch, to_id, backup_received_txn_num_local_vec) < backup_should_receive_txn_num.GetCount(epoch, to_id)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsBackUpTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return GetAllThreadLocalCountNum(epoch, backup_received_txn_num_local_vec) >= backup_should_receive_txn_num.GetCount(epoch, id);
    }
    bool ThreadLocalCounters::IsBackUpPackReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(backup_received_pack_num.GetCount(epoch, i) < backup_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsBackUpPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return backup_received_pack_num.GetCount(epoch, id) >= backup_should_receive_pack_num.GetCount(epoch, id);
    }

    bool ThreadLocalCounters::IsAbortSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return abort_set_received_num.GetCount(epoch, id) >= abort_set_should_receive_num.GetCount(epoch, id);
    }
    bool ThreadLocalCounters::IsAbortSetReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(abort_set_received_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsInsertSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return insert_set_received_num.GetCount(epoch, id) >= insert_set_should_receive_num.GetCount(epoch, id);
    }
    bool ThreadLocalCounters::IsInsertSetReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(insert_set_received_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }


    bool ThreadLocalCounters::IsShardingACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(sharding_received_ack_num.GetCount(epoch, i) < sharding_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsBackUpACKReceiveComplete(const uint64_t &epoch) {
        uint64_t to_id ;
        for(uint64_t i = 0; i < ctx.taasContext.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
            to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
            if(to_id == (uint64_t)ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
            if(backup_received_ack_num.GetCount(epoch, to_id) < backup_should_receive_pack_num.GetCount(epoch, to_id)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsAbortSetACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(abort_set_received_ack_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsInsertSetACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(insert_set_received_ack_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsRedoLogPushDownACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(redo_log_push_down_ack_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(epoch, i)) return false;
        }
        return true;
    }













    bool ThreadLocalCounters::CheckEpochReadValidateComplete(const uint64_t& epoch) {
        if(epoch_read_validate_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) {
            return true;
        }
        if (epoch < EpochManager::GetPhysicalEpoch() && IsReadValidateComplete(epoch)) {
            epoch_read_validate_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadLocalCounters::CheckEpochMergeComplete(const uint64_t& epoch) {
        if(epoch_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) {
            return true;
        }
        if (epoch < EpochManager::GetPhysicalEpoch() && IsMergeComplete(epoch)) {
            epoch_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadLocalCounters::IsEpochMergeComplete(const uint64_t& epoch) {
        return epoch_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->load();
    }
    bool ThreadLocalCounters::CheckEpochCommitComplete(const uint64_t& epoch) {
        if (epoch_commit_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() && IsCommitComplete(epoch)) {
            epoch_commit_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadLocalCounters::IsEpochCommitComplete(const uint64_t& epoch) {
        return epoch_commit_complete[epoch % ctx.taasContext.kCacheMaxLength]->load();
    }

    bool ThreadLocalCounters::IsReadValidateComplete(const uint64_t& epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
            if(GetAllThreadLocalCountNum(epoch,i, epoch_should_read_validate_txn_num_local_vec) >
               GetAllThreadLocalCountNum(epoch,i, epoch_read_validated_txn_num_local_vec))
                return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsMergeComplete(const uint64_t& epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
            if(GetAllThreadLocalCountNum(epoch,i, epoch_should_read_validate_txn_num_local_vec) >
               GetAllThreadLocalCountNum(epoch,i, epoch_read_validated_txn_num_local_vec))
                return false;
            if(GetAllThreadLocalCountNum(epoch,i, epoch_should_merge_txn_num_local_vec) >
               GetAllThreadLocalCountNum(epoch,i, epoch_merged_txn_num_local_vec))
                return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsCommitComplete(const uint64_t & epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
            if(GetAllThreadLocalCountNum(epoch,i, epoch_should_commit_txn_num_local_vec) >
               GetAllThreadLocalCountNum(epoch,i, epoch_committed_txn_num_local_vec))
                return false;
        }
        return true;
    }
    bool ThreadLocalCounters::IsRedoLogComplete(const uint64_t & epoch) {
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
            if(GetAllThreadLocalCountNum(epoch,i, epoch_record_commit_txn_num_local_vec) >
               GetAllThreadLocalCountNum(epoch,i, epoch_record_committed_txn_num_local_vec))
                return false;
        }
        return true;
    }

    void ThreadLocalCounters::ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        for(const auto& i : vec) {
            if(i != nullptr)
                i->Clear(epoch);
        }
    }

    uint64_t ThreadLocalCounters::GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch);
        }
        return ans;
    }
    uint64_t ThreadLocalCounters::GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &sharding_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch, sharding_id);
        }
        return ans;
    }

}