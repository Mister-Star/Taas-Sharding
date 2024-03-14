//
// Created by 周慰星 on 11/9/22.
//
#include <queue>

#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "message/epoch_message_receive_handler.h"
#include "tools/utilities.h"
#include "transaction/merge.h"


namespace Taas {
//    const uint64_t PACKNUM = 1L<<32;///
    Context EpochMessageReceiveHandler::ctx;
    std::vector<uint64_t>
        EpochMessageReceiveHandler::sharding_send_ack_epoch_num,
        EpochMessageReceiveHandler::backup_send_ack_epoch_num,
        EpochMessageReceiveHandler::backup_insert_set_send_ack_epoch_num,
        EpochMessageReceiveHandler::abort_set_send_ack_epoch_num; /// check and reply ack

    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
        EpochMessageReceiveHandler::epoch_backup_txn,
        EpochMessageReceiveHandler::epoch_insert_set,
        EpochMessageReceiveHandler::epoch_abort_set;

    concurrent_unordered_map<std::string, std::shared_ptr<MultiModelTxn>> EpochMessageReceiveHandler::multiModelTxnMap;

    std::vector<std::unique_ptr<std::atomic<bool>>>
        EpochMessageReceiveHandler::epoch_sharding_send_complete,
        EpochMessageReceiveHandler::epoch_sharding_receive_complete,
        EpochMessageReceiveHandler::epoch_back_up_complete,
        EpochMessageReceiveHandler::epoch_abort_set_merge_complete,
        EpochMessageReceiveHandler::epoch_insert_set_complete;

    ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
    AtomicCounters_Cache ///epoch, server_id, value
        ///epoch txn counters
        EpochMessageReceiveHandler::sharding_should_handle_local_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_handled_local_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_should_handle_remote_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_handled_remote_txn_num(10, 1),
        ///local txn counters
        EpochMessageReceiveHandler::sharding_should_send_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_send_txn_num(10, 1),
        ///remote sharding txn counters
        EpochMessageReceiveHandler::sharding_should_receive_pack_num(10, 1),
        EpochMessageReceiveHandler::sharding_received_pack_num(10, 1),
        EpochMessageReceiveHandler::sharding_should_receive_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_received_txn_num(10, 1),
        ///sharding ack
        EpochMessageReceiveHandler::sharding_received_ack_num(10, 1),
        ///backup send counters
        EpochMessageReceiveHandler::backup_should_send_txn_num(10, 1),
        EpochMessageReceiveHandler::backup_send_txn_num(10, 1),
        EpochMessageReceiveHandler::backup_received_ack_num(10, 1),
        ///backup receive
        EpochMessageReceiveHandler::backup_should_receive_pack_num(10, 1),
        EpochMessageReceiveHandler::backup_received_pack_num(10, 1),
        EpochMessageReceiveHandler::backup_should_receive_txn_num(10, 1),
        EpochMessageReceiveHandler::backup_received_txn_num(10, 1),

        ///insert set counters
        EpochMessageReceiveHandler::insert_set_should_receive_num(10, 1),
        EpochMessageReceiveHandler::insert_set_received_num(10, 1),
        EpochMessageReceiveHandler::insert_set_received_ack_num(10, 1),
        ///abort set counters
        EpochMessageReceiveHandler::abort_set_should_receive_num(10, 1),
        EpochMessageReceiveHandler::abort_set_received_num(10, 1),
        EpochMessageReceiveHandler::abort_set_received_ack_num(10, 1),
        ///redo log
        EpochMessageReceiveHandler::redo_log_push_down_ack_num(10, 1),
        EpochMessageReceiveHandler::redo_log_push_down_local_epoch(10, 1);

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
        EpochMessageReceiveHandler::sharding_should_send_txn_num_local_vec,
        EpochMessageReceiveHandler::sharding_send_txn_num_local_vec,
        EpochMessageReceiveHandler::sharding_should_handle_local_txn_num_local_vec,
        EpochMessageReceiveHandler::sharding_handled_local_txn_num_local_vec,

        EpochMessageReceiveHandler::sharding_should_handle_remote_txn_num_local_vec,
        EpochMessageReceiveHandler::sharding_handled_remote_txn_num_local_vec,
        EpochMessageReceiveHandler::sharding_received_txn_num_local_vec,

        EpochMessageReceiveHandler::backup_should_send_txn_num_local_vec,
        EpochMessageReceiveHandler::backup_send_txn_num_local_vec,
        EpochMessageReceiveHandler::backup_received_txn_num_local_vec;

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
        EpochMessageReceiveHandler::epoch_should_read_validate_txn_num_local_vec,
        EpochMessageReceiveHandler::epoch_read_validated_txn_num_local_vec,
        EpochMessageReceiveHandler::epoch_should_merge_txn_num_local_vec,
        EpochMessageReceiveHandler::epoch_merged_txn_num_local_vec,
        EpochMessageReceiveHandler::epoch_should_commit_txn_num_local_vec,
        EpochMessageReceiveHandler::epoch_committed_txn_num_local_vec,
        EpochMessageReceiveHandler::epoch_record_commit_txn_num_local_vec,
        EpochMessageReceiveHandler::epoch_record_committed_txn_num_local_vec;


    bool EpochMessageReceiveHandler::Init(const uint64_t &id) {
        message_ptr = nullptr;
        txn_ptr.reset();
        thread_id = id;
        sharding_num = ctx.taasContext.kTxnNodeNum;
        local_server_id = ctx.taasContext.txn_node_ip_index;
        max_length = ctx.taasContext.kCacheMaxLength;

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


        return true;
    }

    bool EpochMessageReceiveHandler::StaticInit(const Context& context) {
        ctx = context;
        auto max_length = context.taasContext.kCacheMaxLength;
        auto sharding_num = context.taasContext.kTxnNodeNum;

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

        epoch_backup_txn.resize(max_length);
        epoch_insert_set.resize(max_length);
        epoch_abort_set.resize(max_length);
        epoch_sharding_send_complete.resize(max_length);
        epoch_sharding_receive_complete.resize(max_length);
        epoch_back_up_complete.resize(max_length);
        epoch_abort_set_merge_complete.resize(max_length);
        epoch_insert_set_complete.resize(max_length);

        sharding_should_send_txn_num_local_vec.resize(sharding_num);
        sharding_send_txn_num_local_vec.resize(sharding_num);
        sharding_should_handle_local_txn_num_local_vec.resize(sharding_num);
        sharding_handled_local_txn_num_local_vec.resize(sharding_num);
        sharding_should_handle_remote_txn_num_local_vec.resize(sharding_num);
        sharding_handled_remote_txn_num_local_vec.resize(sharding_num);
        sharding_received_txn_num_local_vec.resize(sharding_num);
        backup_should_send_txn_num_local_vec.resize(sharding_num);
        backup_send_txn_num_local_vec.resize(sharding_num);
        backup_received_txn_num_local_vec.resize(sharding_num);

        epoch_should_read_validate_txn_num_local_vec.resize(sharding_num);
        epoch_should_merge_txn_num_local_vec.resize(sharding_num);
        epoch_merged_txn_num_local_vec.resize(sharding_num);
        epoch_should_commit_txn_num_local_vec.resize(sharding_num);
        epoch_committed_txn_num_local_vec.resize(sharding_num);
        epoch_record_commit_txn_num_local_vec.resize(sharding_num);
        epoch_record_committed_txn_num_local_vec.resize(sharding_num);


        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            epoch_sharding_send_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_sharding_receive_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_back_up_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_abort_set_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_insert_set_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_backup_txn[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_insert_set[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_abort_set[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }


        sharding_should_handle_local_txn_num.Init(max_length, sharding_num),sharding_handled_local_txn_num.Init(max_length, sharding_num),
        sharding_should_handle_remote_txn_num.Init(max_length, sharding_num),sharding_handled_remote_txn_num.Init(max_length, sharding_num),

        sharding_should_send_txn_num.Init(max_length, sharding_num),
        sharding_send_txn_num.Init(max_length, sharding_num),
        sharding_received_ack_num.Init(max_length, sharding_num),

        sharding_should_receive_pack_num.Init(max_length, sharding_num, 1),
        sharding_received_pack_num.Init(max_length, sharding_num),
        sharding_should_receive_txn_num.Init(max_length, sharding_num, 0),
        sharding_received_txn_num.Init(max_length, sharding_num),

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

        return true;
    }

    bool EpochMessageReceiveHandler::StaticClear(uint64_t& epoch) {
        auto cache_clear_epoch_num_mod = epoch % ctx.taasContext.kCacheMaxLength;
        sharding_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        sharding_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_received_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_handle_local_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_handled_local_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_handle_remote_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_handled_remote_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),
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

        clearAllThreadLocalCountNum(epoch, sharding_should_send_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, sharding_send_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, sharding_should_handle_local_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, sharding_handled_local_txn_num_local_vec);

        clearAllThreadLocalCountNum(epoch, sharding_should_handle_remote_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, sharding_handled_remote_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, sharding_received_txn_num_local_vec);

        clearAllThreadLocalCountNum(epoch, backup_should_send_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, backup_send_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, backup_received_txn_num_local_vec);

        clearAllThreadLocalCountNum(epoch, epoch_should_read_validate_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, epoch_read_validated_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, epoch_should_merge_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, epoch_merged_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, epoch_should_commit_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, epoch_committed_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, epoch_record_commit_txn_num_local_vec);
        clearAllThreadLocalCountNum(epoch, epoch_record_committed_txn_num_local_vec);

//        epoch_backup_txn[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
//        epoch_insert_set[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
//        epoch_abort_set[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();

        return true;
    }

    bool EpochMessageReceiveHandler::CheckEpochShardingSendComplete(const uint64_t& epoch) {
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

    bool EpochMessageReceiveHandler::CheckEpochShardingReceiveComplete(uint64_t& epoch) {
        if (epoch_sharding_receive_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() &&
            IsShardingPackReceiveComplete(epoch) &&
            IsShardingTxnReceiveComplete(epoch)) {
            epoch_sharding_receive_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }

    bool EpochMessageReceiveHandler::CheckEpochBackUpComplete(uint64_t& epoch) {
        if (epoch_back_up_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetPhysicalEpoch() && IsBackUpACKReceiveComplete(epoch)
           &&IsBackUpSendFinish(epoch)) {
            epoch_back_up_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }

    bool EpochMessageReceiveHandler::CheckEpochAbortSetMergeComplete(uint64_t& epoch) {
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

    bool EpochMessageReceiveHandler::CheckEpochInsertSetMergeComplete(uint64_t& epoch) {
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


    void EpochMessageReceiveHandler::ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_read_validate_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->server_id(), 1);
//        epoch_should_read_validate_txn_num.IncCount(epoch_mod_temp, txn_ptr->server_id(), 1);
        Merger::epoch_read_validate_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        Merger::epoch_read_validate_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void EpochMessageReceiveHandler::MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_merge_txn_num_local->IncCount(epoch_mod_temp, txn_ptr->server_id(), 1);
//        epoch_should_merge_txn_num.IncCount(epoch_mod_temp, txn_ptr->server_id(), 1);
        Merger::epoch_merge_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        Merger::epoch_merge_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void EpochMessageReceiveHandler::CommitQueueEnqueue(uint64_t& epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % ctx.taasContext.kCacheMaxLength;
        epoch_should_commit_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->server_id(), 1);
//        epoch_should_commit_txn_num.IncCount(epoch_mod_temp, txn_ptr->server_id(), 1);
        Merger::epoch_commit_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        Merger::epoch_commit_queue[epoch_mod_temp]->enqueue(nullptr);
    }



    void EpochMessageReceiveHandler::HandleReceivedMessage() {
        while(!EpochManager::IsTimerStop()) {
            if(MessageQueue::listen_message_txn_queue->try_dequeue(message_ptr)) {
                if (message_ptr == nullptr || message_ptr->empty()) continue;
                message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                   message_ptr->size());
                msg_ptr = std::make_unique<proto::Message>();
                res = UnGzip(msg_ptr.get(), message_string_ptr.get());
                assert(res);
                txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
                HandleReceivedTxn();
                txn_ptr.reset();
            }
            else {
                usleep(50);
            }
        }
    }

    void EpochMessageReceiveHandler::HandleReceivedControlMessage() {
        while(!EpochManager::IsTimerStop()) {
            if(MessageQueue::listen_message_epoch_queue->try_dequeue(message_ptr)) {
                if (message_ptr == nullptr || message_ptr->empty()) continue;
                message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                   message_ptr->size());
                msg_ptr = std::make_unique<proto::Message>();
                res = UnGzip(msg_ptr.get(), message_string_ptr.get());
                assert(res);
                txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
                HandleReceivedTxn();
                txn_ptr.reset();
            }
            else {
                usleep(50);
            }
        }
    }

    void EpochMessageReceiveHandler::Sharding() {
        if(ctx.taasContext.taasMode == TaasMode::Sharding) {/// send then validate on the remote server
            auto sharding_row_vector = std::make_shared<std::vector<std::shared_ptr<proto::Transaction>>>() ;
            for(uint64_t i = 0; i < sharding_num; i ++) {
                sharding_row_vector->emplace_back(std::make_shared<proto::Transaction>());
                auto vector_i = &(*((*sharding_row_vector)[i]));
                vector_i->set_csn(txn_ptr->csn());
                vector_i->set_commit_epoch(txn_ptr->commit_epoch());
                vector_i->set_server_id(txn_ptr->server_id());
                vector_i->set_client_ip(txn_ptr->client_ip());
                vector_i->set_client_txn_id(txn_ptr->client_txn_id());
                vector_i->set_sharding_id(i);
                vector_i->set_txn_type(proto::RemoteServerTxn);
            }
            for(auto i = 0; i < txn_ptr->row_size(); i ++) {
                const auto& row = txn_ptr->row(i);
                auto row_ptr = (*sharding_row_vector)[GetHashValue(row.key())]->add_row();
                (*row_ptr) = row;
            }
            for(uint64_t i = 0; i < sharding_num; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index) {
                    ReadValidateQueueEnqueue(message_epoch, (*sharding_row_vector)[i]);
                    MergeQueueEnqueue(message_epoch, (*sharding_row_vector)[i]);
                } else {
                    if((*sharding_row_vector)[i]->row_size() > 0) {
                        sharding_should_send_txn_num_local->IncCount(message_epoch, i, 1);
//                        EpochMessageReceiveHandler::sharding_should_send_txn_num.IncCount(message_epoch, i, 1);
                        EpochMessageSendHandler::SendTxnToServer(message_epoch, i, (*sharding_row_vector)[i], proto::TxnType::RemoteServerTxn);
                        sharding_send_txn_num_local->IncCount(message_epoch, i, 1);
//                        EpochMessageReceiveHandler::sharding_send_txn_num.IncCount(message_epoch, i, 1);
                    }
                }
            }
            backup_should_send_txn_num_local->IncCount(message_epoch, message_server_id, 1);
//            EpochMessageReceiveHandler::backup_should_send_txn_num.IncCount(message_epoch, message_server_id, 1);
            EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, txn_ptr, proto::TxnType::BackUpTxn);
            backup_send_txn_num_local->IncCount(message_epoch, message_server_id, 1);
//            EpochMessageReceiveHandler::backup_send_txn_num.IncCount(message_epoch, message_server_id, 1);
            CommitQueueEnqueue(message_epoch, txn_ptr);
        }
        else if(ctx.taasContext.taasMode == TaasMode::MultiMaster) {/// validate and then send
            ReadValidateQueueEnqueue(message_epoch, txn_ptr);
        }
    }

    bool EpochMessageReceiveHandler::SetMessageRelatedCountersInfo() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = message_epoch % ctx.taasContext.kCacheMaxLength;
        message_server_id = txn_ptr->server_id();
        csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
        return true;
    }

    bool EpochMessageReceiveHandler::HandleReceivedTxn() {
        SetMessageRelatedCountersInfo();
        switch (txn_ptr->txn_type()) {
            ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
            case proto::TxnType::ClientTxn : {/// sql node --> txn node
                if(ctx.taasContext.taasMode == TaasMode::MultiModel) {
                    HandleMultiModelClientTxn();
                }
                else {
                    message_epoch = EpochManager::GetPhysicalEpoch();
                    sharding_should_handle_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
//                    sharding_should_handle_local_txn_num.IncCount(message_epoch, thread_id, 1);
                    txn_ptr->set_commit_epoch(message_epoch);
                    txn_ptr->set_csn(now_to_us());
                    txn_ptr->set_server_id(ctx.taasContext.txn_node_ip_index);
                    txn_ptr->set_txn_type(proto::RemoteServerTxn);/// change to server txn, then, use server_id to check
                    SetMessageRelatedCountersInfo();
                    Sharding();
                    sharding_handled_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
//                    sharding_handled_local_txn_num.IncCount(message_epoch, thread_id, 1);
                }
                break;
            }
            case proto::TxnType::RemoteServerTxn : {
//                sharding_should_handle_remote_txn_num.IncCount(message_epoch, thread_id, 1);
                sharding_should_handle_remote_txn_num_local->IncCount(message_epoch, local_server_id, 1);
                Merger::epoch_txn_map[message_epoch_mod]->insert(csn_temp, txn_ptr);
                if(ctx.taasContext.taasMode == TaasMode::Sharding) { /// multi-master mode, remote txn no need to do read validate.
                    ReadValidateQueueEnqueue(message_epoch, txn_ptr);
                }
                MergeQueueEnqueue(message_epoch, txn_ptr);
                CommitQueueEnqueue(message_epoch, txn_ptr);
                sharding_received_txn_num_local->IncCount(message_epoch,message_server_id, 1);
                sharding_handled_remote_txn_num_local->IncCount(message_epoch, local_server_id, 1);
//                sharding_received_txn_num.IncCount(message_epoch,message_server_id, 1);
//                sharding_handled_remote_txn_num.IncCount(message_epoch, thread_id, 1);
                break;
            }
            case proto::TxnType::EpochEndFlag : {
                sharding_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                sharding_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochShardingReceiveComplete(message_epoch);
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::EpochShardingACK);
                break;
            }
            case proto::TxnType::BackUpTxn : {
                Merger::epoch_back_txn_map[message_epoch_mod]->insert(csn_temp, txn_ptr);
                backup_received_txn_num_local->IncCount(message_epoch,message_server_id, 1);
//                backup_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpEpochEndFlag : {
                backup_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                backup_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::BackUpACK);
                break;
            }
            case proto::TxnType::AbortSet : {
                UpdateEpochAbortSet();
                abort_set_received_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::AbortSetACK);
                break;
            }
            case proto::TxnType::InsertSet : {
                insert_set_received_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::InsertSetACK);
                break;
            }
            case proto::TxnType::EpochShardingACK : {
                sharding_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochShardingSendComplete(message_epoch);
                break;
            }
            case proto::TxnType::BackUpACK : {
                backup_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochBackUpComplete(message_epoch);
                break;
            }
            case proto::TxnType::AbortSetACK : {
                abort_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochAbortSetMergeComplete(message_epoch);
                break;
            }
            case proto::TxnType::InsertSetACK : {
                insert_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochLogPushDownComplete : {
                redo_log_push_down_ack_num.IncCount(message_epoch,message_server_id, 1);
            break;
        }
        case proto::NullMark:
            case proto::TxnType_INT_MIN_SENTINEL_DO_NOT_USE_:
            case proto::TxnType_INT_MAX_SENTINEL_DO_NOT_USE_:
            case proto::CommittedTxn:
            case proto::Lock_ok:
            case proto::Lock_abort:
            case proto::Prepare_req:
            case proto::Prepare_ok:
            case proto::Prepare_abort:
            case proto::Commit_req:
            case proto::Commit_ok:
            case proto::Commit_abort:
            case proto::Abort_txn:
                break;
        }
        return true;
    }

    uint64_t EpochMessageReceiveHandler::getMultiModelTxnId() {
        for(auto i = 0; i < txn_ptr->row_size(); i ++) {
            const auto &row = txn_ptr->row(i);
            if (row.op_type() == proto::OpType::Read) {
                continue;
            }
            std::string tempData = txn_ptr->row(0).data();
            std::string tempKey = txn_ptr->row(0).key();
            uint64_t index;
            if (!tempData.empty()) {
                index = tempData.find("tid:");
                if (index < tempData.length()) {
                    auto tid = std::strtoull(&tempData.at(index), nullptr, 10);
                    return tid;
                }
            }
        }
        return 0;
    }

    void EpochMessageReceiveHandler::HandleMultiModelClientSubTxn(const uint64_t& txn_id) {
//        sharding_should_handle_local_txn_num.IncCount(message_epoch, thread_id, 1);
        sharding_should_handle_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
        txn_ptr->set_commit_epoch(message_epoch);
        txn_ptr->set_csn(txn_id);
        txn_ptr->set_server_id(ctx.taasContext.txn_node_ip_index);
        SetMessageRelatedCountersInfo();
        ReadValidateQueueEnqueue(message_epoch, txn_ptr);
//        HandleClientTxn();
        CommitQueueEnqueue(message_epoch, txn_ptr);
        sharding_handled_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
//        sharding_handled_local_txn_num.IncCount(message_epoch, thread_id, 1);
    }

    bool EpochMessageReceiveHandler::HandleMultiModelClientTxn() {
        std::shared_ptr<MultiModelTxn> multiModelTxn;
        uint64_t txn_id;
        if(txn_ptr->storage_type() == "mot" || txn_ptr->storage_type() == "nebula") {
            txn_id = getMultiModelTxnId();
        }
        else {
            txn_id = txn_ptr->client_txn_id();
        }
        multiModelTxnMap.getValue(std::to_string(txn_id), multiModelTxn);
        if(txn_ptr->storage_type() == "kv") {
            multiModelTxn->total_txn_num = txn_ptr->csn(); // total sub txn num
        }
        multiModelTxn->received_txn_num += 1;
        if(multiModelTxn->total_txn_num == multiModelTxn->received_txn_num) {
            message_epoch = EpochManager::GetPhysicalEpoch();
            sharding_should_handle_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
//            sharding_should_handle_local_txn_num.IncCount(message_epoch, thread_id, 1);
            txn_ptr = multiModelTxn->kv;
            HandleMultiModelClientSubTxn(txn_id);
            if(multiModelTxn->sql != nullptr) {
                txn_ptr = multiModelTxn->sql;
                HandleMultiModelClientSubTxn(txn_id);
            }
            if(multiModelTxn->gql != nullptr) {
                txn_ptr = multiModelTxn->gql;
                HandleMultiModelClientSubTxn(txn_id);
            }
            sharding_handled_local_txn_num_local->IncCount(message_epoch, local_server_id, 1);
//            sharding_handled_local_txn_num.IncCount(message_epoch, thread_id, 1);
        }
        return true;
    }



    bool EpochMessageReceiveHandler::UpdateEpochAbortSet() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.taasContext.kCacheMaxLength;
        for(int i = 0; i < txn_ptr->row_size(); i ++) {
            Merger::epoch_abort_txn_set[message_epoch_mod]->insert(txn_ptr->row(i).key(), txn_ptr->row(i).data());
        }
        return true;
    }



}