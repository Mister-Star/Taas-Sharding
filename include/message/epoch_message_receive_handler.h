//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H
#define TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H

#pragma once

#include "queue"

#include "zmq.hpp"

#include "message/message.h"
#include "tools/utilities.h"
#include "tools/thread_pool_light.h"
#include "tools/thread_local_counters.h"

namespace Taas {

class EpochMessageReceiveHandler : public Taas::ThreadLocalCounters {
    public:
        bool Init(const uint64_t &id);

        void HandleReceivedMessage();
        void HandleReceivedControlMessage();
        bool SetMessageRelatedCountersInfo();
        bool HandleReceivedTxn();
        void HandleMultiModelClientSubTxn(const uint64_t& txn_id);
        uint64_t getMultiModelTxnId();
        bool HandleMultiModelClientTxn();
        bool UpdateEpochAbortSet();

        [[nodiscard]] uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % sharding_num;
        }

        void ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction> &txn_ptr_);
        void MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void CommitQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);

        static bool StaticInit(const Context& context);
        static bool StaticClear(uint64_t& epoch);

    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::shared_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, local_server_id = 0,
                server_dequeue_id = 0, epoch_mod = 0, epoch = 0, max_length = 0, sharding_num = 0,///cache check
                message_epoch = 0, message_epoch_mod = 0, message_sharding_id = 0, message_server_id = 0, ///message epoch info
                server_reply_ack_id = 0,
                cache_clear_epoch_num = 0, cache_clear_epoch_num_mod = 0,
                redo_log_push_down_reply = 1;

        bool res, sleep_flag;
        std::shared_ptr<proto::Transaction> empty_txn_ptr;
        std::hash<std::string> _hash;


    public:

        static Context ctx;

        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
            epoch_backup_txn,
            epoch_insert_set,
            epoch_abort_set;

        static concurrent_unordered_map<std::string, std::shared_ptr<MultiModelTxn>> multiModelTxnMap;

        void Sharding();

    };

}

#endif //TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H
