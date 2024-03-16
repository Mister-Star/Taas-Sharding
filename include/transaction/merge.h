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
#include "tools/thread_counters.h"

#include <cstdint>

namespace Taas {

    class Merger : public ThreadCounters {

    private:
        Context ctx;
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::shared_ptr<proto::Transaction> txn_ptr, write_set, backup_txn, full_txn;
        std::shared_ptr<std::vector<std::shared_ptr<proto::Transaction>>> sharding_row_vector;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t local_server_id,
                server_dequeue_id = 0, epoch_mod = 0, epoch = 0, max_length = 0, sharding_num = 0,///cache check
        message_epoch = 0, message_epoch_mod = 0, message_sharding_id = 0, message_server_id = 0, ///message epoch info
        server_reply_ack_id = 0,
                cache_clear_epoch_num = 0, cache_clear_epoch_num_mod = 0,
                redo_log_push_down_reply = 1, txn_server_id = 0;;

        bool res, sleep_flag;
        std::shared_ptr<proto::Transaction> empty_txn_ptr;
        std::hash<std::string> _hash;

        [[nodiscard]] uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % sharding_num;
        }

    public:
        bool MergeQueueTryDequeue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        bool CommitQueueTryDequeue(uint64_t &epoch_, std::shared_ptr<proto::Transaction> txn_ptr_);

        void MergeInit(const uint64_t &id, const Context &ctx_);
        void ReadValidate();
        void Send();
        void Merge();
        void Commit();
        void EpochMerge();

        void ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction> &txn_ptr_);
        void MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void CommitQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
    };
}

#endif //TAAS_MERGE_H
