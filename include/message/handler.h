//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_H
#define TAAS_HANDLER_H

#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {
    class MessageHandler{
    public:
        bool Init(uint64_t id, Context context);
        bool HandleReceivedMessage();
        bool HandleReceivedClientTxn();
        bool HandleReceivedServerTxn();
        bool Sharding();
//        bool HandleLocalMergedTxn();
        bool CheckTxnReceiveComplete() const;
        bool HandleTxnCache();

        uint64_t GetHashValue(std::string key) {
            return _hash(key) % sharding_num;
        }
    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, server_dequeue_id = 0, epoch_mod = 0,
                epoch = 0, clear_epoch = 0,max_length = 0, sharding_num = 0;
        bool res, sleep_flag;

        //other txn node sends shrading txn which should be merged in current txn node
        //sharding_cache[epoch][server_id(sharding_id)].queue
        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> sharding_cache;
        //sharding counters
        static AtomicCounters_Cache ///epoch, index, value
            sharding_should_receive_pack_num, sharding_received_pack_num,
            sharding_should_receive_txn_num, sharding_received_txn_num;

        static AtomicCounters_Cache ///epoch, index, value
            sharding_should_handle_local_txn_num, sharding_handled_local_txn_num,
            sharding_should_send_pack_num, sharding_send_pack_num,
            sharding_should_send_txn_num, sharding_send_txn_num;

        //other txn node sends shrading txn backup to current txn node
        //backup_cache[epoch][sharding_id(server_id)].queue
        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> backup_cache;
        //backup counters
        static AtomicCounters_Cache ///epoch, index, value
            backup_should_receive_pack_num, backup_received_pack_num,
            backup_should_receive_txn_num, backup_received_txn_num;

        Context ctx;

        std::hash<std::string> _hash;
    };

}

#endif //TAAS_HANDLER_H
