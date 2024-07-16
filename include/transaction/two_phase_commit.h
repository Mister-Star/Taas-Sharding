//
// Created by user on 23-7-16.
//

#ifndef TAAS_TWO_PHASE_COMMIT_H
#define TAAS_TWO_PHASE_COMMIT_H
#pragma once

#include <zmq.hpp>

#include "epoch/epoch_manager.h"
#include "message/twoPC_message_receive_handler.h"
#include "message/twoPC_message_send_handler.h"
#include "proto/message.pb.h"
#include "tools/concurrent_hash_map.h"
#include "tools/utilities.h"
#include "merge.h"

namespace Taas {
  
  class TwoPC {
  public:
    static concurrent_unordered_map<std::string, std::string>
        row_lock_map;  /// key, tid

    // 将事务和元数据map
    static concurrent_unordered_map<std::string, std::shared_ptr<TwoPCTxnStateStruct>>
        txn_state_map;  /// tid, txn struct

    static concurrent_unordered_map<std::string, std::string>
        row_map_csn;      /// tid, csn

    static concurrent_unordered_map<std::string, std::string>
        row_map_data;            /// tid, changed data

    // store sharing txn
    static concurrent_unordered_map<std::string, std::shared_ptr<std::vector<std::shared_ptr<proto::Transaction>>>>
        txn_phase_map;          /// tid, txn sharding vector



      // 工具
    struct Comparator {
      bool operator()(const std::string& x1, const std::string& x2) const{
        return x1 < x2;
      }
    };

    void SetTxnState(proto::Transaction& txn){
        if (txn.shard_id() == ctx.taasContext.txn_node_ip_index) {
            tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
            txn_state_map.getValue(tid, txn_state_struct);
            if (txn_state_struct == nullptr) {
                return;
            }
            if (txn_state_struct->txn_state == TwoPCTxnState::abort_txn) {
                txn.set_txn_type(proto::TxnType::Abort_txn);            // set abort state
                abort_txn_set.insert(tid, tid);
                return;
            }
            if (txn.txn_type() == proto::TxnType::Abort_txn) {
                txn_state_struct->txn_state = TwoPCTxnState::abort_txn;     // set abort state
                abort_txn_set.insert(tid, tid);
            }
        }
    }

    uint64_t GetHashValue(const std::string& key) const {
        uint64_t hash_value = _hash(key);
        return hash_value % shard_num;
    }
    // 生成key_sorted, map[key] = changed_data
    void GetKeySorted(proto::Transaction& txn) {
        key_sorted.clear();
        // k-v : row_key txn
      for (int i = 0; i < (int) txn.row_size(); i++) {
          if (txn.row(i).op_type() == proto::OpType::Read) continue;
          key_sorted[txn.row(i).key()] =  i;
      }
    }

    // validate set
    static bool ValidateReadSet(proto::Transaction& txn){
        std::string version;
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            auto key = txn.storage_type() + ":" + row.key();
            if(row.op_type() != proto::OpType::Read) {
                continue;
            }
            /// indeed, we should use the csn to check the read version,
            /// but there are some bugs in updating the csn to the storage(tikv).
            if (!row_map_data.getValue_1(key, version)) {
                /// should be abort, but Taas do not connect load data,
                /// so read the init snap will get empty in read_version_map
                continue;
            }
            /// data out of date
            if (!version.empty() && version != row.data()) {
                return false;
//                return true;            /// test
            }
        }
        return true;
    }

    // update set
    bool UpdateReadSet(proto::Transaction& txn){
        tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.txn_server_id());
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            auto key = txn.storage_type() + ":" + row.key();
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            row_map_data.insert(key, row.data());
//            row_map_csn.insert(key, tid);
        }
        return true;
    }

    void ClientTxn_Init();
    bool Two_PL_LOCK(proto::Transaction& txn);
    bool Two_PL_LOCK_WAIT(proto::Transaction& txn);
    bool Two_PL_UNLOCK(proto::Transaction& txn);
    static bool Check_2PL_complete(const std::shared_ptr<TwoPCTxnStateStruct> &);
    static bool Check_2PC_Prepare_complete(const std::shared_ptr<TwoPCTxnStateStruct> &);
    static bool Check_2PC_Commit_complete(const std::shared_ptr<TwoPCTxnStateStruct> &);

    void AbortTxn();
    void CommitTxn();
    bool Send(uint64_t& to_whom, proto::Transaction& txn,
              proto::TxnType txn_type);
    static bool SendToClient(proto::Transaction& txn, proto::TxnType txn_type,
                   proto::TxnState txn_state);
    static bool Init(const Taas::Context &ctx_);
      void HandleClientMessage();// 处理接收到的消息 from client
      void HandleReceivedMessage();  // from coordinator

      void HandleReceivedTxn();      // 处理接收到的事务（coordinator/applicant）
      void SetMessageRelatedCountersInfo();

    static void OUTPUTLOG(const std::string& s, uint64_t time); // print logs


    // debug map
    static std::mutex mutex;
    void printSorted(int type){
        std::unique_lock<std::mutex> lock(mutex);
        std::cout << "current tid : " << tid << " === ";
        std::string tmp = "-1";
        if (type == 1) {
            std::cout << "Lock  ";
        } else {
            std::cout << "Unlock  ";
        }
        for (auto & iter : key_sorted) {
            row_lock_map.getValue(iter.first, tmp);
            if (!tmp.empty() && tmp != "0" && tmp != "-1" && tmp != tid){
                std::cout <<"{ key : "<< iter.first <<" tid :" << tmp <<"} ";
            }
        }
        std::cout << std::endl;
        lock.unlock();
    }

    std::unique_ptr<zmq::message_t> message_ptr;
    std::unique_ptr<std::string> message_string_ptr;
    std::unique_ptr<proto::Message> msg_ptr;
    std::shared_ptr<proto::Transaction> txn_ptr;
    std::unique_ptr<pack_params> pack_param;
    std::shared_ptr<std::vector<std::shared_ptr<proto::Transaction>>>  tmp_vector;
    std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
    std::string csn_temp, key_temp, key_str, table_name, csn_result;
    uint64_t thread_id = 0, server_dequeue_id, epoch_mod, epoch, max_length,
                                                         /// cache check
        message_epoch, message_shard_id, message_server_id;  /// message epoch info
    static uint64_t  shard_num;
    static Context ctx;
    static uint64_t pre_time, curr_time;

    std::string tid;  // 记录当前tid
    std::map<std::string, uint64_t> key_sorted; // first is the key/row

    bool res;

    std::hash<std::string> _hash;

    /// 从client得到的txn的tid
    // std::string tid_client, tid_to_remote, tid_from_remote;

    /// to_whom = all
    uint64_t to_whom_all = 0;

    static std::atomic<uint64_t> shard_num_struct_progressing, two_pl_num_progressing, two_pl_req_progressing,
        two_pc_prepare_num_progressing, two_pc_commit_num_progressing;      // finish each process num

    static std::atomic<uint64_t> successTxnNumber , totalTxnNumber, failedTxnNumber, finishedTxnNumber, lags_,
        lockFailed, validateFailed, totalTime, successTime, failedTime;

    static std::atomic<uint64_t> print_time;

    // use queue to lock/unlock
    static BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>> prepare_lock_queue, commit_unlock_queue;
    static concurrent_crdt_unordered_map<std::string, std::string, std::string> abort_txn_set;
    static concurrent_crdt_unordered_map<std::string, std::string, std::string> sent_client_txn_set;

    static void PrepareLockueueEnqueue(const std::shared_ptr<proto::Transaction> &txn_ptr_) {
      prepare_lock_queue.enqueue(txn_ptr_);
      prepare_lock_queue.enqueue(nullptr);
    }

    static void CommitUnlockueueEnqueue(const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        commit_unlock_queue.enqueue(txn_ptr_);
        commit_unlock_queue.enqueue(nullptr);
    }

    static void CleanTxnState(const std::shared_ptr<proto::Transaction>& local_txn_ptr) {
        auto tid = std::to_string(local_txn_ptr->csn()) + ":" + std::to_string(local_txn_ptr->txn_server_id());
        txn_phase_map.remove(tid);
        txn_state_map.remove(tid);
        abort_txn_set.remove(tid);
    }

    void PrepareLockThread();
    void CommitUnlockThread();

      bool SendREP(uint64_t &to_whom, proto::Transaction &txn, proto::TxnType txn_type);
  };

}  // namespace Taas

#endif  // TAAS_TWO_PHASE_COMMIT_H
