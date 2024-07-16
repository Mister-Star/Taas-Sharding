//
// Created by user on 23-7-16.
//

#include "transaction/two_phase_commit.h"

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "proto/transaction.pb.h"
#include "storage/redo_loger.h"
#include "storage/mot.h"

namespace Taas {
    Context TwoPC::ctx;
    uint64_t TwoPC::shard_num;
    uint64_t TwoPC::pre_time, TwoPC::curr_time;

    std::atomic<uint64_t> TwoPC::shard_num_struct_progressing, TwoPC::two_pl_num_progressing, TwoPC::two_pl_req_progressing,
            TwoPC::two_pc_prepare_num_progressing, TwoPC::two_pc_commit_num_progressing;

    std::atomic<uint64_t> TwoPC::successTxnNumber , TwoPC::totalTxnNumber , TwoPC::finishedTxnNumber, TwoPC::lags_,
    TwoPC::failedTxnNumber ,TwoPC::lockFailed, TwoPC::validateFailed, TwoPC::totalTime,
            TwoPC::successTime, TwoPC::failedTime, TwoPC::print_time;
    concurrent_unordered_map<std::string, std::string> TwoPC::row_lock_map;
    concurrent_unordered_map<std::string, std::shared_ptr<TwoPCTxnStateStruct>> TwoPC::txn_state_map;
    std::mutex TwoPC::mutex;
    concurrent_unordered_map<std::string, std::shared_ptr<std::vector<std::shared_ptr<proto::Transaction>>>>
            TwoPC::txn_phase_map;

    concurrent_unordered_map<std::string, std::string>
           TwoPC::row_map_csn;      /// tid, csn

    concurrent_unordered_map<std::string, std::string>
           TwoPC::row_map_data;
    BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>> TwoPC::prepare_lock_queue, TwoPC::commit_unlock_queue;
    concurrent_crdt_unordered_map<std::string, std::string, std::string> TwoPC::abort_txn_set, TwoPC::sent_client_txn_set;

// static 初始化哪些
    bool TwoPC::Init(const Taas::Context &ctx_) {
        ctx = ctx_;
        shard_num = ctx.taasContext.kTxnNodeNum;
        successTxnNumber.store(0);
        totalTxnNumber.store(0);
        failedTxnNumber.store(0);
        lockFailed.store(0);
        validateFailed.store(0);
        totalTime.store(0);
        successTime.store(0);
        failedTime.store(0);
        finishedTxnNumber.store(0);
        lags_.store(0);
        // static init
        pre_time = now_to_us();
        curr_time = now_to_us();
        print_time.store(0);

        shard_num_struct_progressing.store(0);
        two_pl_num_progressing.store(0);
        two_pl_req_progressing.store(0);
        two_pc_prepare_num_progressing.store(0);
        two_pc_commit_num_progressing.store(0);

        return true;
    }


    // 事务发送到client初始化处理
    void TwoPC::ClientTxn_Init() {
        // txn_state_struct 记录当前事务的分片个数，完成个数
        totalTxnNumber.fetch_add(1);
        res = false;
        do{
            txn_ptr->set_csn(now_to_us());
            txn_ptr->set_commit_epoch(EpochManager::GetPushDownEpoch());
            txn_ptr->set_txn_server_id(ctx.taasContext.txn_node_ip_index);
            tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
            res = txn_state_map.insertState(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                                       client_txn));
        } while (!res);

        ///shard  reroute
        auto shard_row_vector = std::make_shared<std::vector<std::shared_ptr<proto::Transaction>>>();
        for (uint64_t i = 0; i < shard_num; i++) {
            shard_row_vector->emplace_back(std::make_shared<proto::Transaction>());
            (*shard_row_vector)[i]->set_csn(txn_ptr->csn());
            (*shard_row_vector)[i]->set_txn_server_id(txn_ptr->txn_server_id());
            (*shard_row_vector)[i]->set_client_ip(txn_ptr->client_ip());
            (*shard_row_vector)[i]->set_client_txn_id(txn_ptr->client_txn_id());
            (*shard_row_vector)[i]->set_shard_id(i);
        }
        for (auto i = 0; i < txn_ptr->row_size(); i++) {
            const auto& row = txn_ptr->row(i);
            auto row_ptr = (*shard_row_vector)[GetHashValue(row.key())]->add_row();
            (*row_ptr) = row;
        }
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        txn_phase_map.insert(tid, shard_row_vector);

        shard_num_struct_progressing.fetch_add(1);

        // send to participants
        for (uint64_t i = 0; i < shard_num; i++) {
            Send(i, *(*shard_row_vector)[i], proto::TxnType::RemoteServerTxn);
        }

    }

  // 2PL 上锁 abort if locked by other txn
    bool TwoPC::Two_PL_LOCK(proto::Transaction& txn) {
        // 上锁完成返回到coordinator
        tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.txn_server_id());
        GetKeySorted(txn);

        if (abort_txn_set.contain(tid)) return false;
        std::atomic<uint64_t> key_lock_num = 0;

        for (auto & iter : key_sorted) {
            /// read needs lock
            std::string holder_tid;
            if (abort_txn_set.contain(tid)) break;
            res = row_lock_map.try_lock(iter.first, tid);
            if (res) {
                key_lock_num.fetch_add(1);
            } else {
                lockFailed.fetch_add(1);
                Two_PL_UNLOCK(txn);
                return false;
            }
        }
          key_sorted.clear();

          if (abort_txn_set.contain(tid)) {
              Two_PL_UNLOCK(txn);
              return false;
          }
        if (!ValidateReadSet(txn)){
            Two_PL_UNLOCK(txn);
           validateFailed.fetch_add(1);
           return false;
        }
        return key_lock_num.load() == key_sorted.size();
    }

    // DeadLock
    bool TwoPC::Two_PL_LOCK_WAIT(proto::Transaction& txn){

        // 上锁完成返回到coordinator
        GetKeySorted(txn);
        tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.txn_server_id());
        std::atomic<uint64_t> key_lock_num = 0;

        for (auto & iter : key_sorted) {
        //          while (!row_lock_map.try_lock(iter->first, tid)){ } /// spinlock never block
            while (true) {
                if (!row_lock_map.try_lock(iter.first, tid)) {
                printSorted(1);
                }
                else{
                    break;
                }
            }
            key_lock_num.fetch_add(1);
        }
        printSorted(1);
        if (!ValidateReadSet(txn)){
        //          LOG(INFO) <<"Txn ValidateReadSet check failed";
          validateFailed.fetch_add(1);
          return false;
        }
        return key_lock_num.load() == key_sorted.size();
    }

    // 2PL 解锁
    bool TwoPC::Two_PL_UNLOCK(proto::Transaction& txn) {
        //      std::lock_guard<std::mutex>lock(mutex);
        GetKeySorted(txn);
        // 事务完全提交或中途abort调用，无需返回coordinator?
        tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.txn_server_id());
        for (auto & iter : key_sorted) {
          row_lock_map.unlock(iter.first, tid);
        }
        key_sorted.clear();
        return true;
    }

  // 检查2PL阶段是否完成
    bool TwoPC::Check_2PL_complete(const std::shared_ptr<TwoPCTxnStateStruct> &txn_state_struct) {
        return txn_state_struct->two_pl_failed_num.load() == 0;
    //    return txn_state_struct->two_pl_num.load() == txn_state_struct->txn_shard_num;
    }

  // 检查2PC prepare是否完成
    bool TwoPC::Check_2PC_Prepare_complete(const std::shared_ptr<TwoPCTxnStateStruct> &txn_state_struct) {
      return txn_state_struct->two_pc_prepare_failed_num.load() == 0;
//    return txn_state_struct->two_pc_prepare_num.load() == txn_state_struct->txn_shard_num;
    }

  // 检查2PC commit是否完成
    bool TwoPC::Check_2PC_Commit_complete(const std::shared_ptr<TwoPCTxnStateStruct> &txn_state_struct) {
      return txn_state_struct->two_pc_commit_failed_num.load() == 0;
//    return txn_state_struct->two_pc_commit_num.load() == txn_state_struct->txn_shard_num;
    }

    void TwoPC::AbortTxn() {
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        if(!txn_state_map.getValue(tid, txn_state_struct)) return;
        if (!txn_phase_map.getValue(tid,tmp_vector)) return;
        if(txn_state_struct == nullptr || tmp_vector == nullptr) return;

        if (sent_client_txn_set.contain(tid, tid)) return;
        sent_client_txn_set.insert(tid, tid);

        for (uint64_t i = 0; i < shard_num; i++) {
            auto to_whom = (*tmp_vector)[i]->shard_id();
            Send(to_whom, *(*tmp_vector)[i], proto::TxnType::Abort_txn);
        }
        SendToClient(*txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
        txn_state_map.insert(tid, nullptr);
        txn_phase_map.insert(tid, nullptr);
//        tmp_vector->clear();
        txn_state_struct.reset();
        tmp_vector.reset();
        CleanTxnState(txn_ptr);
    }

    void TwoPC::CommitTxn() {
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        if(!txn_state_map.getValue(tid, txn_state_struct)) return;
        if (!txn_phase_map.getValue(tid,tmp_vector)) return;

        if (sent_client_txn_set.contain(tid, tid)) return;
        sent_client_txn_set.insert(tid, tid);

        // no need to notify participants
        txn_state_struct->txn_state = commit_done;
        SendToClient(*txn_ptr, proto::TxnType::CommittedTxn, proto::TxnState::Commit);

        if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
            txn_ptr->set_commit_epoch(EpochManager::GetPushDownEpoch());
            txn_ptr->set_storage_type("mot");
//                          LOG(INFO) << "txn log cen: " << txn_ptr->commit_epoch();
            RedoLoger::RedoLog(thread_id, txn_ptr);
        }
        UpdateReadSet(*txn_ptr);          // not this
        CleanTxnState(txn_ptr);
    }

  // 修改哪些元数据
    void TwoPC::SetMessageRelatedCountersInfo() {
        message_server_id = txn_ptr->txn_server_id();
    }

    void TwoPC::OUTPUTLOG(const std::string& s, uint64_t time){
        if(totalTxnNumber.load() == 0) return;
        LOG(INFO) << s.c_str() <<
                "\ntotalTxnNumber: " << totalTxnNumber.load() << "\t\t\t finishedTxnNumber: " << finishedTxnNumber.load() << "\t\t\t disstance: " << lags_ << "\t\t\tfailedTxnNumber: " << failedTxnNumber.load() <<"\t\t\tsuccessTxnNumber: " << successTxnNumber.load() <<
                "\nlockFailed: " << lockFailed.load() << "\t\t\tvalidateFailed: " << validateFailed.load() << "\t\t\tcurTxnTime: " << time <<
                "\n[TotalTxn] avgTotalTxnTime: " << totalTime/totalTxnNumber << "\t\t\t totalTime: " << totalTime.load() <<
                "\n[SuccessTxn] avgSuccessTxnTime: " << (successTxnNumber.load() == 0 ? "Nan": std::to_string(successTime.load()/successTxnNumber.load())) << "\t\t\t successTime: " << successTime.load() <<
                "\n[FailedTxn] avgFailedTxnTime: " << (failedTxnNumber.load() == 0 ? "Nan": std::to_string(failedTime.load()/failedTxnNumber.load())) << "\t\t\t failedTime: " << failedTime.load() <<
                "\n[Lock] lockedRowCount: " << row_lock_map.countLock() <<
                "\n************************************************ ";
        LOG(INFO) << "finish each process shard_num_struct_progressing: " << shard_num_struct_progressing.load() <<", two_pl_num_progressing: " << two_pl_num_progressing.load() << ", two_pl_req_progressing: " <<two_pl_req_progressing.load()
        << ", two_pc_prepare_num_progressing: " << two_pc_prepare_num_progressing.load() << ", two_pc_commit_num_progressing: " << two_pc_commit_num_progressing.load();

        LOG(INFO) << "redo_log_queue size: " << MOT::epoch_redo_log_queue[1]->size_approx()
        << ",txn_phase_map size : " << txn_phase_map.size()
        << ",txn_state_map size : " << txn_state_map.size() << ", row_map_data size :" << row_map_data.size() << ", row_lock_map size : " << row_lock_map.size();
    }

    // handle lock requests
    void TwoPC::PrepareLockThread() {
    std::shared_ptr<proto::Transaction> local_txn_ptr;
    bool sleep_flag;
    LOG(INFO) << "PrepareLockThread starts";
    while (!EpochManager::IsTimerStop()) {
//        sleep_flag = true;
//        TwoPC::prepare_lock_queue.wait_dequeue(local_txn_ptr);
    //        //              if (local_txn_ptr != nullptr && local_txn_ptr->txn_type() == proto::TxnType::NullMark) {
    //            if (local_txn_ptr != nullptr && local_txn_ptr->txn_type() == proto::TxnType::RemoteServerTxn) {
    //                two_pl_req_progressing.fetch_add(1);
    //              if (Two_PL_LOCK(*local_txn_ptr)) {
    //                  // 发送lock ok
    //                  auto to_whom = static_cast<uint64_t >(local_txn_ptr->txn_server_id());
    //                  Send(to_whom, *local_txn_ptr, proto::TxnType::Lock_ok);
    //              } else {
    //                  // 发送lock abort
    //                  auto to_whom = static_cast<uint64_t >(local_txn_ptr->txn_server_id());
    //                  Send(to_whom, *local_txn_ptr, proto::TxnType::Lock_abort);
    //              }
    //            }
    //          key_sorted.clear();
    //          local_txn_ptr.reset();
    //          sleep_flag = false;
    //        }
//            if (sleep_flag) {
                print_time.fetch_add(1);
                if (print_time.load() % 50000 == 0) OUTPUTLOG("============= 2PC + 2PL INFO =============", now_to_us());
                usleep(200);
//            }
    }
    }

    void TwoPC::CommitUnlockThread() {

    }

}  // namespace Taas
