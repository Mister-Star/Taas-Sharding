//
// Created by user on 23-7-16.
//

#include "transaction/two_phase_commit.h"

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "proto/transaction.pb.h"
#include "storage/redo_loger.h"

namespace Taas {
    Context TwoPC::ctx;
    uint64_t TwoPC::shard_num;
    uint64_t TwoPC::pre_time, TwoPC::curr_time;
    std::atomic<uint64_t> TwoPC::successTxnNumber , TwoPC::totalTxnNumber , TwoPC::finishedTxnNumber, TwoPC::lags_,
    TwoPC::failedTxnNumber ,TwoPC::lockFailed, TwoPC::validateFailed, TwoPC::totalTime,
            TwoPC::successTime, TwoPC::failedTime;
    concurrent_unordered_map<std::string, std::string> TwoPC::row_lock_map;
    concurrent_unordered_map<std::string, std::shared_ptr<TwoPCTxnStateStruct>> TwoPC::txn_state_map;
    std::mutex TwoPC::mutex;
    concurrent_unordered_map<std::string ,std::vector<std::shared_ptr<proto::Transaction>>>
            TwoPC::txn_phase_map;

    concurrent_unordered_map<std::string, std::string>
           TwoPC::row_map_csn;      /// tid, csn

    concurrent_unordered_map<std::string, std::string>
           TwoPC::row_map_data;
    BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>> TwoPC::prepare_lock_queue, TwoPC::commit_unlock_queue;
    concurrent_crdt_unordered_map<std::string, std::string, std::string> TwoPC::abort_txn_set;


    // 事务发送到client初始化处理
  void TwoPC::ClientTxn_Init() {

    // txn_state_struct 记录当前事务的分片个数，完成个数
    totalTxnNumber.fetch_add(1);
    bool res = false;
    // avoid txn have the same csn
    do{
        txn_ptr->set_csn(now_to_us());
        txn_ptr->set_txn_server_id(ctx.taasContext.txn_node_ip_index);
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        res = txn_state_map.insertState(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                                   client_txn));
    } while (!res);
  }

  // 事务进行分片
  bool TwoPC::Shard_2PL() {
//      LOG(INFO)<<"Shard_2PL() -- tid : " << tid;
    // 分片
    std::vector<std::shared_ptr<proto::Transaction>> shard_row_vector;
    for (uint64_t i = 0; i < shard_num; i++) {
      shard_row_vector.emplace_back(std::make_unique<proto::Transaction>());
      shard_row_vector[i]->set_csn(txn_ptr->csn());
      shard_row_vector[i]->set_txn_server_id(txn_ptr->txn_server_id());
      shard_row_vector[i]->set_client_ip(txn_ptr->client_ip());
      shard_row_vector[i]->set_client_txn_id(txn_ptr->client_txn_id());
      shard_row_vector[i]->set_shard_id(i);
    }
    // 为创建的事务添加子事务行
    for (auto i = 0; i < txn_ptr->row_size(); i++) {
      const auto& row = txn_ptr->row(i);
      auto row_ptr = shard_row_vector[GetHashValue(row.key())]->add_row();
      (*row_ptr) = row;
    }
    tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
    txn_phase_map.insert(tid, shard_row_vector);

    // send to participants
    for (uint64_t i = 0; i < shard_num; i++) {
        Send(ctx, epoch, i, * shard_row_vector[i], proto::TxnType::RemoteServerTxn);
    }
    return true;
  }

  // 2PL 上锁 abort if locked by other txn
  bool TwoPC::Two_PL_LOCK(proto::Transaction& txn) {
    // 上锁完成返回到coordinator
    tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.txn_server_id());
    GetKeySorted(txn);

    std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
//    if (txn_state_map.get_value_or_empty(tid, txn_state_struct) && txn_state_struct->txn_state == abort_txn) return false;
    if (abort_txn_set.contain(tid)) return false;
    std::atomic<uint64_t> key_lock_num = 0;

    for (auto iter = key_sorted.begin(); iter != key_sorted.end(); iter++) {
        /// read needs lock
        std::string holder_tid;
//        if (txn_state_map.get_value_or_empty(tid, txn_state_struct) && txn_state_struct->txn_state == abort_txn) break;
        if (abort_txn_set.contain(tid)) break;
        auto res = row_lock_map.try_lock(iter->first, tid);
        if (res) {
            key_lock_num.fetch_add(1);
        } else {
            lockFailed.fetch_add(1);
            Two_PL_UNLOCK(txn);
            return false;
        }
        while(false && !abort_txn_set.contain(tid)) {
            auto res = row_lock_map.try_lock_wound_wait(iter->first, tid, holder_tid);
//            auto res = row_lock_map.try_lock(iter->first, tid);
            if ( res == 1 ) {
                key_lock_num.fetch_add(1);
                break;
            } else if (res == 2) {
                abort_txn_set.insert(holder_tid, holder_tid);
                key_lock_num.fetch_add(1);
                break;
            } else {
//                lockFailed.fetch_add(1);
//                return false;
            }
        }
    }

//    if (abort_txn_set.contain(tid) || txn_state_map.get_value_or_empty(tid, txn_state_struct) && txn_state_struct->txn_state == abort_txn) {
//      Two_PL_UNLOCK(txn);
//      return false;
//    }

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

      for (auto iter = key_sorted.begin(); iter != key_sorted.end(); iter++) {
//          while (!row_lock_map.try_lock(iter->first, tid)){ } /// spinlock never block
          while (true) {
              if (!row_lock_map.try_lock(iter->first, tid)) {
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
    for (auto iter = key_sorted.begin(); iter != key_sorted.end(); iter++) {
      row_lock_map.unlock(iter->first, tid);
    }
    return true;
  }

  // 检查2PL阶段是否完成
  bool TwoPC::Check_2PL_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct) {
//    std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
//    txn_state_map.getValue(tid, txn_state_struct);
    return txn_state_struct->two_pl_failed_num.load() == 0;
//    return txn_state_struct->two_pl_num.load() == txn_state_struct->txn_shard_num;
  }

  // 检查2PC prepare是否完成
  bool TwoPC::Check_2PC_Prepare_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct) {
      return txn_state_struct->two_pc_prepare_failed_num.load() == 0;
//    return txn_state_struct->two_pc_prepare_num.load() == txn_state_struct->txn_shard_num;
  }

  // 检查2PC commit是否完成
  bool TwoPC::Check_2PC_Commit_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct) {
      return txn_state_struct->two_pc_commit_failed_num.load() == 0;
//    return txn_state_struct->two_pc_commit_num.load() == txn_state_struct->txn_shard_num;
  }

  // 发送事务给指定applicant、coordinator
  // to_whom 为编号
  // TODO: send to other nodes use send_to_server_queue
  bool TwoPC::Send(const Context& ctx, uint64_t& epoch, uint64_t& to_whom, proto::Transaction& txn,
                   proto::TxnType txn_type) {
    if (to_whom == ctx.taasContext.txn_node_ip_index){
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = txn;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str = std::string();
        Gzip(msg.get(), &serialized_txn_str);
        void *data = static_cast<void *>(const_cast<char *>(serialized_txn_str.data()));
        MessageQueue::listen_message_epoch_queue->enqueue(
                std::make_unique<zmq::message_t>(data, serialized_txn_str.size()));
        return MessageQueue::listen_message_epoch_queue->enqueue(nullptr);
    }
    auto msg = std::make_unique<proto::Message>();
    auto* txn_temp = msg->mutable_txn();
    *(txn_temp) = txn;
    txn_temp->set_txn_type(txn_type);
    auto serialized_txn_str_ptr = std::make_unique<std::string>();
    Gzip(msg.get(), serialized_txn_str_ptr.get());

    assert(!serialized_txn_str_ptr->empty());
    // printf("send thread  message epoch %d server_id %lu type %d\n", 0, to_whom, txn_type);
    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(
        to_whom, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr), nullptr));
    return MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(
        to_whom, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
  }

  // 发送给client abort/commit
  // TODO: send to client use send_to_client_queue
  bool TwoPC::SendToClient(const Context& ctx, proto::Transaction& txn, proto::TxnType txn_type,
                           proto::TxnState txn_state) {
    // commit/abort时发送
    // 不是本地事务不进行回复
      if (txn.txn_server_id() != ctx.taasContext.txn_node_ip_index) return true;
      uint64_t currTxnTime = now_to_us() - txn.csn();

      finishedTxnNumber.fetch_add(1);
      lags_.store(totalTxnNumber.load() - finishedTxnNumber.load());
      if (txn_state == proto::TxnState::Commit){
          successTxnNumber.fetch_add(1);
          successTime.fetch_add(currTxnTime);
          totalTime.fetch_add(currTxnTime);
          if ((successTxnNumber.load() + failedTxnNumber.load()) % 100 == 0 || lags_ > 200) OUTPUTLOG("============= 2PC + 2PL INFO =============", currTxnTime);
      } else {
          failedTxnNumber.fetch_add(1);
          failedTime.fetch_add(currTxnTime);
          totalTime.fetch_add(currTxnTime);
          if ((successTxnNumber.load() + failedTxnNumber.load()) % 100 == 0 || lags_ > 200) OUTPUTLOG("============= 2PC + 2PL INFO =============", currTxnTime);
      }
      // only coordinator can send to client

    // 设置txn的状态并创建proto对象
    txn.set_txn_state(txn_state);
    auto msg = std::make_unique<proto::Message>();
    auto rep = msg->mutable_reply_txn_result_to_client();
    rep->set_txn_state(txn_state);
    rep->set_client_txn_id(txn.client_txn_id());

    // 将Transaction使用protobuf进行序列化，序列化的结果在serialized_txn_str_ptr中
    auto serialized_txn_str_ptr = std::make_unique<std::string>();
    Gzip(msg.get(), serialized_txn_str_ptr.get());

    // free txn_state_map and txn_phase_map
    // CleanTxnState(txn_ptr);

    // 将序列化的Transaction放到send_to_client_queue中，等待发送给client
    MessageQueue::send_to_client_queue->enqueue(std::make_unique<send_params>(
        txn.client_txn_id(), txn.csn(), txn.client_ip(), txn.commit_epoch(), txn_type,
        std::move(serialized_txn_str_ptr), nullptr));
    return MessageQueue::send_to_client_queue->enqueue(
        std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
  }

  // static 初始化哪些
  bool TwoPC::Init(const Taas::Context& ctx_, uint64_t id) {
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
    return true;
  }

  // 处理接收到的消息
  // change queue to listen_message_epoch_queue
  bool TwoPC::HandleClientMessage() {
      while(!EpochManager::IsTimerStop()) {
//          if (MessageQueue::listen_message_txn_queue->try_dequeue(message_ptr)) {
////              LOG(INFO) << "receive a client txn message";
//              if (message_ptr == nullptr || message_ptr->empty()) continue;
//              message_string_ptr = std::make_unique<std::string>(
//                      static_cast<const char *>(message_ptr->data()), message_ptr->size());
//              msg_ptr = std::make_unique<proto::Message>();
//              res = UnGzip(msg_ptr.get(), message_string_ptr.get());
//              assert(res);
//              if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
//                  txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
//                  SetMessageRelatedCountersInfo();
//                  HandleReceivedTxn();
//              } else {
//                  MessageQueue::request_queue->enqueue(std::move(msg_ptr));
//                  MessageQueue::request_queue->enqueue(nullptr);
//              }
//          } else {
//              usleep(50);
//          }

        // TODO: receive from client use listen_message_txn_queue
          MessageQueue::listen_message_txn_queue->wait_dequeue(message_ptr);
          if (message_ptr == nullptr || message_ptr->empty()) continue;
          message_string_ptr = std::make_unique<std::string>(
                  static_cast<const char *>(message_ptr->data()), message_ptr->size());
          msg_ptr = std::make_unique<proto::Message>();
          res = UnGzip(msg_ptr.get(), message_string_ptr.get());
          assert(res);
          if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
              txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
              SetMessageRelatedCountersInfo();
              HandleReceivedTxn();
          } else {
              MessageQueue::request_queue->enqueue(std::move(msg_ptr));
              MessageQueue::request_queue->enqueue(nullptr);
          }
      }
      return true;
  }

  // 处理接收到的消息 from server
  // change queue to listen_message_epoch_queue
  bool TwoPC::HandleReceivedMessage() {
    while(!EpochManager::IsTimerStop()) {
//        if (MessageQueue::listen_message_epoch_queue->try_dequeue(message_ptr)) {
////            LOG(INFO) << "receive a server message";
//            if (message_ptr == nullptr || message_ptr->empty()) continue;
//            message_string_ptr = std::make_unique<std::string>(
//                    static_cast<const char *>(message_ptr->data()), message_ptr->size());
//            msg_ptr = std::make_unique<proto::Message>();
//            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
//            assert(res);
//            if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
//                txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
//                SetTxnState(*txn_ptr);
//                SetMessageRelatedCountersInfo();
//                HandleReceivedTxn();
//            } else {
//                MessageQueue::request_queue->enqueue(std::move(msg_ptr));
//                MessageQueue::request_queue->enqueue(nullptr);
//            }
//        } else {
//            usleep(50);
//        }

        // TODO: receive from other nodes use listen_message_epoch_queue
        MessageQueue::listen_message_epoch_queue->wait_dequeue(message_ptr);
        if (message_ptr == nullptr || message_ptr->empty()) continue;
        message_string_ptr = std::make_unique<std::string>(
                static_cast<const char *>(message_ptr->data()), message_ptr->size());
        msg_ptr = std::make_unique<proto::Message>();
        res = UnGzip(msg_ptr.get(), message_string_ptr.get());
        assert(res);
        if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
            txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
            SetTxnState(*txn_ptr);
            SetMessageRelatedCountersInfo();
            HandleReceivedTxn();
        } else {
            MessageQueue::request_queue->enqueue(std::move(msg_ptr));
            MessageQueue::request_queue->enqueue(nullptr);
        }
    }
    return true;
  }

    // 核心：根据txn type操作
  bool TwoPC::HandleReceivedTxn() {
//      if (txn_ptr->txn_type() == proto::TxnType::ClientTxn) {
//          ClientTxn_Init();
//          Shard_2PL();       // shard and send to remote servers
//          return true;
//      }
        curr_time = now_to_us();
        if (curr_time - pre_time >= 1000) {
            pre_time = curr_time;
            OUTPUTLOG("============= 2PC + 2PL INFO =============", curr_time);
        }
    switch (txn_ptr->txn_type()) {
      case proto::TxnType::ClientTxn: {
        ClientTxn_Init();
        Shard_2PL();
        break;
      }
      case proto::TxnType::RemoteServerTxn: {
          PrepareLockueueEnqueue(epoch_mod, txn_ptr);

//        if (Two_PL_LOCK(*txn_ptr)) {
//          // 发送lock ok
//         auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
//         Send(ctx, epoch, to_whom, *txn_ptr, proto::TxnType::Lock_ok);
//        } else {
//          // 发送lock abort
//          auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
//          Send(ctx, epoch, to_whom, *txn_ptr, proto::TxnType::Lock_abort);
//        }
        break;
      }
      case proto::TxnType::Lock_ok: {
        // 修改元数据
          if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
              tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
              std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
              txn_state_map.getValue(tid, txn_state_struct);
              if (txn_state_struct == nullptr) {
                  break;
                  txn_state_map.insert(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0,
                                                                                        client_txn));
                  txn_state_map.getValue(tid, txn_state_struct);
              }

              txn_state_struct->two_pl_reply.fetch_add(1);
              txn_state_struct->two_pl_num.fetch_add(1);

              // 所有应答收到
              if (txn_state_struct->two_pl_reply.load() == txn_state_struct->txn_shard_num) {
                  if (Check_2PL_complete(*txn_ptr, txn_state_struct)) {
                      // 2pl完成，开始2pc prepare阶段
                      std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
                      txn_phase_map.getValue(tid,tmp_vector);
                      if (tmp_vector.empty()) return true;  // if already send to client
                      for (uint64_t i = 0; i < shard_num; i++) {
                          auto to_whom = tmp_vector[i]->shard_id();
                          Send(ctx, epoch, to_whom, *tmp_vector[i], proto::TxnType::Prepare_req);
                      }
                  } else {
                      // do nothing
                      // 统一处理abort
//                      for (uint64_t i = 0; i < shard_num; i++) {
//                          // the unlock request is handled by other threads
//                          Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
//                      }
//                      // 发送abort给client
//                      SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
                        // clean state
//                      LOG(INFO) << "************** Lock abort : "<< tid << " **************";
                      std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
                      txn_phase_map.getValue(tid, tmp_vector);
                      if (tmp_vector.empty()) return true;
                      for (uint64_t i = 0; i < shard_num; i++) {
                          auto to_whom = tmp_vector[i]->shard_id();
                          Send(ctx, epoch, to_whom, *tmp_vector[i], proto::TxnType::Abort_txn);
                      }
                      // 发送abort给client
                      SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
                  }
              }
          }
        break;
      }
      case proto::TxnType::Lock_abort: {
        // 直接发送abort
          if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
              tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
              std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
              txn_state_map.getValue(tid, txn_state_struct);
              if (txn_state_struct == nullptr) {
                  break;
                  txn_state_map.insert(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0,
                                                                                        client_txn));
                  txn_state_map.getValue(tid, txn_state_struct);
              }

              txn_state_struct->two_pl_reply.fetch_add(1);
              txn_state_struct->two_pl_failed_num.fetch_add(1);

//              LOG(INFO) << "************** Lock abort : "<< tid << " **************";
              if (txn_state_struct->two_pl_reply.load() == txn_state_struct->txn_shard_num) {
                  std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
                  txn_phase_map.getValue(tid, tmp_vector);
                  if (tmp_vector.empty()) return true;
                  for (uint64_t i = 0; i < shard_num; i++) {
                      auto to_whom = tmp_vector[i]->shard_id();
                      Send(ctx, epoch, to_whom, *tmp_vector[i], proto::TxnType::Abort_txn);
                  }
                  // 发送abort给client
                  SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
              }
          }
        break;
      }
      case proto::TxnType::Prepare_req: {
//          LOG(INFO) << "receive Prepare_req";
        // 日志操作等等，总之返回Prepare_ok
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
//        if (abort_txn_set.contain(tid)) Send(ctx, epoch, to_whom, *txn_ptr, proto::TxnType::Prepare_abort);

        Send(ctx, epoch, to_whom, *txn_ptr, proto::TxnType::Prepare_ok);
        break;
      }
      case proto::TxnType::Prepare_ok: {
        // 修改元数据
          if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
              tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
              std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
              txn_state_map.getValue(tid, txn_state_struct);
              if (txn_state_struct == nullptr) {
                  break;
                  txn_state_map.insert(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0,
                                                                                        client_txn));
                  txn_state_map.getValue(tid, txn_state_struct);
              }
              txn_state_struct->two_pc_prepare_reply.fetch_add(1);
              txn_state_struct->two_pc_prepare_num.fetch_add(1);

              // 当所有应答已经收到
              if (txn_state_struct->two_pc_prepare_reply.load() == txn_state_struct->txn_shard_num) {
                  if (Check_2PC_Prepare_complete(*txn_ptr, txn_state_struct)) {
                      std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
                      txn_phase_map.getValue(tid,tmp_vector);
                      if (tmp_vector.empty()) return true;
                      for (uint64_t i = 0; i < shard_num; i++) {
                          auto to_whom = tmp_vector[i]->shard_id();
                          Send(ctx, epoch, to_whom, *tmp_vector[i], proto::TxnType::Commit_req);
                      }
                  } else {
                      // 统一处理abort
                      // do nothing
//                      for (uint64_t i = 0; i < shard_num; i++) {
//                          // Send(ctx, shard_row_vector[i], proto::TxnType::Abort_txn);
//                          Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
//                      }
//                      // 发送abort给client
//                      SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
//                      clean state

                      std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
                      txn_phase_map.getValue(tid,tmp_vector);
                      if (tmp_vector.empty()) return true;
                      for (uint64_t i = 0; i < shard_num; i++) {
                          auto to_whom = tmp_vector[i]->shard_id();
                          Send(ctx, epoch, to_whom, *tmp_vector[i], proto::TxnType::Abort_txn);
                      }
                      // 发送abort给client
                      SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
                  }
              }
          }
        break;
      }
      case proto::TxnType::Prepare_abort: {
        // 修改元数据, no need
        // 直接发送abort
          if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
              tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
              std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
              txn_state_map.getValue(tid, txn_state_struct);
              if (txn_state_struct == nullptr) {
                  break;
                  txn_state_map.insert(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0,
                                                                                        client_txn));
                  txn_state_map.getValue(tid, txn_state_struct);
              }
              txn_state_struct->two_pc_prepare_reply.fetch_add(1);
              txn_state_struct->two_pc_prepare_failed_num.fetch_add(1);

              std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
              txn_phase_map.getValue(tid,tmp_vector);
              if (tmp_vector.empty()) return true;
              for (uint64_t i = 0; i < shard_num; i++) {
                  auto to_whom = tmp_vector[i]->shard_id();
                  Send(ctx, epoch, to_whom, *tmp_vector[i], proto::TxnType::Abort_txn);
              }
              // 发送abort给client
              SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
          }
        break;
      }
      case proto::TxnType::Commit_req: {
        // 日志操作等等，总之返回Commit_ok
//          LOG(INFO) << "receive Commit_req";
        Two_PL_UNLOCK(*txn_ptr);    // unlock

        auto to_whom = static_cast<uint64_t >(txn_ptr->txn_server_id());
        Send(ctx, epoch, to_whom, *txn_ptr, proto::TxnType::Commit_ok);
        if (txn_ptr->txn_server_id() != ctx.taasContext.txn_node_ip_index) CleanTxnState(txn_ptr);
        break;
      }
      case proto::TxnType::Commit_ok: {
          if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
              // 与上相同
              // 修改元数据
              tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
              std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
              txn_state_map.getValue(tid, txn_state_struct);
              if (txn_state_struct == nullptr) {
                  break;
                  txn_state_map.insert(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0,
                                                                                        client_txn));
                  txn_state_map.getValue(tid, txn_state_struct);
              }
              txn_state_struct->two_pc_commit_reply.fetch_add(1);
              txn_state_struct->two_pc_commit_num.fetch_add(1);

              // 当所有应答已经收到，并且commit阶段未完成
              if (txn_state_struct->two_pc_commit_reply.load() == txn_state_struct->txn_shard_num) {
                  if (Check_2PC_Commit_complete(*txn_ptr, txn_state_struct)) {
                      std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
                      txn_phase_map.getValue(tid, tmp_vector);
                      if (tmp_vector.empty()) return true;
                      // no need to notify participants
                      txn_state_struct->txn_state = commit_done;
                      SendToClient(ctx, *txn_ptr, proto::TxnType::CommittedTxn, proto::TxnState::Commit);

                      if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
                          txn_ptr->set_commit_epoch(EpochManager::GetPushDownEpoch());
                          RedoLoger::RedoLog(thread_id, txn_ptr);
                      }
                      UpdateReadSet(*txn_ptr);
                      CleanTxnState(txn_ptr);
                  } else {
                      // 统一处理abort
                      // do nothing
//                      for (uint64_t i = 0; i < shard_num; i++) {
//                          Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
//                      }
//                      SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
//                      clean state
                  }
              }
          }
        break;
      }
      case proto::TxnType::Commit_abort: {
        // 直接发送abort
          if (txn_ptr->txn_server_id() == ctx.taasContext.txn_node_ip_index) {
              tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
              std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
              txn_state_map.getValue(tid, txn_state_struct);
              if (txn_state_struct == nullptr) {
                  break;
                  txn_state_map.insert(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(shard_num, 0, 0, 0, 0, 0, 0,
                                                                                        client_txn));
                  txn_state_map.getValue(tid, txn_state_struct);
              }
              txn_state_struct->two_pc_commit_reply.fetch_add(1);
              txn_state_struct->two_pc_commit_failed_num.fetch_add(1);

              std::vector<std::shared_ptr<proto::Transaction>> tmp_vector;
              txn_phase_map.getValue(tid, tmp_vector);
              if (tmp_vector.empty()) return true;
              // no need to notify participants
              // 发送abort给client
              SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
          }
        break;
      }
      case proto::TxnType::Abort_txn: {
          tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
          Two_PL_UNLOCK(*txn_ptr);
          CleanTxnState(txn_ptr);
          break;
      }
      default:
        break;
    }

    SetMessageRelatedCountersInfo();

    return true;
  }

  // 修改哪些元数据
  bool TwoPC::SetMessageRelatedCountersInfo() {
    message_server_id = txn_ptr->txn_server_id();
    txn_ptr->shard_id();
    return true;
  }

  void TwoPC::OUTPUTLOG(const std::string& s, uint64_t time){
      LOG(INFO) << s.c_str() <<
                "\ntotalTxnNumber: " << totalTxnNumber.load() << "\t\t\t disstance" << lags_ << "\t\t\tfailedTxnNumber: " << failedTxnNumber.load() <<"\t\t\tsuccessTxnNumber: " << successTxnNumber.load() <<
                "\nlockFailed: " << lockFailed.load() << "\t\t\tvalidateFailed: " << validateFailed.load() << "\t\t\tcurTxnTime: " << time <<
                "\n[TotalTxn] avgTotalTxnTime: " << totalTime/totalTxnNumber << "\t\t\t totalTime: " << totalTime.load() <<
                "\n[SuccessTxn] avgSuccessTxnTime: " << (successTxnNumber.load() == 0 ? "Nan": std::to_string(successTime.load()/successTxnNumber.load())) << "\t\t\t successTime: " << successTime.load() <<
                "\n[FailedTxn] avgFailedTxnTime: " << (failedTxnNumber.load() == 0 ? "Nan": std::to_string(failedTime.load()/failedTxnNumber.load())) << "\t\t\t failedTime: " << failedTime.load() <<
                "\n[Lock] lockedRowCount: " << row_lock_map.countLock() <<
                "\n************************************************ ";

  }

  // handle lock requests
  void TwoPC::PrepareLockThread() {
    std::shared_ptr<proto::Transaction> local_txn_ptr;
    bool sleep_flag;
    LOG(INFO) << "PrepareLockThread starts";
    while (!EpochManager::IsTimerStop()) {
          sleep_flag = true;
          while(TwoPC::prepare_lock_queue.try_dequeue(local_txn_ptr)){
              sleep_flag = true;

//              if (local_txn_ptr != nullptr && local_txn_ptr->txn_type() == proto::TxnType::NullMark) {
                if (local_txn_ptr != nullptr && local_txn_ptr->txn_type() == proto::TxnType::RemoteServerTxn) {
                  if (Two_PL_LOCK(*local_txn_ptr)) {
                      // 发送lock ok
                      auto to_whom = static_cast<uint64_t >(local_txn_ptr->txn_server_id());
                      Send(ctx, epoch, to_whom, *local_txn_ptr, proto::TxnType::Lock_ok);
                  } else {
                      // 发送lock abort
                      auto to_whom = static_cast<uint64_t >(local_txn_ptr->txn_server_id());
                      Send(ctx, epoch, to_whom, *local_txn_ptr, proto::TxnType::Lock_abort);
                  }
                }
              local_txn_ptr.reset();
              sleep_flag = false;
          }
        if (sleep_flag) usleep(200);
    }
  }

  void TwoPC::CommitUnlockThread() {

  }

}  // namespace Taas
