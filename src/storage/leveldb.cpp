//
// Created by zwx on 23-6-30.
//

#include <queue>
#include "storage/leveldb.h"
#include "epoch/epoch_manager.h"

namespace Taas {
    Context LevelDB::ctx;
    AtomicCounters_Cache
            LevelDB::epoch_should_push_down_txn_num, 
            LevelDB::epoch_pushed_down_txn_num;
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> 
            LevelDB::task_queue,        // 存储任务队列
            LevelDB::redo_log_queue;    // 存储重做日志
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>> 
            LevelDB:: epoch_redo_log_queue; 
    std::vector<std::unique_ptr<std::atomic<bool>>> 
            LevelDB::epoch_redo_log_complete;
    brpc::Channel LevelDB::channel;     // brpc无client，只有channel

    /* 赋值ctx
    初始化task_queue,redo_log_queue...
    初始化channel代替client，非线程安全
    */ 
    void LevelDB::StaticInit(const Context &ctx_){
        
        ctx = ctx_;
        epoch_should_push_down_txn_num.Init(ctx.kCacheMaxLength,ctx.kTxnNodeNum);
        epoch_pushed_down_txn_num.Init(ctx.kCacheMaxLength,ctx.kTxnNodeNum);
        task_queue = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        epoch_redo_log_queue.resize(ctx.kCacheMaxLength);
        epoch_redo_log_complete.resize(ctx.kCacheMaxLength);


        for (size_t i = 0; i < ctx.kCacheMaxLength; i++){
            epoch_redo_log_queue[i] = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false); 
        }

        brpc::ChannelOptions options;   // 设置options
        // int Init(const char* server_addr_and_port, const ChannelOptions* options);
        channel.Init(ctx.kLevevDBIP.c_str(), &options); 

    }

    /* 清除所有数据
    */
    void LevelDB::StaticClear(uint64_t &epoch){
        epoch_should_push_down_txn_num.Clear(epoch);
        epoch_pushed_down_txn_num.Clear(epoch);
        epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(false);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->try_dequeue(txn_ptr));
    }

    /* 生成一个task
    should_num++? 加入task q
    */
    void LevelDB::bool GeneratePushDownTask(uint64_t &epoch){
        auto txn_ptr = std::make_unique<proto::Transaction>();
        txn_ptr->set_commit_epoch = epoch;
        task_queue->enqueue(std::move(tnx_ptr));
        task_queue->enqueue(nullptr);
        return true;
    }

    /* 睡眠一段时间自己重新变活跃，检查 
    推送queue中所有txn到leveldb

    */
    void LevelDB::SendTransactionToLevelDB_Usleep(){
        std::unique_ptr<proto::Transaction> txn_ptr;
        // 通过protobuf生成 xxx_stub 替代调用函数
        proto::KvDBPutService_Stub put_stub(&channel);
        proto::KvDBGetService_Stub get_stub(&channel);

        uint64_t epoch;
        epoch = EpochManager::GetPushDownEpoch();

        while(redo_log_queue->try_dequeue(txn_ptr)) {
            if(txn_ptr == nullptr) continue;

            proto::KvDBRequest request;
            proto::KvDBResponse response;
            brpc::Controller cntl;

            cntl.set_timeout_ms(500);
            auto csn = txn_ptr->csn();

            // Transaction的消息类型，其中包含一个名为row的repeated字段，表示当前事务中所有行
            for(auto i : txn_ptr->row()) {          // 表示当前事务中所有行
                if(i.op_type() == proto::OpType::Read) {
                    continue;
                }
                auto data = request.add_data();
                data->set_op_type(i.op_type());
                data->set_key(i.key());
                data->set_value(i.data());
                data->set_csn(csn);
                put_stub.Put(&cntl, &request, &response, NULL);

                if (cntl.Failed()) {
                    // RPC失败了. response里的值是未定义的，勿用。
                } else {
                    // RPC成功了，response里有我们想要的回复数据。
                }
            }
            epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
        
    }

    /* 阻塞？推送queue中所有txn到leveldb
    */
    void LevelDB::SendTransactionToLevelDB_Block(){
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        while(!EpochManager::IsTimerStop()) {
            redo_log_queue->wait_dequeue(txn_ptr);

            if(txn_ptr == nullptr) continue;
            epoch = txn_ptr->commit_epoch();

            proto::KvDBRequest request;
            proto::KvDBResponse response;
            brpc::Controller cntl;

            cntl.set_timeout_ms(500);
            auto csn = txn_ptr->csn();

            // Transaction的消息类型，其中包含一个名为row的repeated字段，表示当前事务中所有行
            for(auto i : txn_ptr->row()) {          // 表示当前事务中所有行
                if(i.op_type() == proto::OpType::Read) {
                    continue;
                }
                auto data = request.add_data();
                data->set_op_type(i.op_type());
                data->set_key(i.key());
                data->set_value(i.data());
                data->set_csn(csn);
                put_stub.Put(&cntl, &request, &response, NULL);

                if (cntl.Failed()) {
                    // RPC失败了. response里的值是未定义的，勿用。
                } else {
                    // RPC成功了，response里有我们想要的回复数据。
                }
            }

//            if(tikv_client_ptr == nullptr) continue;
//            auto tikv_txn = tikv_client_ptr->begin();
//            for (auto i = 0; i < txn_ptr->row_size(); i++) {
//                const auto& row = txn_ptr->row(i);
//                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
//                    tikv_txn.put(row.key(), row.data());
//                }
//            }
//            tikv_txn.commit();
            epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
    }

    /* 检定是否epoch所有txn都被推送
    小于logicalEpoch，不然num无法访问到
    */
    bool LevelDB::CheckEpochPushDownComplete(uint64_t &epoch){
        if (epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
        // 全部推送但complete还未完成修改的情况
        if (epoch < EpochManager::GetLogicalEpoch && 
            epoch_should_push_down_txn_num.GetCount(epoch) <= epoch_pushed_down_txn_num.GetCount(epoch)) {
                epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
        }
        return false;
    }


    void LevelDB::LevelDBRedoLogQueueEnqueue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr){
        epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->enqueue(std::move(txn_ptr));
        epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->enqueue(nullptr);
    }

    bool LevelDB::LevelDBRedoLogQueueTryDequeue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr){
        return epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->try_dequeue(txn_ptr);
    }


}
