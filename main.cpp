//
// Created by 周慰星 on 11/8/22.
//

#include "epoch/epoch_manager.h"
#include "worker/worker_epoch_manager.h"
#include "worker/worker_epoch_merge.h"
#include "worker/worker_message.h"
#include "worker/worker_storage.h"

#include "leveldb_server/leveldb_server.h"
#include "storage/tikv.h"
#include "test/test.h"

#include <glog/logging.h>

#include <iostream>
#include <thread>
using namespace std;

namespace Taas {
    int main() {
        // 读取配置信息
        Context ctx("../TaaS_config.xml", "../Storage_config.xml");

        // 初始化glog日志库
        FLAGS_log_dir = ctx.glog_path_;
        FLAGS_alsologtostderr = true;
        google::InitGoogleLogging("Taas-sharding");
        LOG(INFO) << "System Start\n";
        // 存储工作线程
        std::vector<std::unique_ptr<std::thread>> threads;

        // 判断服务器类型
        if(ctx.server_type == 1) { ///TaaS servers
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            // 启动工作线程
            // 工作线程执行WorkerForPhysicalThreadMain函数，该函数接受一个Context对象作为参数
            threads.push_back(std::make_unique<std::thread>(WorkerForPhysicalThreadMain, ctx));

//        threads.push_back(std::make_unique<std::thread>(WorkerForLogicalThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalTxnMergeCheckThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalAbortSetMergeCheckThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalCommitCheckThreadMain, ctx));
//        threads.push_back(std::make_unique<std::thread>(WorkerForLogicalRedoLogPushDownCheckThreadMain, ctx));

            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalReceiveAndReplyCheckThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochAbortSendThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochEndFlagSendThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochBackUpEndFlagSendThreadMain, ctx));

//        for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
            for(int i = 0; i < 16; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroTxnMessageThreadMain, ctx, i));///txn message
                threads.push_back(std::make_unique<std::thread>(WorkerFroMergeThreadMain, ctx, i));///merge
                threads.push_back(std::make_unique<std::thread>(WorkerFroCommitThreadMain, ctx, i));///commit
            }

            threads.push_back(std::make_unique<std::thread>(WorkerForClientListenThreadMain, ctx));///client
            threads.push_back(std::make_unique<std::thread>(WorkerForClientSendThreadMain, ctx));
            
            if(ctx.kTxnNodeNum > 1) {
                for(int i = 0; i < 16; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroEpochMessageThreadMain, ctx, i));///Epoch message
                }
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain, ctx));
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain_Epoch, ctx));
                threads.push_back(std::make_unique<std::thread>(WorkerForServerSendThreadMain, ctx));
            }
            // 如果启用TiKV，创建客户端并启动工作线程
            if(ctx.is_tikv_enable) {
                TiKV::tikv_client_ptr = new tikv_client::TransactionClient({ctx.kTiKVIP});
//            for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, ctx, 0));///tikv push down
//            }
            }
            // 启动MOT存储线程
            threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain)); ///mot push down


            for(int i = 0; i < (int)ctx.kTestClientNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(Client, ctx, i));
            }
        }
        else if(ctx.server_type == 2) { ///leveldb server

            ///todo : add brpc

            LevelDBServer(ctx);

        }
        else if(ctx.server_type == 3) { ///hbase server

        }



        if(ctx.kDurationTime_us != 0) {
            while(!test_start.load()) usleep(sleep_time);
            usleep(ctx.kDurationTime_us);
            EpochManager::SetTimerStop(true);
        }
        for(auto &i : threads) {
            i->join();
        }

        std::cout << "============================================================================" << std::endl;
        std::cout << "=====================              END                 =====================" << std::endl;
        std::cout << "============================================================================" << std::endl;

        return 0;
    }

}

int main() {
    Taas::main();
}