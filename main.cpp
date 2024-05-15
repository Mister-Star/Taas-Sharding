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

#include "workload/multi_model_workload.h"
#include "workload/worker.h"

#include <glog/logging.h>

#include <iostream>
#include <thread>
#include <future>


using namespace std;

namespace Taas {

    int main() {
        Context ctx;

        FLAGS_log_dir = "/tmp";
        FLAGS_alsologtostderr = true;
        google::InitGoogleLogging("Taas-shard");
        LOG(INFO) << "System Start\n";
        auto res = ctx.taasContext.Print();
        LOG(INFO) << res;
        printf("%s\n", res.c_str());
        std::vector<std::unique_ptr<std::thread>> threads;
        int cnt = 0;
        if(ctx.taasContext.server_type == ServerMode::Taas) { ///TaaS servers
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            threads.push_back(std::make_unique<std::thread>(WorkerForPhysicalThreadMain, ctx)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalThreadMain, ctx)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalRedoLogPushDownCheckThreadMain, ctx)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochControlMessageThreadMain, ctx)); cnt++;

//            for(int i = 0; i < (int)ctx.taasContext.kEpochTxnThreadNum; i ++) {///handle client txn
//                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageThreadMain, ctx, i));  cnt++;///client txn message
//            }
//            for(int i = 0; i < (int)ctx.taasContext.kEpochMessageThreadNum; i ++) {/// handle remote server message
//                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageEpochThreadMain, ctx, i));  cnt++;///epoch message
//            }
            for(int i = 0; i < (int)ctx.taasContext.kMergeThreadNum; i ++) {
//                threads.push_back(std::make_unique<std::thread>(WorkerFroMergeThreadMain, ctx, i));  cnt++;///merge & commit
                threads.push_back(std::make_unique<std::thread>(EpochWorkerThreadMain, ctx, i));  cnt++;
            }

            threads.push_back(std::make_unique<std::thread>(WorkerForClientListenThreadMain, ctx));  cnt++;///client
            threads.push_back(std::make_unique<std::thread>(WorkerForClientSendThreadMain, ctx)); cnt++;

            threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain, ctx)); cnt++;///Server
            threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain_Epoch, ctx)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForServerSendThreadMain, ctx)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForServerSendPUBThreadMain, ctx)); cnt++;

            ///Storage
//            threads.push_back(std::make_unique<std::thread>(WorkerForStorageSendMOTThreadMain, ctx)); cnt++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForStorageSendNebulaThreadMain, ctx)); cnt++;

            if(ctx.storageContext.is_mot_enable) {
                for(int i = 0; i < (int)ctx.storageContext.kMOTThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain, ctx, i));  cnt++;///mot push down
                }
                for(int i = 0; i < (int)ctx.storageContext.kMOTThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroNebulaStorageThreadMain, ctx, i));  cnt++;///nebula push down
                }
            }
            if(ctx.storageContext.is_tikv_enable) {
                TiKV::tikv_client_ptr = new tikv_client::TransactionClient({ctx.storageContext.kTiKVIP});
                for(int i = 0; i < (int)ctx.storageContext.kTikvThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, ctx, i)); cnt++;///tikv push down
                }
            }
            if(ctx.storageContext.is_leveldb_enable) {
                threads.push_back(std::make_unique<std::thread>(LevelDBServer,ctx));
                for(int i = 0; i < (int)ctx.storageContext.kLeveldbThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroLevelDBStorageThreadMain, ctx, i)); cnt++;///tikv push down
                }
            }
            if(ctx.storageContext.is_hbase_enable) {
                for(int i = 0; i < (int)ctx.storageContext.kHbaseThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroHBaseStorageThreadMain, ctx, i)); cnt++;///tikv push down
                }
            }
//            for(int i = 0; i < 1; i ++) {
            for(int i = 0; i < (int)ctx.taasContext.kTestClientNum; i ++) {
                if(ctx.storageContext.is_leveldb_enable) {
                    LOG(INFO) << "LevelDBClient inserting";
                    threads.push_back(std::make_unique<std::thread>(LevelDBClient, ctx, i));
                    cnt++;
                }
                else {
                    threads.push_back(std::make_unique<std::thread>(Client, ctx, i));
                    cnt++;
                }
            }
        }
        else if(ctx.taasContext.server_type == ServerMode::LevelDB) { ///leveldb server
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            LevelDBServer(ctx);
        }
        else if(ctx.taasContext.server_type == ServerMode::HBase) { ///hbase server
            //do nothing
        }
        else if(ctx.taasContext.server_type == ServerMode::MultiModelClient) { ///hbase server
            workload::main();
        }



        if(ctx.taasContext.kDurationTime_us != 0) {
            while(!test_start.load()) usleep(sleep_time);
            usleep(ctx.taasContext.kDurationTime_us);
            EpochManager::SetTimerStop(true);
        }
//        else {
//            std::signal(SIGINT, signalHandler);
//        }
        for(auto &i : threads) {
            i->join();
        }
        google::ShutdownGoogleLogging();
        std::cout << "============================================================================" << std::endl;
        std::cout << "=====================              END                 =====================" << std::endl;
        std::cout << "============================================================================" << std::endl;
        return 0;
    }

}

int main() {

    Taas::Context ctx;

    auto server_num = ctx.taasContext.kTxnNodeNum,
            shard_num = ctx.taasContext.kShardNum,
            replica_num = ctx.taasContext.kReplicaNum,
            local_server_id = ctx.taasContext.txn_node_ip_index,
            max_length = ctx.taasContext.kCacheMaxLength;
    std::vector<std::vector<bool>> is_local_shard;
    is_local_shard.resize(server_num);
    for(auto &i : is_local_shard) {
        i.resize(shard_num);
    }
    for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
        for(uint64_t i = 0; i < shard_num; i ++) {
            for(uint64_t j = 0; j < replica_num; j ++ ) {
                if((i + server_num - j) % server_num == server_id) {
                    is_local_shard[server_id][i] = true;
                }
            }
        }
    }
    std::string s = "";
    for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
        for(uint64_t i = 0; i < shard_num; i ++) {
            if(is_local_shard[server_id][i]) {
                s += "1";
            }
            else {
                s += "0";
            }
        }
        s += "\n";
    }

    printf("============================\n");
    printf("shard replication statues:\n%s", s.c_str());
    printf("============================\n");
    Taas::main();
}