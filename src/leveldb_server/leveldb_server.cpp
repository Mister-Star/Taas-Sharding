//
// Created by zwx on 23-6-30.
//


#include <future>
#include "leveldb_server/leveldb_server.h"
#include "leveldb_server/rocksdb_connection.h"


namespace Taas {

    static std::vector<std::unique_ptr<RocksDBConnection>> leveldb_connections;
    static std::atomic<uint64_t> connection_num(0);

    void LevelDBServer(const Context &context, std::promise<void> serverReady){
        brpc::Server leveldb_server;
        brpc::ServerOptions options;
        LevelDBGetService leveldb_get_service;
        LevelDBPutService leveldb_put_service;

//        leveldb_connections.resize(10001);
       leveldb_connections.resize(1);
//        for(int i = 0; i < 10; i ++) {
            LOG(INFO) << "current thread :" << 0 ;
            leveldb_connections[0] = RocksDBConnection::NewConnection("leveldb");
//        }


        if(leveldb_server.AddService(&leveldb_get_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add leveldb_get_service";
            assert(false);
        }
        if(leveldb_server.AddService(&leveldb_put_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add leveldb_put_service";
            assert(false);
        }
        if(leveldb_server.Start(context.kLevevDBIP.c_str(), &options)!=0){
            LOG(ERROR) << "Fail to start leveldb_server";
        }
        serverReady.set_value();
        leveldb_server.RunUntilAskedToQuit();

    }

    void LevelDBGetService::Get(::google::protobuf::RpcController *controller, const ::proto::KvDBRequest *request,
                         ::proto::KvDBResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        auto num = connection_num.fetch_add(1);
        std::string value;
        const auto& data = request->data();
        const std::string& key= data[0].key();
        LOG(INFO) << "get-key : " << key;
        auto res = leveldb_connections[0]->get(key, &value);
        LOG(INFO) << "get-value : " << value;
        // 填写response
        response->set_result(res);
        const auto& response_data = response->add_data();
        response_data->set_value(value);

        LOG(INFO) << "response result : " << res ;
//        KvDbGet::Get(controller, request, response, done);
//        done_guard.release();
    }

    void LevelDBPutService::Put(::google::protobuf::RpcController *controller, const ::proto::KvDBRequest *request,
                         ::proto::KvDBResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);

//        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        auto num = connection_num.fetch_add(1);
        //std::string value;
        const auto& data = request->data();
        const std::string& key = data[0].key();
        const std::string& value = data[0].value();
        LOG(INFO) << "put-key : " << key << "\tput-value : " << value;
        auto res = leveldb_connections[0]->syncPut(key, value);
        LOG(INFO) << "response result : " << res ;

        // 填写response
        response->set_result(res);
//        KvDbPut::Put(controller, request, response, done);
//        done_guard.release();
    }
}