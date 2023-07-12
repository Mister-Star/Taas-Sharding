#include "test/test_leveldb.h"
#include "transaction/merge.h"
#include <random>
#include <brpc/channel.h>
#include <proto/transaction.pb.h>
#include "proto/kvdb_server.pb.h"

namespace Taas {

    void LevelDB_Client(const Context& ctx, uint64_t id){
    
        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;

        // 创建随机数生成器
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(1, 100);
        
        if (channel.Init(ctx.kLevevDBIP.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
            return -1;
        }

        proto::KvDBPutService_Stub put_stub(&channel);
        proto::KvDBGetService_Stub get_stub(&channel);

        // 调用put
        proto::KvDBRequest put_Request;
        proto::KvDBResponse put_Response;
        brpc::Controller put_Cntl;

        auto data = put_Request.add_data();

        int rand_num = dis(gen);
        data->set_key("hello" + std::to_string(rand_num));
        data->set_value("world" + std::to_string(rand_num));

        put_stub.Put(&put_Cntl, &put_Request, &put_Response, NULL);
        if (!put_Cntl.Failed()) {
            LOG(INFO) << "Received response from " << put_Cntl.remote_side()
                << " to " << put_Cntl.local_side()
                << ": " << put_Response.result() << " (attached="
                << put_Cntl.response_attachment() << ")"
                << " latency=" << put_Cntl.latency_us() << "us";
        } else {
            LOG(WARNING) << put_Cntl.ErrorText();
        }
        

        // get请求
        proto::KvDBRequest get_Request;
        proto::KvDBResponse get_Response;
        brpc::Controller get_Cntl;
        get_Request.set_key("hello" + std::to_string(rand_num));

        get_stub.Get(&get_Cntl, &get_Request, &get_Response, NULL);
        if (!get_Cntl.Failed()) {
            LOG(INFO) << "Get success: " << get_Response.success();
            if (get_Response.success()) {
            LOG(INFO) << "Value: " << get_Response.value();
            }
        } else {
            LOG(ERROR) << get_cntl.ErrorText();
        }


        LOG(INFO) << "LevelDBClient is going to quit";
        return 0;
    }


}