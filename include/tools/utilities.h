//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_UTILITIES_H
#define TAAS_UTILITIES_H

#pragma once

#include "proto/message.pb.h"
#include "google/protobuf/io/gzip_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

#include <stdlib.h>
#include <iostream>
#include <glog/logging.h>

namespace Taas {
#define UNUSED_VALUE(v) (void)(v);

    bool Gzip(google::protobuf::MessageLite* ptr, std::string* serialized_str_ptr);

    bool UnGzip(google::protobuf::MessageLite* ptr, const std::string* str);

    void SetCPU();

    uint64_t now_to_us();
}



#endif //TAAS_UTILITIES_H
