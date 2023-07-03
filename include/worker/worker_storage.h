//
// Created by zwx on 23-7-3.
//

#ifndef TAAS_WORKER_STORAGE_H
#define TAAS_WORKER_STORAGE_H
#pragma once
#include "tools/context.h"

namespace Taas {

    extern void WorkerFroTiKVStorageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroMOTStorageThreadMain();

    extern void StateChecker(const Context& ctx);
}

#endif //TAAS_WORKER_STORAGE_H