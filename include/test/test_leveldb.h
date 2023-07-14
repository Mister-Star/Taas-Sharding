#ifndef TAAS_TEST_LEVELDB_H
#define TAAS_TEST_LEVELDB_H

#pragma once

#include "tools/context.h"
#include "tools/utilities.h"
#include "epoch/epoch_manager.h"

#include <cstdlib>

namespace Taas {
    void LevelDB_Client(const Context& ctx, uint64_t id);
}
#endif //TAAS_TEST_LEVELDB_H