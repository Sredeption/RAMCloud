#ifndef RAMCLOUD_MIGRATIONTARGETMANAGER_H
#define RAMCLOUD_MIGRATIONTARGETMANAGER_H

#include "Common.h"
#include "Key.h"
#include "RangeList.h"

namespace RAMCloud {


class MigrationTargetManager {
  PRIVATE:

    class Migration {
        Migration(uint64_t migrationId);

      PRIVATE:
        uint64_t migrationId;
        RangeList rangeList;
        friend MigrationTargetManager;
        DISALLOW_COPY_AND_ASSIGN(Migration);
    };

    std::unordered_map<uint64_t, Migration *> migrations;
    SpinLock lock;
  PUBLIC:

    MigrationTargetManager();

    void init(uint64_t migrationId);

    bool isLocked(uint64_t migrationId, Key &key);

    void update(uint64_t migrationId,
                vector<WireFormat::MigrationIsLocked::Range> &ranges);

};

}


#endif //RAMCLOUD_MIGRATIONTARGETMANAGER_H
