#ifndef RAMCLOUD_MIGRATIONTARGETMANAGER_H
#define RAMCLOUD_MIGRATIONTARGETMANAGER_H

#include "Common.h"
#include "Key.h"

namespace RAMCloud {


class MigrationTargetManager {
  PRIVATE:

    class Migration {
        uint64_t migrationId;
    };

  PUBLIC:

    void init(uint64_t migrationId);

    bool isLocked(uint64_t migrationId, Key &key);

};

}


#endif //RAMCLOUD_MIGRATIONTARGETMANAGER_H
