#ifndef RAMCLOUD_MIGRATIONCLIENT_H
#define RAMCLOUD_MIGRATIONCLIENT_H

#include "Key.h"
#include "Tablet.h"

namespace RAMCloud {

class MigrationClient {
  PRIVATE:

    struct TabletKey {
        uint64_t tableId;       // tableId of the tablet
        KeyHash keyHash;        // start key hash value

        /**
         * The operator < is overridden to implement the
         * correct comparison for the tableMap.
         */
        bool operator<(const TabletKey &key) const
        {
            return tableId < key.tableId ||
                   (tableId == key.tableId && keyHash < key.keyHash);
        }
    };

  PUBLIC:

    struct MigratingTablet {
        Tablet tablet;

        ServerId sourceId;
        ServerId targetId;

        MigratingTablet(Tablet tablet, uint64_t sourceId,
                        uint64_t targetId)
            : tablet(tablet), sourceId(sourceId),
              targetId(targetId)
        {}
    };

  PRIVATE:

    RamCloud *ramcloud;
    std::map<TabletKey, MigratingTablet> tableMap;

    DISALLOW_COPY_AND_ASSIGN(MigrationClient);
  PUBLIC:

    MigrationClient(RamCloud *ramcloud);

    void putTablet(uint64_t tableId, const void *key, uint16_t keyLength,
                   uint64_t sourceId, uint64_t targetId);

    MigratingTablet *
    getTablet(uint64_t tableId, const void *key, uint16_t keyLength);

    void removeTablet(uint64_t tableId, const void *key, uint16_t keyLength);
};

}

#endif //RAMCLOUD_MIGRATIONCLIENT_H
