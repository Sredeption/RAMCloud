#ifndef RAMCLOUD_MIGRATIONCLIENT_H
#define RAMCLOUD_MIGRATIONCLIENT_H

#include "Key.h"
#include "Tablet.h"
#include "RamCloud.h"

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

class MigrationReadTask {
  PRIVATE:
    enum State {
        INIT, NORMAL, NORMAL_WAIT, MIGRATING, MIGRATING_WAIT, DONE
    };
    RamCloud *ramcloud;
    uint64_t tableId;
    const void *key;
    uint16_t keyLength;
    Buffer *value;
    const RejectRules *rejectRules;

    Tub<ReadRpc> readRpc;
    Tub<MigrationReadRpc> sourceReadRpc, targetReadRpc;

    State state;
    Buffer sourceBuffer, targetBuffer;

    uint64_t version;
    bool objectExists;

    DISALLOW_COPY_AND_ASSIGN(MigrationReadTask)

  PUBLIC:

    MigrationReadTask(
        RamCloud *ramcloud, uint64_t tableId, const void *key,
        uint16_t keyLength, Buffer *value, const RejectRules *rejectRules);

    void performTask();

    bool isReady();

    void wait(uint64_t *version, bool *objectExists);
};

}

#endif //RAMCLOUD_MIGRATIONCLIENT_H
