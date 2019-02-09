#ifndef RAMCLOUD_MIGRATIONTARGETMANAGER_H
#define RAMCLOUD_MIGRATIONTARGETMANAGER_H

#include "Common.h"
#include "Key.h"
#include "RangeList.h"

namespace RAMCloud {


class MigrationTargetManager {
  PRIVATE:

    class Migration {
      PUBLIC:

        Migration(uint64_t migrationId, uint64_t tableId, uint64_t firstKeyHash,
                  uint64_t lastKeyHash, uint64_t sourceId, uint64_t targetId);

        void finish();

      PRIVATE:
        uint64_t migrationId;
        RangeList rangeList;
        uint64_t tableId;
        uint64_t firstKeyHash;
        uint64_t lastKeyHash;
        uint64_t sourceId;
        uint64_t targetId;
        friend MigrationTargetManager;
        DISALLOW_COPY_AND_ASSIGN(Migration);
    };

    MasterService *masterService;
    std::unordered_map<uint64_t, Migration *> migrations;
    SpinLock lock;

    struct FinishNotifier {
        virtual void notify(uint64_t migrationId) = 0;

        virtual ~FinishNotifier()
        {}
    };

    struct RealFinishNotifier : public FinishNotifier {

        void notify(uint64_t migrationId);

        virtual ~RealFinishNotifier()
        {}
    };

    DISALLOW_COPY_AND_ASSIGN(MigrationTargetManager);
  PUBLIC:

    MigrationTargetManager(MasterService *masterService);

    void startMigration(uint64_t migrationId, uint64_t tableId,
                        uint64_t firstKeyHash, uint64_t lastKeyHash,
                        uint64_t sourceId, uint64_t targetId);

    void finishMigration(uint64_t migrationId, bool successful);

    bool isLocked(uint64_t migrationId, Key &key);

    void update(uint64_t migrationId,
                vector<WireFormat::MigrationIsLocked::Range> &ranges);

    Migration *getMigration(uint64_t migrationId);

    bool disableMigrationRecover;
};

}


#endif //RAMCLOUD_MIGRATIONTARGETMANAGER_H
