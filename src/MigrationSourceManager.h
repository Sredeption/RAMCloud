#ifndef RAMCLOUD_MIGRATIONSOURCEMANAGER_H
#define RAMCLOUD_MIGRATIONSOURCEMANAGER_H

#include <queue>
#include <unordered_map>

#include "Key.h"
#include "SpinLock.h"
#include "WorkerTimer.h"
#include "TransactionManager.h"


namespace RAMCloud {


class MigrationSourceManager : public WorkerTimer {
  PRIVATE:

    enum ItemType {
        LOCK, UNLOCK
    };

    struct Item {
        ItemType type;
        uint64_t migrationId;
        uint64_t keyHash;
    };

    class Migration {
      PUBLIC:

        Migration(MigrationSourceManager *manager,
                  uint64_t migrationId,
                  uint64_t tableId,
                  uint64_t firstKeyHash,
                  uint64_t lastKeyHash,
                  uint64_t sourceId,
                  uint64_t targetId);

        void lock(uint64_t keyHash);

        void unlock(uint64_t keyHash);

        void start();

      PRIVATE:
        MigrationSourceManager *manager;
        uint64_t migrationId;
        uint64_t tableId;
        uint64_t firstKeyHash;
        uint64_t lastKeyHash;
        uint64_t sourceId;
        uint64_t targetId;
        uint64_t timestamp;
        uint64_t startEpoch;
        bool active;
        friend MigrationSourceManager;
        DISALLOW_COPY_AND_ASSIGN(Migration);
    };

    MasterService *masterService;
    TransactionManager *transactionManager;
    std::unique_ptr<std::queue<Item>[]> operatingBuffer;
    uint8_t operatingIndex;
    SpinLock bufferLock;
    std::mutex treeLock;
    std::unordered_map<uint64_t, Migration *> migrations;
    std::list<Migration *> prepQueue, activeQueue;
    uint64_t epoch;


  PUBLIC:

    MigrationSourceManager(MasterService *masterService);

    ~MigrationSourceManager();

    virtual void handleTimerEvent();

    void startMigration(uint64_t migrationId, uint64_t tableId,
                        uint64_t firstKeyHash, uint64_t lastKeyHash,
                        uint64_t sourceId, uint64_t targetId);

    void lock(uint64_t migrationId, Key &key);

    void unlock(uint64_t migrationId, Key &key);

  PRIVATE:

    DISALLOW_COPY_AND_ASSIGN(MigrationSourceManager);
};

}

#endif //RAMCLOUD_MIGRATIONSOURCEMANAGER_H
