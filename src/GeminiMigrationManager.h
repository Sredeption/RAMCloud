#ifndef RAMCLOUD_GEMINIMIGRATIONMANAGER_H
#define RAMCLOUD_GEMINIMIGRATIONMANAGER_H

#include "Dispatch.h"
#include "Context.h"
#include "ServerId.h"
#include "ObjectManager.h"

namespace RAMCloud {

class GeminiMigration;

class GeminiMigrationManager : Dispatch::Poller {
  public:
    explicit GeminiMigrationManager(Context *context, string localLocator);

    ~GeminiMigrationManager();

    int poll();

    bool startMigration(ServerId sourceServerId, uint64_t tableId,
                        uint64_t startKeyHash, uint64_t endKeyHash);

    bool requestPriorityHash(uint64_t tableId, uint64_t startKeyHash,
                             uint64_t endKeyHash, uint64_t priorityHash);

  PRIVATE:
    // Shared RAMCloud information.
    Context *context;

    Tub<ObjectManager::TombstoneProtector> tombstoneProtector;

    // Address of this RAMCloud master. Required by RocksteadyMigration.
    const string localLocator;

    // The list of in-progress migrations for which this RAMCloud master
    // is the destination.
    std::vector<GeminiMigration *> migrationsInProgress;

    bool active;

    DISALLOW_COPY_AND_ASSIGN(GeminiMigrationManager);
};

}

#endif //RAMCLOUD_GEMINIMIGRATIONMANAGER_H
