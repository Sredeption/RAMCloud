#include "GeminiMigrationManager.h"

namespace RAMCloud {

GeminiMigrationManager::GeminiMigrationManager(
    Context *context, string localLocator)
    : Dispatch::Poller(context->auxDispatch, "GeminiMigrationManager"),
      context(context), tombstoneProtector(), localLocator(localLocator),
      migrationsInProgress(), active(false)
{

}

GeminiMigrationManager::~GeminiMigrationManager()
{

}

int GeminiMigrationManager::poll()
{
    return 0;
}

bool GeminiMigrationManager::startMigration(ServerId sourceServerId,
                                            uint64_t tableId,
                                            uint64_t startKeyHash,
                                            uint64_t endKeyHash)
{
    return false;
}

bool GeminiMigrationManager::requestPriorityHash(uint64_t tableId,
                                                 uint64_t startKeyHash,
                                                 uint64_t endKeyHash,
                                                 uint64_t priorityHash)
{
    return false;
}

}
