#include "MigrationTargetManager.h"

namespace RAMCloud {

MigrationTargetManager::MigrationTargetManager() :
    migrations(), lock("MigrationTargetManager"), disableMigrationRecover(false)
{

}

void MigrationTargetManager::init(uint64_t migrationId)
{

    SpinLock::Guard guard(lock);
    Migration *migration = new Migration(migrationId);
    migrations[migrationId] = migration;

}

bool MigrationTargetManager::isLocked(uint64_t migrationId, Key &key)
{
    SpinLock::Guard guard(lock);
    auto migration = migrations.find(migrationId);
    if (migration == migrations.end())
        throw FatalError(HERE, "Migration not found");
    return migration->second->rangeList.isLocked(key.getHash());
}

void MigrationTargetManager::update(uint64_t migrationId,
                                    vector<WireFormat::MigrationIsLocked::Range> &ranges)
{

    SpinLock::Guard guard(lock);
    auto migration = migrations.find(migrationId);
    if (migration == migrations.end())
        throw FatalError(HERE, "Migration not found");
    for (auto range : ranges)
        migration->second->rangeList.push(range);

}

MigrationTargetManager::Migration::Migration(uint64_t migrationId)
    : migrationId(migrationId), rangeList()
{

}
}
