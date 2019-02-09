#include "MigrationTargetManager.h"
#include "MasterService.h"

namespace RAMCloud {

MigrationTargetManager::MigrationTargetManager(MasterService *masterService) :
    masterService(masterService), migrations(), lock("MigrationTargetManager"),
    disableMigrationRecover(false)
{
}

void
MigrationTargetManager::startMigration(
    uint64_t migrationId, uint64_t tableId, uint64_t firstKeyHash,
    uint64_t lastKeyHash, uint64_t sourceId, uint64_t targetId)
{

    SpinLock::Guard guard(lock);
    Migration *migration = new Migration(migrationId, tableId, firstKeyHash,
                                         lastKeyHash, sourceId, targetId);
    migrations[migrationId] = migration;

    bool added = masterService->tabletManager.addTablet(
        tableId,
        firstKeyHash,
        lastKeyHash,
        TabletManager::MIGRATION_TARGET);
    masterService->tabletManager.migrateTablet(
        tableId, firstKeyHash, lastKeyHash, migrationId, sourceId, targetId,
        TabletManager::MIGRATION_TARGET);

    if (!added) {
        throw Exception(HERE,
                        format("Cannot recover tablet that overlaps "
                               "an already existing one (tablet to recover: %lu "
                               "range [0x%lx,0x%lx], current tablet map: %s)",
                               tableId,
                               firstKeyHash, lastKeyHash,
                               masterService->tabletManager.toString().c_str()));
    } else {
        TableStats::addKeyHashRange(&masterService->masterTableMetadata,
                                    tableId, firstKeyHash, lastKeyHash);
    }

}

void
MigrationTargetManager::finishMigration(uint64_t migrationId, bool successful)
{
    SpinLock::Guard guard(lock);
    Migration *migration;
    std::unordered_map<uint64_t, Migration *>::iterator iter =
        migrations.find(migrationId);
    if (iter == migrations.end())
        return;
    migration = iter->second;
    migrations.erase(iter);
    bool cancelRecovery = false;
    if (!disableMigrationRecover)
        cancelRecovery = CoordinatorClient::migrationFinished(
            masterService->context, migrationId, ServerId(migration->targetId),
            successful);

    if (!cancelRecovery) {
        // Re-grab all transaction locks.
//        transactionManager.regrabLocksAfterRecovery(&objectManager);

        bool changed = masterService->tabletManager.changeState(
            migration->tableId,
            migration->firstKeyHash, migration->lastKeyHash,
            TabletManager::MIGRATION_TARGET, TabletManager::NORMAL);
        if (!changed) {
            throw FatalError(
                HERE, format("Could not change recovering "
                             "tablet's state to NORMAL (%lu range [%lu,%lu])",
                             migration->tableId,
                             migration->firstKeyHash,
                             migration->lastKeyHash));
        }
    } else {
        bool removed = masterService->tabletManager.deleteTablet(
            migration->tableId, migration->firstKeyHash,
            migration->lastKeyHash);
        if (removed) {
            TableStats::deleteKeyHashRange(
                &masterService->masterTableMetadata, migration->tableId,
                migration->firstKeyHash, migration->lastKeyHash);
        }
        masterService->objectManager.removeOrphanedObjects();
        masterService->transactionManager.removeOrphanedOps();
    }
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

MigrationTargetManager::Migration *
MigrationTargetManager::getMigration(uint64_t migrationId)
{
    SpinLock::Guard guard(lock);
    std::unordered_map<uint64_t, Migration *>::iterator migrationPair =
        migrations.find(migrationId);
    if (migrationPair == migrations.end())
        return NULL;
    return migrationPair->second;
}

MigrationTargetManager::Migration::Migration(
    uint64_t migrationId, uint64_t tableId, uint64_t firstKeyHash,
    uint64_t lastKeyHash, uint64_t sourceId, uint64_t targetId)
    : migrationId(migrationId), rangeList(), tableId(tableId),
      firstKeyHash(firstKeyHash), lastKeyHash(lastKeyHash), sourceId(sourceId),
      targetId(targetId)
{

}

void MigrationTargetManager::RealFinishNotifier::notify(uint64_t migrationId)
{

}

}
