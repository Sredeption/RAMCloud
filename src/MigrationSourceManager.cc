

#include "MigrationSourceManager.h"
#include "MasterService.h"

namespace RAMCloud {

MigrationSourceManager::MigrationSourceManager(
    MasterService *masterService)
    : WorkerTimer(masterService->context->dispatch),
      masterService(masterService),
      transactionManager(&masterService->transactionManager),
      operatingBuffer(new std::queue<Item>[2]), operatingIndex(0),
      bufferLock("migrationBufferLock"), treeLock(), migrations(), prepQueue(),
      activeQueue(), epoch(0)
{
}

MigrationSourceManager::~MigrationSourceManager()
{
}

void MigrationSourceManager::handleTimerEvent()
{
    std::queue<Item> *buffer;
    {
        SpinLock::Guard guard(bufferLock);
        buffer = &operatingBuffer[operatingIndex];
        operatingIndex = static_cast<uint8_t>(operatingIndex == 1 ? 0 : 1);
    }

    std::lock_guard<std::mutex> guard(treeLock);
    while (!buffer->empty()) {
        Item &item = buffer->front();

        Migration *migration = migrations[item.migrationId];
        switch (item.type) {
            case LOCK:
                migration->lock(item.keyHash);
                break;
            case UNLOCK:
                migration->unlock(item.keyHash);
                break;
        }
        buffer->pop();
    }
    epoch++;

    uint64_t minTimestamp = transactionManager->getMinTimestamp();
    RAMCLOUD_LOG(NOTICE, "Current min timestamp: %lu", minTimestamp);
    std::list<Migration *>::iterator iterator = prepQueue.begin();
    while (iterator != prepQueue.end()) {
        Migration *migration = *iterator;
        if (minTimestamp > migration->timestamp) {
            RAMCLOUD_LOG(NOTICE, "Min timestamp: %lu. migration %lu start",
                         minTimestamp, migration->migrationId);
            migration->start();
            iterator = prepQueue.erase(iterator);
        } else {
            iterator++;
        }
    }

    if (!prepQueue.empty())
        start(0);
}

void MigrationSourceManager::startMigration(
    uint64_t migrationId, uint64_t tableId, uint64_t firstKeyHash,
    uint64_t lastKeyHash, uint64_t sourceId, uint64_t targetId)
{
    {
        std::lock_guard<std::mutex> guard(treeLock);
        Migration *migration = new Migration(this, migrationId, tableId,
                                             firstKeyHash,
                                             lastKeyHash, sourceId,
                                             targetId);
        migrations[migrationId] = migration;
        prepQueue.push_back(migration);
    }

    start(0);
}

void MigrationSourceManager::lock(uint64_t migrationId, Key &key)
{
    SpinLock::Guard guard(bufferLock);
    operatingBuffer[operatingIndex].emplace(
        Item{LOCK, migrationId, key.getHash()});
}

void MigrationSourceManager::unlock(uint64_t migrationId, Key &key)
{
    SpinLock::Guard guard(bufferLock);
    operatingBuffer[operatingIndex].emplace(
        Item{UNLOCK, migrationId, key.getHash()});
}

MigrationSourceManager::Migration::Migration(
    MigrationSourceManager *manager,
    uint64_t migrationId,
    uint64_t tableId,
    uint64_t firstKeyHash,
    uint64_t lastKeyHash,
    uint64_t sourceId,
    uint64_t targetId)
    : manager(manager), migrationId(migrationId), tableId(tableId),
      firstKeyHash(firstKeyHash), lastKeyHash(lastKeyHash), sourceId(sourceId),
      targetId(targetId), timestamp(Cycles::rdtsc()), startEpoch(0),
      active(false)
{
}

void MigrationSourceManager::Migration::lock(uint64_t keyHash)
{

}

void MigrationSourceManager::Migration::unlock(uint64_t keyHash)
{

}

void MigrationSourceManager::Migration::start()
{
    active = true;
    manager->masterService->tabletManager.migrateTablet(
        tableId, firstKeyHash, lastKeyHash, migrationId, sourceId, targetId,
        TabletManager::MIGRATION_SOURCE);

    auto replicas = manager->masterService->objectManager.getReplicas();

    ServerId sourceServerId(sourceId);
    ServerId targetServerId(targetId);
    startEpoch = manager->epoch;

    MasterClient::migrationTargetStart(
        manager->masterService->context, targetServerId, migrationId,
        sourceServerId, targetServerId, tableId, firstKeyHash,
        lastKeyHash, manager->masterService->tabletManager.getSafeVersion(),
        replicas.data(), downCast<uint32_t>(replicas.size()));
}

}
