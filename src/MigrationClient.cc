#include "MigrationClient.h"
#include "RamCloud.h"
#include "ObjectFinder.h"
#include "Key.h"


namespace RAMCloud {

MigrationClient::MigrationClient(RAMCloud::RamCloud *ramcloud)
    : ramcloud(ramcloud),
      tableMap()
{

}

void MigrationClient::putTablet(uint64_t tableId, const void *key,
                                uint16_t keyLength,
                                uint64_t sourceId, uint64_t targetId)
{
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);

    ramcloud->clientContext->objectFinder->flush(tableId);
    Tablet tablet = ramcloud->clientContext->objectFinder
        ->lookupTablet(tableId, keyHash)->tablet;
    tableMap.emplace(TabletKey{tablet.tableId, tablet.startKeyHash},
                     MigratingTablet(tablet, sourceId, targetId));
}

MigrationClient::MigratingTablet *
MigrationClient::getTablet(uint64_t tableId, const void *key,
                           uint16_t keyLength)
{
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);
    Tablet tablet = ramcloud->clientContext->objectFinder
        ->lookupTablet(tableId, keyHash)->tablet;

    std::map<TabletKey, MigratingTablet>::iterator tabletIterator;
    tabletIterator = tableMap.find(
        TabletKey{tablet.tableId, tablet.startKeyHash});
    if (tabletIterator != tableMap.end()) {
        return &tabletIterator->second;
    }
    return NULL;
}

void MigrationClient::removeTablet(uint64_t tableId, const void *key,
                                   uint16_t keyLength)
{
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);
    ramcloud->clientContext->objectFinder->flush(tableId);
    Tablet tablet = ramcloud->clientContext->objectFinder
        ->lookupTablet(tableId, keyHash)->tablet;

    tableMap.erase(TabletKey{tablet.tableId, tablet.startKeyHash});
}


MigrationReadTask::MigrationReadTask(
    RamCloud *ramcloud, uint64_t tableId, const void *key, uint16_t keyLength,
    Buffer *value, const RejectRules *rejectRules)
    : ramcloud(ramcloud), tableId(tableId), key(key), keyLength(keyLength),
      value(value), rejectRules(rejectRules), readRpc(), sourceReadRpc(),
      targetReadRpc(), state(INIT), sourceBuffer(), targetBuffer(), version(),
      objectExists()
{

}

void MigrationReadTask::performTask()
{
    if (state == INIT) {
        MigrationClient::MigratingTablet *migratingTablet =
            ramcloud->migrationClient->getTablet(tableId, key, keyLength);
        if (migratingTablet) {
            sourceReadRpc.construct(
                ramcloud, migratingTablet->sourceId, tableId, key, keyLength,
                &sourceBuffer, rejectRules);
            targetReadRpc.construct(
                ramcloud, migratingTablet->targetId, tableId, key, keyLength,
                &targetBuffer, rejectRules);
            state = MIGRATING;
        } else {
            readRpc.construct(ramcloud, tableId, key, keyLength, value,
                              rejectRules);
            state = NORMAL;
        }

    }

    if (state == NORMAL) {
        if (readRpc->isReady()) {
            bool migrating;
            uint64_t sourceId, targetId;
            readRpc->wait(&version, &objectExists, &migrating, &sourceId,
                          &targetId);

            if (migrating) {
                ramcloud->migrationClient->putTablet(
                    tableId, key, keyLength, sourceId, targetId);
                state = INIT;
            } else {
                state = DONE;
            }
        }
    }

    if (state == MIGRATING) {
        if (sourceReadRpc->isReady() && targetReadRpc->isReady()) {
            bool migrating;
            uint64_t sourceId;
            uint64_t sourceVersion;
            bool sourceObjectExists;
            uint64_t targetId;
            uint64_t targetVersion;
            bool targetObjectExists;

            bool success = true;
            success = success && sourceReadRpc->wait(
                &sourceVersion, &sourceObjectExists, &migrating,
                &sourceId, &targetId);
            success = success && targetReadRpc->wait(
                &targetVersion, &targetObjectExists);

            if (!success) {
                state = INIT;
                return;
            }

            if (!migrating) {
                ramcloud->migrationClient->removeTablet(tableId, key,
                                                        keyLength);
            }
            value->reset();

            uint64_t versionConclusion;
            bool existsConclusion;
            if (sourceVersion > targetVersion) {
                versionConclusion = sourceVersion;
                value->append(&sourceBuffer, 0u, sourceBuffer.size());
                existsConclusion = sourceObjectExists;
            } else {
                versionConclusion = targetVersion;
                value->append(&targetBuffer, 0u, targetBuffer.size());
                existsConclusion = targetObjectExists;
            }
            if (version)
                version = versionConclusion;
            if (objectExists)
                objectExists = existsConclusion;
            state = DONE;
        }
    }

}

bool MigrationReadTask::isReady()
{
    performTask();
    return state == DONE;
}

void MigrationReadTask::wait(uint64_t *version, bool *objectExists)
{
    while (!isReady());
    if (version)
        *version = this->version;
    if (objectExists)
        *objectExists = this->objectExists;
}
}
