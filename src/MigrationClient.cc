#include "MigrationClient.h"
#include "RamCloud.h"
#include "ObjectFinder.h"
#include "Key.h"
#include "Dispatch.h"


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
    tableMap.emplace(
        TabletKey{tablet.tableId, tablet.startKeyHash},
        MigratingTablet(tablet, sourceId, targetId));
    RAMCLOUD_LOG(NOTICE, "%lu to migrating", tablet.tableId);
//    MigratingTablet &migratingTablet = result.first->second;
//    CoordinatorClient::migrationGetLocator(
//        ramcloud->clientContext,
//        migratingTablet.sourceId.getId(),
//        migratingTablet.targetId.getId(),
//        &migratingTablet.sourceLocator,
//        &migratingTablet.targetLocator);
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
    RAMCLOUD_LOG(NOTICE, "%lu finish migrating", tablet.tableId);

    tableMap.erase(TabletKey{tablet.tableId, tablet.startKeyHash});
}

}