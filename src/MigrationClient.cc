#include "MigrationClient.h"
#include "RamCloud.h"


RAMCloud::MigrationClient::MigrationClient(RAMCloud::RamCloud *ramcloud)
    : ramcloud(ramcloud),
      tableMap()
{

}

void RAMCloud::MigrationClient::putTablet(Tablet &tablet,
                                          uint32_t sourceId, uint32_t targetId)
{
    string sourceLocator = ramcloud->clientContext->serverList->getLocator(
        ServerId(sourceId));
    string targetLocator = ramcloud->clientContext->serverList->getLocator(
        ServerId(targetId));
}
