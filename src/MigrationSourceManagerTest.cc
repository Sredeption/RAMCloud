
#include "TestUtil.h"
#include "MasterService.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "MasterClient.h"

namespace RAMCloud {

class MigrationRefresher : public ObjectFinder::TableConfigFetcher {
  public:
    MigrationRefresher() : refreshCount(1)
    {}

    bool tryGetTableConfig(
        uint64_t tableId,
        std::map<TabletKey, TabletWithLocator> *tableMap,
        std::multimap<std::pair<uint64_t, uint8_t>,
            IndexletWithLocator> *tableIndexMap)
    {
        tableMap->clear();

        Tablet rawEntry({1, 0, uint64_t(~0), ServerId(),
                         Tablet::NORMAL, LogPosition()});
        TabletWithLocator entry(rawEntry, "mock:host=master1");

        TabletKey key{entry.tablet.tableId, entry.tablet.startKeyHash};
        tableMap->insert(std::make_pair(key, entry));

        refreshCount--;
        return true;
    }

    // After this many refreshes we stop including table 99 in the
    // map; used to detect that misdirected requests are rejected by
    // the target server.
    int refreshCount;
};

class MigrationSourceManagerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    ServerList serverList;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    ServerConfig backup1Config;
    ServerId backup1Id;

    ServerConfig masterConfig;
    MasterService *service;
    Server *sourceServer;
    Server *targetServer;


    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    explicit MigrationSourceManagerTest(uint32_t segmentSize = 256 * 1024)
        : logEnabler(), context(), serverList(&context), cluster(&context),
          ramcloud(), backup1Config(ServerConfig::forTesting()), backup1Id(),
          masterConfig(ServerConfig::forTesting()), service(), sourceServer(),
          targetServer(), mutex()
    {
        Logger::get().setLogLevels(RAMCloud::WARNING);
        backup1Config.localLocator = "mock:host=backup1";
        backup1Config.services = {WireFormat::BACKUP_SERVICE,
                                  WireFormat::ADMIN_SERVICE};
        backup1Config.segmentSize = segmentSize;
        backup1Config.backup.numSegmentFrames = 30;
        Server *server = cluster.addServer(backup1Config);
        server->backup->testingSkipCallerIdCheck = true;
        backup1Id = server->serverId;

        masterConfig = ServerConfig::forTesting();
        masterConfig.segmentSize = segmentSize;
        masterConfig.maxObjectDataSize = segmentSize / 4;
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::ADMIN_SERVICE};
        masterConfig.master.logBytes = segmentSize * 30;
        masterConfig.master.numReplicas = 0;

        masterConfig.localLocator = "mock:host=master1";
        sourceServer = cluster.addServer(masterConfig);
        service = sourceServer->master.get();
        service->objectManager.log.sync();
        service->tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);

        masterConfig.localLocator = "mock:host=target";
        targetServer = cluster.addServer(masterConfig);
        targetServer->master.get()->objectManager.log.sync();
        targetServer->master.get()->migrationTargetManager.
            disableMigrationRecover = true;

        ramcloud.construct(&context, "mock:host=coordinator");
        context.objectFinder->tableConfigFetcher.reset(
            new MigrationRefresher);
        sourceServer->context->objectFinder->tableConfigFetcher.reset(
            new MigrationRefresher);
        targetServer->context->objectFinder->tableConfigFetcher.reset(
            new MigrationRefresher);
    }

    DISALLOW_COPY_AND_ASSIGN(MigrationSourceManagerTest);

    bool
    isObjectLocked(Key &key)
    {
        service->objectManager.objectMap.prefetchBucket(key.getHash());
        ObjectManager::HashTableBucketLock lock(service->objectManager, key);

        TabletManager::Tablet tablet;
        if (!service->objectManager.tabletManager->getTablet(key, &tablet))
            throw UnknownTabletException(HERE);

        return service->objectManager.lockTable.isLockAcquired(key);
    }
};

TEST_F(MigrationSourceManagerTest, increment_basic)
{
    Buffer buffer;
    uint64_t version = 0;
    int64_t oldInt64 = 1;
    int64_t newInt64;
    double oldDouble = 1.0;
    double newDouble;

    ramcloud->write(1, "key0", 4, &oldInt64, sizeof(oldInt64), NULL, NULL);
    newInt64 = ramcloud->incrementInt64(1, "key0", 4, 2, NULL, &version);
    EXPECT_EQ(2U, version);
    EXPECT_EQ(3, newInt64);

    ramcloud->read(1, "key0", 4, &buffer);
    buffer.copy(0, sizeof(newInt64), &newInt64);
    EXPECT_EQ(3, newInt64);

    ramcloud->write(1, "key1", 4, &oldDouble, sizeof(oldDouble), NULL, NULL);
    newDouble = ramcloud->incrementDouble(1, "key1", 4, 2.0, NULL, &version);
    EXPECT_EQ(3U, version);
    EXPECT_DOUBLE_EQ(3.0, newDouble);

    buffer.reset();
    ramcloud->read(1, "key1", 4, &buffer);
    buffer.copy(0, sizeof(newDouble), &newDouble);
    EXPECT_EQ(3.0, newDouble);
}

TEST_F(MigrationSourceManagerTest, txPrepare_basics)
{
    uint64_t version;
    ramcloud->write(1, "key1", 4, "item1", 5);
    ramcloud->write(1, "key2", 4, "item2", 5);
    ramcloud->write(1, "key3", 4, "item3", 5);

    using WireFormat::TxParticipant;
    using WireFormat::TxPrepare;
    using WireFormat::TxDecision;

    Key key1(1, "key1", 4);
    Key key2(1, "key2", 4);
    Key key3(1, "key3", 4);
    Key key4(1, "key4", 4);
    Buffer buffer, buffer2;

    WireFormat::TxParticipant participants[4];
    participants[0] = TxParticipant(key1.getTableId(), key1.getHash(), 10U);
    participants[1] = TxParticipant(key2.getTableId(), key2.getHash(), 11U);
    participants[2] = TxParticipant(key3.getTableId(), key3.getHash(), 12U);
    participants[3] = TxParticipant(key4.getTableId(), key4.getHash(), 12U);

    WireFormat::TxPrepare::Request reqHdr;
    WireFormat::TxPrepare::Response respHdr;
    Buffer reqBuffer, respBuffer;
    Service::Rpc rpc(NULL, &reqBuffer, &respBuffer);

    reqHdr.common.opcode = WireFormat::Opcode::TX_PREPARE;
    reqHdr.common.service = WireFormat::MASTER_SERVICE;
    reqHdr.lease = {1, 10, 5};
    reqHdr.clientTxId = 1;
    reqHdr.ackId = 8;
    reqHdr.participantCount = 4;
    reqHdr.opCount = 3;
    reqBuffer.appendCopy(&reqHdr, sizeof32(reqHdr));
    reqBuffer.appendExternal(participants, sizeof32(TxParticipant) * 4);
    TransactionId txId(1U, 1U);

    // 2A. ReadOp
    RejectRules rejectRules;
    rejectRules = {1UL, false, false, false, true};
    TxPrepare::Request::ReadOp op1(key1.getTableId(),
                                   10,
                                   key1.getStringKeyLength(),
                                   rejectRules);
    reqBuffer.appendExternal(&op1, sizeof32(op1));
    reqBuffer.appendExternal(key1.getStringKey(), key1.getStringKeyLength());

    // 2B. RemoveOp
    rejectRules = {2UL, false, false, false, true};
    TxPrepare::Request::RemoveOp op2(key2.getTableId(),
                                     11,
                                     key2.getStringKeyLength(),
                                     rejectRules);

    reqBuffer.appendExternal(&op2, sizeof32(op2));
    reqBuffer.appendExternal(key2.getStringKey(), key2.getStringKeyLength());

    // 2C. WriteOp
    rejectRules = {3UL, false, false, false, true};
    Buffer keysAndValueBuf;
    Object::appendKeysAndValueToBuffer(key3, "new", 3, &keysAndValueBuf);
    TxPrepare::Request::WriteOp op3(key3.getTableId(),
                                    12,
                                    keysAndValueBuf.size(),
                                    rejectRules);

    reqBuffer.appendExternal(&op3, sizeof32(op3));
    reqBuffer.appendExternal(&keysAndValueBuf);

    // 3. Prepare.
    EXPECT_FALSE(isObjectLocked(key1));
    EXPECT_FALSE(isObjectLocked(key2));
    EXPECT_FALSE(isObjectLocked(key3));
    EXPECT_FALSE(service->objectManager.unackedRpcResults->hasRecord(
        txId.clientLeaseId,
        txId.clientTransactionId));
    {
        Buffer value;
        ramcloud->read(1, "key1", 4, &value, NULL, &version);
        EXPECT_EQ(1U, version);
        EXPECT_EQ("item1", string(reinterpret_cast<const char *>(
                                      value.getRange(0, value.size())),
                                  value.size()));
        value.reset();
        ramcloud->read(1, "key2", 4, &value, NULL, &version);
        EXPECT_EQ(2U, version);
        EXPECT_EQ("item2", string(reinterpret_cast<const char *>(
                                      value.getRange(0, value.size())),
                                  value.size()));
        value.reset();
        ramcloud->read(1, "key3", 4, &value, NULL, &version);
        EXPECT_EQ(3U, version);
        EXPECT_EQ("item3", string(reinterpret_cast<const char *>(
                                      value.getRange(0, value.size())),
                                  value.size()));
    }
    service->txPrepare(&reqHdr, &respHdr, &rpc);

    uint64_t migrationId = 1;
    MigrationSourceStartRpc migrationRpc(
        &context, sourceServer->serverId, migrationId, sourceServer->serverId,
        targetServer->serverId, 1ul, 0ul, ~0ul);
    migrationRpc.wait();

    EXPECT_EQ(STATUS_OK, respHdr.common.status);
    EXPECT_EQ(TxPrepare::PREPARED, respHdr.vote);

    // 4. Check outcome of Prepare.
    EXPECT_EQ(3U, service->transactionManager.items.size());
    EXPECT_TRUE(isObjectLocked(key1));
    EXPECT_TRUE(isObjectLocked(key2));
    EXPECT_TRUE(isObjectLocked(key3));
    {
        TransactionManager::Lock lock(service->transactionManager.mutex);
        EXPECT_TRUE(service->transactionManager.getTransaction(txId,
                                                               lock) != NULL);
    }
    Buffer value;
    ramcloud->read(1, "key1", 4, &value, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ("item1", string(reinterpret_cast<const char *>(
                                  value.getRange(0, value.size())),
                              value.size()));
    value.reset();
    ramcloud->read(1, "key2", 4, &value, NULL, &version);
    EXPECT_EQ(2U, version);
    EXPECT_EQ("item2", string(reinterpret_cast<const char *>(
                                  value.getRange(0, value.size())),
                              value.size()));
    value.reset();
    ramcloud->read(1, "key3", 4, &value, NULL, &version);
    EXPECT_EQ(3U, version);
    EXPECT_EQ("item3", string(reinterpret_cast<const char *>(
                                  value.getRange(0, value.size())),
                              value.size()));

    MigrationSourceManager::Migration *migration =
        service->migrationSourceManager.migrations[migrationId];
    EXPECT_FALSE(migration->active);

    WireFormat::TxDecision::Request decisionReqHdr;
    WireFormat::TxDecision::Response decisionRespHdr;
    Buffer decisionReqBuffer;
    Service::Rpc decisionRpc(NULL, &decisionReqBuffer, NULL);

    decisionReqHdr.decision = TxDecision::COMMIT;
    decisionReqHdr.leaseId = 1U;
    decisionReqHdr.transactionId = 1U;
    decisionReqHdr.recovered = false;
    decisionReqHdr.participantCount = 3U;
    decisionReqBuffer.appendExternal(&decisionReqHdr,
                                     sizeof32(decisionReqHdr));
    decisionReqBuffer.appendExternal(participants, sizeof32(TxParticipant) * 3);

    service->txDecision(&decisionReqHdr, &decisionRespHdr, &decisionRpc);

    value.reset();
    ramcloud->read(1, "key3", 4, &value, NULL, &version);
    EXPECT_EQ(4U, version);
    EXPECT_EQ("new", string(reinterpret_cast<const char *>(
                                value.getRange(0, value.size())),
                            value.size()));

    EXPECT_FALSE(isObjectLocked(key2));
    service->migrationSourceManager.manager->handleTimerEvent();
    while (!migration->active);
    service->transactionManager.cleaner.stop();
    service->migrationSourceManager.stop();
}

}
