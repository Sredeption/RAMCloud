/* Copyright (c) 2010-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "Migration.h"
#include "MockExternalStorage.h"

namespace RAMCloud {

using namespace MigrationInternal; // NOLINT

struct MigrationTest : public ::testing::Test {
    typedef MigrationStartReadingRpc::Replica Replica;
    Context context;
    TaskQueue taskQueue;
    MigrationTracker tracker;
    ServerList serverList;
    MockExternalStorage storage;
    CoordinatorUpdateManager manager;
    TableManager tableManager;
    std::mutex mutex;

    MigrationTest()
        : context(),
          taskQueue(),
          tracker(&context),
          serverList(&context),
          storage(true),
          manager(&storage),
          tableManager(&context, &manager),
          mutex()
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
    }

    void
    addServersToTracker(size_t count, ServiceMask services)
    {
        for (uint32_t i = 1; i < count + 1; ++i) {
            string locator = format("mock:host=server%u", i);
            tracker.enqueueChange({{i, 0}, locator, services,
                                   100, ServerStatus::UP}, SERVER_ADDED);
        }
        ServerDetails _;
        ServerChangeEvent __;
        while (tracker.getChange(_, __));
    }

    typedef std::unique_lock<std::mutex> Lock;

  private:
    DISALLOW_COPY_AND_ASSIGN(MigrationTest);
};
namespace {
void
populateLogDigest(MigrationStartReadingRpc::Result &result,
                  uint64_t segmentId,
                  std::vector<uint64_t> segmentIds)
{
    // Doesn't matter for these tests.
    LogDigest digest;
        foreach (uint64_t id, segmentIds)digest.addSegmentId(id);

    Buffer buffer;
    digest.appendToBuffer(buffer);
    result.logDigestSegmentEpoch = 100;
    result.logDigestSegmentId = segmentId;
    result.logDigestBytes = buffer.size();
    result.logDigestBuffer =
        std::unique_ptr<char[]>(new char[result.logDigestBytes]);
    buffer.copy(0, buffer.size(), result.logDigestBuffer.get());
}
} // namespace


TEST_F(MigrationTest, splitTabletsNoEstimator)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    uint64_t firstKeyHash = 0;
    uint64_t lastKeyHash = 9;
    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, firstKeyHash, lastKeyHash, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet(
        {tableId, lastKeyHash + 1, lastKeyHash + 10, sourceId, Tablet::NORMAL,
         {}});

    migration.construct(&context, taskQueue, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, firstKeyHash, lastKeyHash);

    auto tablets = tableManager.markTabletsMigration(sourceId, tableId,
                                                     firstKeyHash, lastKeyHash);

    EXPECT_EQ(1u, tablets.size());

    migration->splitTablets(&tablets, NULL);

    Tablet *tablet = NULL;

    EXPECT_EQ(1u, tablets.size());

    EXPECT_EQ("{ 1: 0x0-0x9 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x0-0x9 }", tablet->debugString(1));
    }
    tablet = NULL;
}

TEST_F(MigrationTest, splitTabletsBadEstimator)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> recovery;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    uint64_t firstKeyHash = 0;
    uint64_t lastKeyHash = 9;
    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, firstKeyHash, lastKeyHash, sourceId, Tablet::NORMAL, {}});

    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, firstKeyHash, lastKeyHash);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId,
                                                     firstKeyHash, lastKeyHash);


    TableStats::Estimator e(NULL);

    EXPECT_FALSE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    Tablet *tablet = NULL;

    EXPECT_EQ(1u, tablets.size());

    EXPECT_EQ("{ 1: 0x0-0x9 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x0-0x9 }", tablet->debugString(1));
    }
    tablet = NULL;
}

TEST_F(MigrationTest, splitTabletsMultiTablet)
{
    // Case where there is more than one tablet.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet({tableId, 0, 9, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet({tableId, 10, 29, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet({tableId, 30, 49, sourceId, Tablet::NORMAL, {}});
    migration.construct(&context, taskQueue, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 29);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 0, 29);

    char buffer[sizeof(TableStats::DigestHeader) +
                3 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 3;
    digest->header.otherBytesPerKeyHash = 0;
    digest->header.otherRecordsPerKeyHash = 0;
    digest->entries[0].tableId = 1;
    digest->entries[0].bytesPerKeyHash = Migration::PARTITION_MAX_BYTES / 10;
    digest->entries[0].recordsPerKeyHash =
        Migration::PARTITION_MAX_RECORDS / 10;

    digest->entries[1].tableId = 2;
    digest->entries[1].bytesPerKeyHash = Migration::PARTITION_MAX_BYTES / 10;
    digest->entries[1].recordsPerKeyHash =
        Migration::PARTITION_MAX_RECORDS / 10;

    digest->entries[2].tableId = 3;
    digest->entries[2].bytesPerKeyHash = Migration::PARTITION_MAX_BYTES / 10;
    digest->entries[2].recordsPerKeyHash =
        Migration::PARTITION_MAX_RECORDS / 10;

    TableStats::Estimator e(digest);

    EXPECT_TRUE(e.valid);

    EXPECT_EQ(2u, tablets.size());
    migration->splitTablets(&tablets, &e);
    EXPECT_EQ(3u, tablets.size());
}

TEST_F(MigrationTest, splitTabletsBasic)
{
    // Tests that a basic split of a single tablet will execute and update the
    // TableManager tablet information.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t keyHashCount = 5;
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, 1, keyHashCount, sourceId, Tablet::NORMAL, {}});
    migration.construct(&context, taskQueue, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 1, keyHashCount);

    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 1,
                                                     keyHashCount);

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (3.0 / double(keyHashCount))
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (3.0 / double(keyHashCount))
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    migration->splitTablets(&tablets, &e);

    Tablet *tablet = NULL;

    EXPECT_EQ(3u, tablets.size());

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x1-0x2 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 1u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x1-0x2 }", tablet->debugString(1));
    }
    tablet = NULL;

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x3-0x4 }", tablets[1].debugString(1));
    tablet = tableManager.testFindTablet(1u, 3u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x3-0x4 }", tablet->debugString(1));
    }
    tablet = NULL;

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x5-0x5 }", tablets[2].debugString(1));
    tablet = tableManager.testFindTablet(1u, 5u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x5-0x5 }", tablet->debugString(1));
    }
    tablet = NULL;
}

TEST_F(MigrationTest, splitTabletsByteDominated)
{
    // Ensure the tablet count can be determined by the byte count.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> recovery;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t keyHashCount = 1000;
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, 1, keyHashCount, sourceId, Tablet::NORMAL, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, 1, keyHashCount);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 1,
                                                     keyHashCount);

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (6.0 / double(keyHashCount))
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (2.0 / double(keyHashCount))
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(6u, tablets.size());
}

TEST_F(MigrationTest, splitTabletsRecordDominated)
{
    // Ensure the tablet count can be determined by the record count.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> recovery;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t keyHashCount = 1000;
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, 1, keyHashCount, {99, 0}, Tablet::NORMAL, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, 1, keyHashCount);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 1,
                                                     keyHashCount);

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (2.0 / double(keyHashCount))
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (6.0 / double(keyHashCount))
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(6u, tablets.size());
}

TEST_F(MigrationTest, partitionTabletsNoEstimator)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    uint64_t tableId = 123;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    Tub<Migration> recovery;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);

    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, 0, 30);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 0, 30);
    recovery->partitionTablets(tablets, NULL);
    EXPECT_EQ(0lu, recovery->numPartitions);

    tableManager.testCreateTable("t", tableId);
    tableManager.testAddTablet(
        {tableId, 0, 9, sourceId, Tablet::RECOVERING, {}});
    tableManager.testAddTablet(
        {tableId, 20, 29, sourceId, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, 0, 30);
    tablets = tableManager.markAllTabletsRecovering(ServerId(99));
    recovery->partitionTablets(tablets, NULL);
    EXPECT_EQ(2lu, recovery->numPartitions);

    tableManager.testAddTablet(
        {tableId, 10, 19, sourceId, Tablet::RECOVERING, {}});
    recovery.construct(&context, taskQueue, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, 0, 30);
    tablets = tableManager.markAllTabletsRecovering(ServerId(99));
    recovery->partitionTablets(tablets, NULL);
    EXPECT_EQ(3lu, recovery->numPartitions);
}

TEST_F(MigrationTest, partitionTabletsSplits)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet({tableId, 0, 99, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet(
        {tableId, 100, 199, sourceId, Tablet::NORMAL, {}});

    migration.construct(&context, taskQueue, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 199);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 0, 199);

    char buffer[sizeof(TableStats::DigestHeader) +
                2 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 2;
    digest->header.otherBytesPerKeyHash = 0;
    digest->header.otherRecordsPerKeyHash = 0;
    digest->entries[0].tableId = 1;
    digest->entries[0].bytesPerKeyHash = (10.0 / 100)
                                         * Migration::PARTITION_MAX_BYTES;
    digest->entries[0].recordsPerKeyHash = (1.0 / 100)
                                           * Migration::PARTITION_MAX_RECORDS;
    digest->entries[1].tableId = 2;
    digest->entries[1].bytesPerKeyHash = (1.0 / 100)
                                         * Migration::PARTITION_MAX_BYTES;
    digest->entries[1].recordsPerKeyHash = (10.0 / 100)
                                           * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(20lu, migration->numPartitions);
}

TEST_F(MigrationTest, partitionTabletsBasic)
{
    // This covers the following cases:
    //      (1) No partitions to choose from
    //      (2) Single partiton to choose from
    //      (3) Evicting a partition
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    for (uint64_t i = 0; i < 250; i++) {
        tableManager.testAddTablet(
            {tableId, i * 10, i * 10 + 9, sourceId, Tablet::NORMAL, {}});
    }
    migration.construct(&context, taskQueue, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 2499);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 0,
                                                     2499);

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (0.01 / 10)
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (0.01 / 10)
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(3lu, migration->numPartitions);
}

TEST_F(MigrationTest, partitionTabletsAllPartitionsOpen)
{
    // Covers the addtional case where no partitions are large enough to be
    // evicted, there is more than one partition to choose from and none will
    // fit.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    for (uint64_t i = 0; i < 20; i++) {
        tableManager.testAddTablet(
            {tableId, i * 10, i * 10 + 9, sourceId, Tablet::NORMAL, {}});
    }
    migration.construct(&context, taskQueue, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 199);
    auto tablets = tableManager.markTabletsMigration(sourceId, tableId, 0, 199);

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (0.6 / 10)
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (0.6 / 10)
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(20lu, migration->numPartitions);
}

bool
tabletCompare(const Tablet &a,
              const Tablet &b)
{
    if (a.tableId != b.tableId)
        return a.tableId < b.tableId;
    else
        return a.startKeyHash < b.startKeyHash;
}

TEST_F(MigrationTest, partitionTabletsMixed)
{
    // Covers the addtional case where partitions will contain one large tablet
    // and filled by many smaller ones.  Case covers selecting from multiple
    // partitons and having a tablet fit.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    for (uint64_t i = 0; i < 6; i++) {
        tableManager.testAddTablet(
            {tableId, i * 600, i * 600 + 599, sourceId, Tablet::NORMAL, {}});
    }
    for (uint64_t i = 0; i < 180; i++) {
        tableManager.testAddTablet(
            {tableId, i * 10 + 3600, i * 10 + 3609,
             sourceId, Tablet::NORMAL, {}});
    }
    migration.construct(&context, taskQueue, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 5399);
    vector<Tablet> tablets =
        tableManager.markTabletsMigration(sourceId, tableId, 0, 5399);

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (0.6 / 600)
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (0.6 / 600)
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    // Tablets need to be sorted because the list returned from
    // markAllTabletsRecovering is built from unordered_map::iterator, which
    // means that the ordering can change from version to version of gcc.
    std::sort(tablets.begin(), tablets.end(), tabletCompare);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(6lu, migration->numPartitions);
}


TEST_F(MigrationTest, startBackups)
{
    /**
     * Called by BackupStartTask instead of sending the startReadingData
     * RPC. The callback mocks out the result of the call for testing.
     * Each call into the callback corresponds to the send of the RPC
     * to an individual backup.
     */
    struct Cb : public BackupStartTask::TestingCallback {
        int callCount;

        Cb() : callCount()
        {}

        void backupStartTaskSend(MigrationStartReadingRpc::Result &result)
        {
            if (callCount == 0) {
                // Two segments on backup1, one that overlaps with backup2
                // Includes a segment digest
                result.replicas.push_back(Replica{88lu, 100lu, false});
                result.replicas.push_back(Replica{89lu, 100lu, true});
                populateLogDigest(result, 89, {88, 89});
                result.primaryReplicaCount = 1;
            } else if (callCount == 1) {
                // One segment on backup2
                result.replicas.push_back(Replica{88lu, 100lu, false});
                result.primaryReplicaCount = 1;
            } else if (callCount == 2) {
                // No segments on backup3
            }
            callCount++;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    uint64_t tableId = 123;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};


    tableManager.testCreateTable("t", tableId);
    tableManager.testAddTablet({tableId, 10, 19, sourceId, Tablet::NORMAL, {}});
    Migration recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                       sourceId, targetId, tableId, 10, 19);
    recovery.testingBackupStartTaskSendCallback = &callback;
    recovery.startBackups();
    EXPECT_EQ((vector<WireFormat::Recover::Replica>{
        {1, 88},
        {2, 88},
        {1, 89},
    }), recovery.replicaMap);
}

TEST_F(MigrationTest, startBackupsFailureContactingSomeBackup)
{
    Migration recovery(&context, taskQueue, &tableManager, &tracker, NULL,
                       ServerId(99), ServerId(100), 123, 0, 9);
    BackupStartTask task(&recovery, {2, 0});
    EXPECT_NO_THROW(task.send());
}

TEST_F(MigrationTest, startBackupsSecondariesEarlyInSomeList)
{
    // See buildReplicaMap test for info about how the callback is used.
    struct Cb : public BackupStartTask::TestingCallback {
        int callCount;

        Cb() : callCount()
        {}

        void backupStartTaskSend(MigrationStartReadingRpc::Result &result)
        {
            if (callCount == 0) {
                result.replicas.push_back(Replica{88lu, 100u, false});
                result.replicas.push_back(Replica{89lu, 100u, false});
                result.replicas.push_back(Replica{90lu, 100u, false});
                result.primaryReplicaCount = 3;
            } else if (callCount == 1) {
                result.replicas.push_back(Replica{88lu, 100u, false});
                result.replicas.push_back(Replica{91lu, 100u, true});
                populateLogDigest(result, 91, {88, 89, 90, 91});
                result.primaryReplicaCount = 1;
            } else if (callCount == 2) {
                result.replicas.push_back(Replica{91lu, 100u, true});
                result.primaryReplicaCount = 1;
            }
            callCount++;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Migration migration(&context, taskQueue, &tableManager, &tracker, NULL,
                       ServerId(99), ServerId(100), 123, 10, 19);
    migration.testingBackupStartTaskSendCallback = &callback;
    migration.startBackups();
    ASSERT_EQ(6U, migration.replicaMap.size());
    // The secondary of segment 91 must be last in the list.
    EXPECT_EQ(91U, migration.replicaMap.at(5).segmentId);
}
}
