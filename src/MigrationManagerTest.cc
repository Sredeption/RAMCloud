#include "TestUtil.h"
#include "MockCluster.h"

namespace RAMCloud {
struct MigrationManagerTest : public ::testing::Test {
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    CoordinatorService *service;
    CoordinatorServerList *serverList;
    TableManager *tableManager;
    MigrationManager *mgr;
    std::mutex mutex;

    typedef std::unique_lock<std::mutex> Lock;

    MigrationManagerTest()
        : logEnabler(), context(), cluster(&context), service(), serverList(),
          tableManager(), mgr(), mutex()
    {
        service = cluster.coordinator.get();
        serverList = service->context->coordinatorServerList;
        serverList->haltUpdater();
        tableManager = service->context->tableManager;
        mgr = service->context->migrationManager;
        mgr->startMigrationsEvenIfNoThread = true;
        mgr->skipRescheduleDelay = true;

        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    ServerId addMaster(Lock &lock, ServerStatus status = ServerStatus::UP)
    {
        ServerId serverId = serverList->enlistServer(
            {WireFormat::MASTER_SERVICE}, 0, 0, "fake-locator");
        serverList->sync();
        serverList->haltUpdater();
        while (!mgr->taskQueue.isIdle())
            mgr->taskQueue.performTask();
        CoordinatorServerList::Entry *entry = serverList->getEntry(serverId);
        entry->status = status;
        return serverId;
    }

    void crashServer(Lock &lock, ServerId crashedServerId)
    {
        serverList->serverCrashed(crashedServerId);
        serverList->sync();
        serverList->haltUpdater();
        while (!mgr->taskQueue.isIdle())
            mgr->taskQueue.performTask();
    }

    DISALLOW_COPY_AND_ASSIGN(MigrationManagerTest);
};

TEST_F(MigrationManagerTest, startAndHalt)
{
    mgr->start(); // check start
    EXPECT_TRUE(mgr->thread);
    mgr->start(); // check dup start call
    EXPECT_TRUE(mgr->thread);
    mgr->halt(); // check halt
    EXPECT_FALSE(mgr->thread);
    mgr->halt(); // check dup halt call
    EXPECT_FALSE(mgr->thread);
    mgr->start(); // check restart after halt
    EXPECT_TRUE(mgr->thread);
}

TEST_F(MigrationManagerTest, startMigration)
{
    Lock lock(mutex); // For calls to internal functions without  {real lock.
    ServerId sourceId = addMaster(lock);
    addMaster(lock);
    addMaster(lock);
    ServerId targetId = addMaster(lock);

    uint64_t tableId = 0;
    tableManager->testCreateTable("t0", tableId);
    tableManager->testAddTablet(
        {tableId, 0, ~0lu, sourceId, Tablet::NORMAL, {2, 3}});

    TestLog::reset();
//    TestLog::Enable _("startMigration");
    mgr->startMigration(sourceId, targetId, tableId, 0, (~0lu) / 2);
    mgr->taskQueue.performTask();
    mgr->taskQueue.performTask();
    mgr->taskQueue.performTask();
    std::cout << TestLog::get() << std::endl;
}

}
