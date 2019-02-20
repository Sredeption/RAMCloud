#include "MigrationTargetManager.h"
#include "MasterService.h"
#include "WorkerManager.h"

namespace RAMCloud {

MigrationTargetManager::MigrationTargetManager(Context *context) :
    Dispatch::Poller(context->dispatch, "MigrationTargetManager"),
    context(context), migrations(), migrationsInProgress(),
    lock("MigrationTargetManager"), finishNotifier(new RealFinishNotifier()),
    disableMigrationRecover(false), polling(false)
{
}

void
MigrationTargetManager::startMigration(uint64_t migrationId, Buffer *payload)
{
    Migration *migration = new Migration(
        context, payload, finishNotifier->clone());
    migrations[migrationId] = migration;
    migrationsInProgress.push_back(migration);
    RAMCLOUD_LOG(NOTICE, "Start migration %lu in Target", migrationId);
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
    std::unordered_map<uint64_t, Migration *>::iterator migrationPair =
        migrations.find(migrationId);
    if (migrationPair == migrations.end())
        return NULL;
    return migrationPair->second;
}

int MigrationTargetManager::poll()
{

    int workPerformed = 0;
    if (polling)
        return workPerformed;
    polling = true;
    for (auto migration = migrationsInProgress.begin();
         migration != migrationsInProgress.end();) {
        Migration *currentMigration = *migration;


        workPerformed += currentMigration->poll();

        if (currentMigration->phase == Migration::COMPLETED) {
            migration = migrationsInProgress.erase(migration);
//            migrations.erase(currentMigration->migrationId);
            delete currentMigration;
        } else {
            migration++;
        }
    }

    polling = false;
    return workPerformed == 0 ? 0 : 1;
}

MigrationTargetManager::Migration::Migration(
    Context *context, Buffer *payload, FinishNotifier *finishNotifier)
    : context(context), localLocator(), migrationId(), replicas(),
      replicaIterator(), rangeList(), tableId(), firstKeyHash(),
      lastKeyHash(), sourceServerId(),
      targetServerId(), tabletManager(), objectManager(), phase(SETUP),
      freePullBuffers(), freeReplayBuffers(), freePullRpcs(), busyPullRpcs(),
      freeReplayRpcs(), busyReplayRpcs(), freeSideLogs(), sideLogCommitRpc(),
      totalReplayedBytes(0), numReplicas(), numCompletedReplicas(0),
      migrationStartTS(), migrationEndTS(), migratedMegaBytes(),
      sideLogCommitStartTS(), sideLogCommitEndTS(),
      finishNotifier(finishNotifier), tombstoneProtector()
{
    WireFormat::MigrationTargetStart::Request *reqHdr =
        payload->getOffset<WireFormat::MigrationTargetStart::Request>(0);

    migrationId = reqHdr->migrationId;
    tableId = reqHdr->tableId;
    firstKeyHash = reqHdr->firstKeyHash;
    lastKeyHash = reqHdr->lastKeyHash;
    sourceServerId = ServerId(reqHdr->sourceServerId);
    targetServerId = ServerId(reqHdr->targetServerId);
    numReplicas = reqHdr->numReplicas;

    MasterService *masterService = context->getMasterService();
    tabletManager = &(masterService->tabletManager);
    objectManager = &(masterService->objectManager);
    tabletManager->raiseSafeVersion(reqHdr->safeVersion);
    objectManager->raiseSafeVersion(reqHdr->safeVersion);
    localLocator = masterService->config->localLocator;

    replicas.reserve(numReplicas);
    uint32_t offset = sizeof32(WireFormat::MigrationTargetStart::Request);
    for (uint32_t i = 0; i < numReplicas; ++i) {
        const WireFormat::MigrationTargetStart::Replica *replicaLocation =
            payload->getOffset<WireFormat::MigrationTargetStart::Replica>(
                offset);
        offset += sizeof32(WireFormat::MigrationTargetStart::Replica);
        Replica replica(replicaLocation->backupId, replicaLocation->segmentId);
        replicas.push_back(replica);
    }

    RAMCLOUD_LOG(WARNING, "replicas number: %u", numReplicas);
    replicaIterator = replicas.begin();
    //TODO: handle log head
    RAMCLOUD_CLOG(WARNING, "skip segment %lu", replicaIterator->segmentId);
    replicaIterator++;
    numReplicas--;
    RAMCLOUD_CLOG(WARNING, "skip segment %lu", replicaIterator->segmentId);
    replicaIterator++;
    numReplicas--;

    for (uint32_t i = 0; i < PIPELINE_DEPTH; i++) {
        freePullBuffers.push_back(&(rpcBuffers[i]));
    }

    for (uint32_t i = 0; i < MAX_PARALLEL_PULL_RPCS; i++) {
        freePullRpcs.push_back(&(pullRpcs[i]));
    }

    // To begin with, all replay rpcs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        freeReplayRpcs.push_back(&(replayRpcs[i]));
    }

    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        sideLogs[i].construct(objectManager->getLog());
    }

    // To begin with, all sidelogs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        freeSideLogs.push_back(&(sideLogs[i]));
    }

    migrationStartTS = Cycles::rdtsc();
}

int MigrationTargetManager::Migration::poll()
{
    switch (phase) {
        case SETUP :
            return prepare();

        case MIGRATING_DATA :
            return pullAndReplay_main();

        case SIDE_LOG_COMMIT :
            return sideLogCommit();

        case COMPLETED :
            return 0;

        default :
            return 0;
    }
}

int MigrationTargetManager::Migration::prepare()
{
    bool added = tabletManager->addTablet(
        tableId,
        firstKeyHash,
        lastKeyHash,
        TabletManager::MIGRATION_TARGET);
    if (!added) {
        throw Exception(HERE,
                        format("Cannot recover tablet that overlaps "
                               "an already existing one (tablet to recover: %lu "
                               "range [0x%lx,0x%lx], current tablet map: %s)",
                               tableId,
                               firstKeyHash, lastKeyHash,
                               tabletManager->toString().c_str()));
    }
    tabletManager->migrateTablet(
        tableId, firstKeyHash, lastKeyHash, migrationId, sourceServerId.getId(),
        targetServerId.getId(), TabletManager::MIGRATION_TARGET);

    phase = MIGRATING_DATA;
    if (!tombstoneProtector)
        tombstoneProtector.construct(objectManager);

    RAMCLOUD_LOG(NOTICE, "Migration %lu start serving at Target",
                 migrationId);
    return 1;
}

int MigrationTargetManager::Migration::pullAndReplay_main()
{
    int workDone = 0;

    workDone += pullAndReplay_reapPullRpcs();

    workDone += pullAndReplay_reapReplayRpcs();

    if (numCompletedReplicas == numReplicas) {
        migrationEndTS = Cycles::rdtsc();
        sideLogCommitStartTS = migrationEndTS;

        double migrationSeconds = Cycles::toSeconds(migrationEndTS -
                                                    migrationStartTS);

        migratedMegaBytes =
            static_cast<double>(totalReplayedBytes) / (1024. * 1024.);

        RAMCLOUD_LOG(WARNING,
                     "Migration has completed. Changing state to state to"
                     " SIDE_LOG_COMMIT (Tablet[0x%lx, 0x%lx] in table %lu)."
                     " Moving %.2f MB of data over took %.2f seconds (%.2f MB/s)",
                     firstKeyHash, lastKeyHash, tableId, migratedMegaBytes,
                     migrationSeconds, migratedMegaBytes / migrationSeconds);

        bool changed = tabletManager->changeState(
            tableId, firstKeyHash, lastKeyHash,
            TabletManager::MIGRATION_TARGET, TabletManager::NORMAL);
        if (!changed) {
            throw FatalError(
                HERE, format("Could not change recovering "
                             "tablet's state to NORMAL (%lu range [%lu,%lu])",
                             tableId, firstKeyHash, lastKeyHash));
        }
        phase = SIDE_LOG_COMMIT;

        return workDone;

    }

    workDone += pullAndReplay_sendPullRpcs();

    workDone += pullAndReplay_sendReplayRpcs();
    return workDone;
}

__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_reapPullRpcs()
{
    int workDone = 0;
    size_t numBusyPullRpcs = busyPullRpcs.size();

    for (size_t i = 0; i < numBusyPullRpcs; i++) {
        Tub<PullRpc> *pullRpc = busyPullRpcs.front();
        if ((*pullRpc)->rpc->isReady()) {
            RAMCLOUD_LOG(NOTICE, "pulled segment %lu", (*pullRpc)->segmentId);
            (*pullRpc)->rpc->wait();
            freeReplayBuffers.push_back((*pullRpc)->responseBuffer);
            (*pullRpc).destroy();

            freePullRpcs.push_back(pullRpc);
            workDone++;
        } else {
            busyPullRpcs.push_back(pullRpc);
        }
        busyPullRpcs.pop_front();
    }
    return 0;
}

__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_reapReplayRpcs()
{
    int workDone = 0;

    size_t numBusyReplayRpcs = busyReplayRpcs.size();

    for (size_t i = 0; i < numBusyReplayRpcs; i++) {
        Tub<ReplayRpc> *replayRpc = busyReplayRpcs.front();

        if ((*replayRpc)->isReady()) {
            Tub<Buffer> *responseBuffer = (*replayRpc)->responseBuffer;
            Tub<SideLog> *sideLog = (*replayRpc)->sideLog;
            uint32_t numReplayedBytes = (*responseBuffer)->size();
            totalReplayedBytes += numReplayedBytes;
            (*responseBuffer).destroy();
            freePullBuffers.push_back(responseBuffer);
            freeSideLogs.push_back(sideLog);

            (*replayRpc).destroy();
            freeReplayRpcs.push_back(replayRpc);
            numCompletedReplicas++;
            RAMCLOUD_LOG(NOTICE, "replay progress: %u/%u",
                         numCompletedReplicas, numReplicas);
            workDone++;
        } else {
            busyReplayRpcs.push_back(replayRpc);
        }
        busyReplayRpcs.pop_front();
    }
    return 0;
}

__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_sendPullRpcs()
{
    int workDone = 0;
    while (!freePullRpcs.empty() && !freePullBuffers.empty() &&
           replicaIterator != replicas.end()) {
        Tub<PullRpc> *pullRpc = freePullRpcs.front();

        Tub<Buffer> *responseBuffer = freePullBuffers.front();
        responseBuffer->construct();
        pullRpc->construct(context, replicaIterator->backupId, migrationId,
                           sourceServerId, replicaIterator->segmentId,
                           responseBuffer);
        RAMCLOUD_LOG(DEBUG, "fetch segment %lu", replicaIterator->segmentId);
        replicaIterator++;

        freePullBuffers.pop_front();
        freePullRpcs.pop_front();
        busyPullRpcs.push_back(pullRpc);
        workDone++;
    }

    return workDone;
}

__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_sendReplayRpcs()
{

    int workDone = 0;

    while (!freeReplayRpcs.empty() && !freeReplayBuffers.empty()) {
        Tub<ReplayRpc> *replayRpc = freeReplayRpcs.front();
        Tub<SideLog> *sideLog = freeSideLogs.front();
        Tub<Buffer> *responseBuffer = freeReplayBuffers.front();
        void *respHdr = (*responseBuffer)->getRange(0, sizeof32(
            WireFormat::MigrationGetData::Response));

        SegmentCertificate certificate =
            (reinterpret_cast<
                WireFormat::MigrationGetData::Response *>(
                respHdr))->certificate;
        (*responseBuffer)->truncateFront(
            sizeof32(WireFormat::MigrationGetData::Response));

        (*replayRpc).construct(responseBuffer, sideLog,
                               localLocator, certificate);
        context->workerManager->handleRpc(replayRpc->get());

        freeReplayBuffers.pop_front();
        freeSideLogs.pop_front();
        busyReplayRpcs.push_back(replayRpc);
        freeReplayRpcs.pop_front();
        workDone++;
    }
    return workDone;
}

int MigrationTargetManager::Migration::sideLogCommit()
{

    int workDone = 0;

    if (finishNotifier->isSent()) {
        if (finishNotifier->isReady()) {
            finishNotifier->wait();
            if (tombstoneProtector)
                tombstoneProtector.destroy();
            RAMCLOUD_LOG(WARNING, "SideLog commit finish, migration %lu "
                                  "is over", migrationId);
            phase = COMPLETED;
            return 1;
        }
        return 0;
    }

    if (sideLogCommitRpc) {
        if (sideLogCommitRpc->isReady()) {
            sideLogCommitRpc.destroy();
            workDone++;
        }
    }

    if (!sideLogCommitRpc) {
        if (freeSideLogs.empty()) {
            sideLogCommitEndTS = Cycles::rdtsc();
            finishNotifier->notify(this);
            RAMCLOUD_LOG(WARNING, "SideLog commit finish, notifying");
        } else {
            sideLogCommitRpc.construct(freeSideLogs.front(), localLocator);
            context->workerManager->handleRpc(sideLogCommitRpc.get());
            freeSideLogs.pop_front();
        }
        workDone++;
    }
    return workDone;
}

MigrationTargetManager::Migration::Replica::Replica(
    uint64_t backupId, uint64_t segmentId)
    : backupId(backupId), segmentId(segmentId)
{

}


MigrationTargetManager::RealFinishNotifier::RealFinishNotifier()
    : rpc()
{
}

MigrationTargetManager::RealFinishNotifier::~RealFinishNotifier()
{
}

void MigrationTargetManager::RealFinishNotifier::notify(Migration *migration)
{
    rpc.construct(migration->context, migration->migrationId,
                  migration->targetServerId, true);
}

bool MigrationTargetManager::RealFinishNotifier::isSent()
{
    return rpc;
}

bool MigrationTargetManager::RealFinishNotifier::isReady()
{
    if (rpc)
        return rpc->isReady();
    else
        return false;
}

bool MigrationTargetManager::RealFinishNotifier::wait()
{
    if (rpc)
        return rpc->wait();
    return false;

}

MigrationTargetManager::FinishNotifier *
MigrationTargetManager::RealFinishNotifier::clone()
{
    return new RealFinishNotifier();
}


}
