#include "MigrationBackupManager.h"
#include "MigrationSegmentBuilder.h"
#include "WorkerManager.h"

namespace RAMCloud {

MigrationBackupManager::Replica::Replica(const BackupStorage::FrameRef &frame,
                                         Migration *migration)
    : frame(frame), migration(migration),
      metadata(
          static_cast<const BackupReplicaMetadata *>(frame->getMetadata())),
      migrationSegment(),
      recoveryException(),
      built(),
      lastAccessTime(0),
      refCount(0),
      fetchCount(0),
      head(NULL)
{

}

MigrationBackupManager::Replica::~Replica()
{

}

void MigrationBackupManager::Replica::load()
{
    void *tryHead = NULL;
    if (!head)
        tryHead = frame->copyIfOpen();
    if (tryHead)
        head = tryHead;
    else
        frame->startLoading();
}

void MigrationBackupManager::Replica::filter()
{
    recoveryException.reset();
    migrationSegment.reset();
    std::unique_ptr<Segment> migrationSegment(new Segment());
    uint64_t start = Cycles::rdtsc();
    void *replicaData = NULL;
    if (head)
        replicaData = head;
    else
        replicaData = frame->load();

    try {
        MigrationSegmentBuilder::build(replicaData, migration->segmentSize,
                                       metadata->certificate,
                                       migrationSegment.get(),
                                       migration->tableId,
                                       migration->firstKeyHash,
                                       migration->lastKeyHash);
    } catch (const Exception &e) {
        // Can throw SegmentIteratorException or SegmentRecoveryFailedException.
        // Exception is a little broad, but it catches them both; hopefully we
        // don't try to recover from anything else too serious.
        RAMCLOUD_LOG(NOTICE, "Couldn't build migration segments for <%s,%lu>: "
                             "%s",
                     migration->sourceServerId.toString().c_str(),
                     metadata->segmentId, e.what());
        recoveryException.reset(
            new SegmentRecoveryFailedException(HERE));
        built = true;
    }
    RAMCLOUD_LOG(NOTICE,
                 "<%s,%lu> migration  segments took %lu us to construct.",
                 migration->sourceServerId.toString().c_str(),
                 metadata->segmentId,
                 Cycles::toNanoseconds(Cycles::rdtsc() - start) / 1000);
    this->migrationSegment = std::move(migrationSegment);
    Fence::sfence();
    built = true;
    lastAccessTime = Cycles::rdtsc();
    if (head)
        std::free(head);
    else
        frame->unload();
}

MigrationBackupManager::Migration::Migration(
    MigrationBackupManager *manager, uint64_t migrationId, uint64_t sourceId,
    uint64_t targetId, uint64_t tableId, uint64_t firstKeyHash,
    uint64_t lastKeyHash, const std::vector<BackupStorage::FrameRef> &frames)
    : manager(manager), context(manager->context), migrationId(migrationId),
      sourceServerId(sourceId), targetServerId(targetId), tableId(tableId),
      firstKeyHash(firstKeyHash), lastKeyHash(lastKeyHash),
      segmentSize(manager->segmentSize), phase(LOADING), replicas(),
      replicasIterator(), replicaToFilter(), segmentIdToReplica(), replicaNum(),
      completedReplicaNum(), freeLoadRpcs(), busyLoadRpcs(), freeFilterRpcs(),
      busyFilterRpcs()
{
    vector<BackupStorage::FrameRef> primaries;
    vector<BackupStorage::FrameRef> secondaries;

    for (auto &frame: frames) {
        const BackupReplicaMetadata *metadata =
            reinterpret_cast<const BackupReplicaMetadata *>(frame->getMetadata());
        if (!metadata->checkIntegrity()) {
            RAMCLOUD_LOG(NOTICE,
                         "Replica of <%s,%lu> metadata failed integrity check; "
                         "will not be used for recovery (note segment id in "
                         "this log message may be corrupted as a result)",
                         sourceServerId.toString().c_str(),
                         metadata->segmentId);
            continue;
        }
        if (metadata->logId != sourceServerId.getId())
            continue;
        (metadata->primary ? primaries : secondaries).push_back(frame);
    }

    RAMCLOUD_LOG(DEBUG,
                 "Backup preparing for migration %lu of source server %s; "
                 "loading %lu primary replicas", migrationId,
                 sourceServerId.toString().c_str(),
                 primaries.size());

    vector<BackupStorage::FrameRef>::reverse_iterator rit;
    for (rit = primaries.rbegin(); rit != primaries.rend(); ++rit) {
        replicas.emplace_back(*rit, this);
        auto &replica = replicas.back();
        segmentIdToReplica[replica.metadata->segmentId] = &replica;
    }

    replicasIterator = replicas.begin();
    replicaNum = static_cast<uint32_t>(replicas.size());

    for (uint32_t i = 0; i < MAX_PARALLEL_LOAD_RPCS; i++) {
        freeLoadRpcs.push_back(&(loadRpcs[i]));
    }

    for (uint32_t i = 0; i < MAX_PARALLEL_FILTER_RPCS; i++) {
        freeFilterRpcs.push_back(&(filterRpcs[i]));
    }

}

int MigrationBackupManager::Migration::poll()
{
    switch (phase) {
        case LOADING:
            return loadAndFilter_main();
        case SERVING:
            return 0;
        case COMPLETED:
            return 0;
    }
    return 0;
}

Status MigrationBackupManager::Migration::getSegment(
    uint64_t segmentId, Buffer *buffer, SegmentCertificate *certificate)
{

    auto replicaIt = segmentIdToReplica.find(segmentId);
    if (replicaIt == segmentIdToReplica.end()) {
        RAMCLOUD_LOG(NOTICE,
                     "Asked for a recovery segment for segment <%s,%lu> "
                     "which isn't part of this recovery",
                     sourceServerId.toString().c_str(), segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    Replica *replica = replicaIt->second;

    Fence::lfence();
    if (!replica->built) {
        if (replica->frame->isLoaded()) {
            RAMCLOUD_LOG(DEBUG,
                         "Deferring because <%s,%lu> not yet filtered: %p",
                         sourceServerId.toString().c_str(), segmentId, replica);
        } else {
            RAMCLOUD_LOG(DEBUG, "Deferring because <%s,%lu> not yet loaded: %p",
                         sourceServerId.toString().c_str(), segmentId, replica);
        }

        throw RetryException(HERE, 5000, 10000,
                             "desired segment not yet filtered");
    }

    if (replica->recoveryException) {
        auto e = SegmentRecoveryFailedException(*replica->recoveryException);
        replica->recoveryException.reset();
        throw e;
    }

    if (buffer)
        replica->migrationSegment->appendToBuffer(*buffer);
    if (certificate)
        replica->migrationSegment->getAppendedLength(certificate);

//    replica->migrationSegment.release();

    replica->fetchCount++;
    return STATUS_OK;
}

int MigrationBackupManager::Migration::loadAndFilter_main()
{
    int workPerformed = 0;
    workPerformed += loadAndFilter_reapLoadRpcs();
    workPerformed += loadAndFilter_reapFilterRpcs();

    if (completedReplicaNum == replicaNum && phase < SERVING) {
        phase = SERVING;
        RAMCLOUD_LOG(NOTICE, "migration %lu finish loading", migrationId);
    }

    workPerformed += loadAndFilter_sendLoadRpcs();
    workPerformed += loadAndFilter_sendFilterRpcs();
    return workPerformed;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_reapLoadRpcs()
{
    size_t numBusyLoadRpcs = busyLoadRpcs.size();
    for (size_t i = 0; i < numBusyLoadRpcs; i++) {
        Tub<LoadRpc> *loadRpc = busyLoadRpcs.front();
        if ((*loadRpc)->isReady()) {
            Replica *replica = (*loadRpc)->replica;
            RAMCLOUD_LOG(DEBUG, "load segmented %lu in memory",
                         replica->metadata->segmentId);
            replicaToFilter.push_back(replica);
            (*loadRpc).destroy();
            freeLoadRpcs.push_back(loadRpc);

        } else {
            busyLoadRpcs.push_back(loadRpc);
        }

        busyLoadRpcs.pop_front();

    }
    return 0;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_reapFilterRpcs()
{
    int workPerformed = 0;
    size_t numBusyFilterRpcs = busyFilterRpcs.size();
    for (size_t i = 0; i < numBusyFilterRpcs; i++) {
        Tub<FilterRpc> *filterRpc = busyFilterRpcs.front();
        if ((*filterRpc)->isReady()) {

            Replica *replica = (*filterRpc)->replica;

            RAMCLOUD_LOG(DEBUG, "filter segmented %lu in memory",
                         replica->metadata->segmentId);
            (*filterRpc).destroy();
            completedReplicaNum++;
            workPerformed++;
            freeFilterRpcs.push_back(filterRpc);
        } else {
            busyFilterRpcs.push_back(filterRpc);
        }

        busyFilterRpcs.pop_front();
    }
    return workPerformed;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_sendLoadRpcs()
{
    int workPerformed = 0;
    while (!freeLoadRpcs.empty() && replicasIterator != replicas.end()) {
        Tub<LoadRpc> *loadRpc = freeLoadRpcs.front();

        Replica &replica = *replicasIterator;
        loadRpc->construct(manager->localLocator, &replica);
        context->workerManager->handleRpc(loadRpc->get());
        workPerformed++;

        replicasIterator++;
        freeLoadRpcs.pop_front();
        busyLoadRpcs.push_back(loadRpc);
    }
    return workPerformed;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_sendFilterRpcs()
{
    int workPerformed = 0;
    while (!freeFilterRpcs.empty() && !replicaToFilter.empty()) {
        Tub<FilterRpc> *filterRpc = freeFilterRpcs.front();
        Replica *replica = replicaToFilter.front();

        RAMCLOUD_LOG(DEBUG, "send segmented %lu to filter",
                     replica->metadata->segmentId);
        filterRpc->construct(manager->localLocator, replica);

        busyFilterRpcs.push_back(filterRpc);
        context->workerManager->handleRpc(filterRpc->get());
        workPerformed++;

        freeFilterRpcs.pop_front();
        replicaToFilter.pop_front();
    }
    return workPerformed;
}


MigrationBackupManager::MigrationBackupManager(
    Context *context, string localLocator, uint32_t segmentSize)
    : Dispatch::Poller(context->dispatch, "MigrationBackupManager"),
      context(context), localLocator(localLocator), segmentSize(segmentSize),
      migrationsInProgress(), migrationMap()
{
}

int MigrationBackupManager::poll()
{
    int workPerformed = 0;

    for (auto migration = migrationsInProgress.begin();
         migration != migrationsInProgress.end();) {
        Migration *currentMigration = *migration;

        workPerformed += currentMigration->poll();

        if (currentMigration->phase == Migration::COMPLETED) {
            migration = migrationsInProgress.erase(migration);
            delete currentMigration;
        } else {
            migration++;
        }
    }

    return workPerformed == 0 ? 0 : 1;
}

void MigrationBackupManager::start(
    uint64_t migrationId, uint64_t sourceId, uint64_t targetId,
    uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
    const std::vector<BackupStorage::FrameRef> &frames)
{
    Migration *migration = new Migration(
        this, migrationId, sourceId, targetId, tableId, firstKeyHash,
        lastKeyHash, frames);

    migrationsInProgress.push_back(migration);
    migrationMap[migrationId] = migration;

}

Status MigrationBackupManager::getSegment(
    uint64_t migrationId, uint64_t segmentId, Buffer *buffer,
    SegmentCertificate *certificate)
{

    auto migrationIt = migrationMap.find(migrationId);
    if (migrationIt == migrationMap.end()) {
        RAMCLOUD_LOG(NOTICE,
                     "couldn't find migration %lu", migrationId);
        throw BackupBadSegmentIdException(HERE);
    }

    migrationIt->second->getSegment(segmentId, buffer, certificate);

    return STATUS_OK;
}


}
