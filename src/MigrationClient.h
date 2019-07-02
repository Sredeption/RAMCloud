#ifndef RAMCLOUD_MIGRATIONCLIENT_H
#define RAMCLOUD_MIGRATIONCLIENT_H

#include "Key.h"
#include "Tablet.h"
#include "RamCloud.h"
#include "Dispatch.h"

namespace RAMCloud {

class MigrationClient {
  PRIVATE:

    struct TabletKey {
        uint64_t tableId;       // tableId of the tablet
        KeyHash keyHash;        // start key hash value

        /**
         * The operator < is overridden to implement the
         * correct comparison for the tableMap.
         */
        bool operator<(const TabletKey &key) const
        {
            return tableId < key.tableId ||
                   (tableId == key.tableId && keyHash < key.keyHash);
        }
    };

  PUBLIC:

    struct MigratingTablet {
        Tablet tablet;

        ServerId sourceId;
        ServerId targetId;

        MigratingTablet(Tablet tablet, uint64_t sourceId,
                        uint64_t targetId)
            : tablet(tablet), sourceId(sourceId),
              targetId(targetId)
        {}
    };

  PRIVATE:

    RamCloud *ramcloud;
    std::map<TabletKey, MigratingTablet> tableMap;

    DISALLOW_COPY_AND_ASSIGN(MigrationClient);
  PUBLIC:

    MigrationClient(RamCloud *ramcloud);

    void putTablet(uint64_t tableId, const void *key, uint16_t keyLength,
                   uint64_t sourceId, uint64_t targetId);

    MigratingTablet *
    getTablet(uint64_t tableId, const void *key, uint16_t keyLength);

    void removeTablet(uint64_t tableId, const void *key, uint16_t keyLength);
};

template<class Normal, class Migration>
class MigrationReadTask {
  PRIVATE:
    enum State {
        INIT, NORMAL, NORMAL_WAIT, MIGRATING, MIGRATING_WAIT, DONE
    };
    RamCloud *ramcloud;
    uint64_t tableId;
    void *key;
    uint16_t keyLength;
    Buffer *value;
    const RejectRules *rejectRules;

    Tub<Normal> readRpc;
    Tub<Migration> sourceReadRpc, targetReadRpc;

    State state;
    Buffer sourceBuffer, targetBuffer;
    uint64_t finishTime;

    uint64_t version;
    bool objectExists;

    DISALLOW_COPY_AND_ASSIGN(MigrationReadTask)

  PUBLIC:
    KeyHash keyHash;

    MigrationReadTask(
        RamCloud *ramcloud, uint64_t tableId, const void *key,
        uint16_t keyLength, Buffer *value,
        const RejectRules *rejectRules = NULL)
        : ramcloud(ramcloud), tableId(tableId), key(NULL), keyLength(keyLength),
          value(value), rejectRules(rejectRules), readRpc(), sourceReadRpc(),
          targetReadRpc(), state(INIT), sourceBuffer(), targetBuffer(),
          finishTime(), version(), objectExists(false), keyHash()
    {
        this->key = std::malloc(keyLength);
        std::memcpy(this->key, key, keyLength);
        keyHash = Key::getHash(tableId, key, keyLength);
    }

    ~MigrationReadTask()
    {
        std::free(key);
    }


    void performTask()
    {
        if (state == INIT) {
            MigrationClient::MigratingTablet *migratingTablet =
                ramcloud->migrationClient->getTablet(tableId, key, keyLength);
            if (migratingTablet) {
                sourceReadRpc.construct(
                    ramcloud, migratingTablet->sourceId, tableId, key,
                    keyLength, &sourceBuffer, rejectRules);
                targetReadRpc.construct(
                    ramcloud, migratingTablet->targetId, tableId, key,
                    keyLength, &targetBuffer, rejectRules);
                state = MIGRATING;
            } else {
                readRpc.construct(ramcloud, tableId, key, keyLength, value,
                                  rejectRules);
                state = NORMAL;
            }


            KeyHash hash = Key(tableId, key, keyLength).getHash();
            if (ramcloud->lookupRegularPullProgrss(hash)) {
            } else if (ramcloud->lookupPriorityPullProgrss(hash)) {
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
            } else {
                ramcloud->clientContext->dispatch->poll();
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

                if (!migrating) {
                    ramcloud->migrationClient->removeTablet(tableId, key,
                                                            keyLength);
                }

                if (!success) {
                    state = INIT;
                    return;
                }

                value->reset();

                targetReadRpc->updateProgress();

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
                version = versionConclusion;
                objectExists = existsConclusion;
                state = MIGRATING_WAIT;
                finishTime = Cycles::rdtsc();
            } else {
                ramcloud->clientContext->dispatch->poll();
            }
        }

        if (state == MIGRATING_WAIT &&
            Cycles::toMicroseconds(Cycles::rdtsc() - finishTime) > 5) {
            state = DONE;
        }

    }

    bool isReady()
    {
        performTask();
        return state == DONE;
    }

    void wait(uint64_t *version, bool *objectExists)
    {
        while (!isReady());
        if (version)
            *version = this->version;
        if (objectExists)
            *objectExists = this->objectExists;
    }
};

}

#endif //RAMCLOUD_MIGRATIONCLIENT_H
