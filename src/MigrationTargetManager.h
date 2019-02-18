#ifndef RAMCLOUD_MIGRATIONTARGETMANAGER_H
#define RAMCLOUD_MIGRATIONTARGETMANAGER_H

#include "Common.h"
#include "Key.h"
#include "RangeList.h"
#include "Dispatch.h"
#include "TabletManager.h"
#include "ObjectManager.h"

namespace RAMCloud {


class MigrationTargetManager : public Dispatch::Poller {
  PRIVATE:

    class Migration;

    class FinishNotifier {
      PUBLIC:

        virtual ~FinishNotifier()
        {}

        virtual void notify(Migration *migration) = 0;

        virtual bool isSent() = 0;

        virtual bool isReady() = 0;

        virtual bool wait() = 0;

        virtual FinishNotifier *clone() = 0;

    };

    class Migration {
      PUBLIC:

        struct Replica {
            Replica(uint64_t backupId, uint64_t segmentId);

            ServerId backupId;
            uint64_t segmentId;
        };

        Migration(Context *context, Buffer *payload,
                  FinishNotifier *finishNotifier);

        int poll();

      PRIVATE:
        Context *context;
        string localLocator;
        uint64_t migrationId;
        vector<Replica> replicas;
        vector<Replica>::iterator replicaIterator;
        RangeList rangeList;
        uint64_t tableId;
        uint64_t firstKeyHash;
        uint64_t lastKeyHash;
        ServerId sourceServerId;
        ServerId targetServerId;
        TabletManager *tabletManager;
        ObjectManager *objectManager;

        enum MigrationPhase {
            SETUP,

            MIGRATING_DATA,

            SIDE_LOG_COMMIT,

            COMPLETED
        };

        MigrationPhase phase;

        static const uint32_t PIPELINE_DEPTH = 16;

        Tub<Buffer> rpcBuffers[PIPELINE_DEPTH];

        std::deque<Tub<Buffer> *> freePullBuffers;

        std::deque<Tub<Buffer> *> freeReplayBuffers;

        class PullRpc {
          PUBLIC:

            PullRpc(Context *context, ServerId backupServerId,
                    uint64_t migrationId, ServerId sourceServerId,
                    uint64_t segmentId, Tub<Buffer> *response)
                : segmentId(segmentId), responseBuffer(response), rpc()
            {
                rpc.construct(context, backupServerId, migrationId,
                              sourceServerId, segmentId, response->get());
            }

          PRIVATE:
            uint64_t segmentId;

            Tub<Buffer> *responseBuffer;

            Tub<MigrationGetDataRpc> rpc;

            friend class Migration;
            DISALLOW_COPY_AND_ASSIGN(PullRpc);

        };

        static const uint32_t MAX_PARALLEL_PULL_RPCS = 8;

        Tub<PullRpc> pullRpcs[MAX_PARALLEL_PULL_RPCS];

        std::deque<Tub<PullRpc> *> freePullRpcs;

        std::deque<Tub<PullRpc> *> busyPullRpcs;


        class ReplayRpc : public Transport::ServerRpc {
          PUBLIC:

            explicit ReplayRpc(Tub<Buffer> *response, Tub<SideLog> *sideLog,
                               string localLocator,
                               SegmentCertificate certificate)
                : responseBuffer(response),
                  sideLog(sideLog), completed(false), localLocator(localLocator)
            {
                WireFormat::MigrationReplay::Request *reqHdr =
                    requestPayload.emplaceAppend<
                        WireFormat::MigrationReplay::Request>();

                reqHdr->common.opcode =
                    WireFormat::MigrationReplay::opcode;
                reqHdr->common.service =
                    WireFormat::MigrationReplay::service;

                reqHdr->bufferPtr = reinterpret_cast<uintptr_t>(responseBuffer);
                reqHdr->sideLogPtr = reinterpret_cast<uintptr_t>(sideLog);
                reqHdr->certificate = certificate;
            }

            ~ReplayRpc()
            {}

            void sendReply()
            {
                completed = true;
            }

            string getClientServiceLocator()
            {
                return this->localLocator;
            }

            bool isReady()
            {
                return completed;
            }

          PRIVATE:
            Tub<Buffer> *responseBuffer;

            Tub<SideLog> *sideLog;

            bool completed;

            const string localLocator;

            friend class Migration;
            DISALLOW_COPY_AND_ASSIGN(ReplayRpc);
        };

        static const uint32_t MAX_PARALLEL_REPLAY_RPCS = 6;
        Tub<ReplayRpc> replayRpcs[MAX_PARALLEL_REPLAY_RPCS];
        std::deque<Tub<ReplayRpc> *> freeReplayRpcs;
        std::deque<Tub<ReplayRpc> *> busyReplayRpcs;
        Tub<SideLog> sideLogs[MAX_PARALLEL_REPLAY_RPCS];
        std::deque<Tub<SideLog> *> freeSideLogs;

        class SideLogCommitRpc : public Transport::ServerRpc {
          public:
            explicit SideLogCommitRpc(Tub<SideLog> *sideLog,
                                      string localLocator)
                : sideLog(sideLog), completed(false), localLocator(localLocator)
            {
                WireFormat::MigrationSideLogCommit::Request *reqHdr =
                    requestPayload.emplaceAppend<
                        WireFormat::MigrationSideLogCommit::Request>();

                reqHdr->common.opcode =
                    WireFormat::MigrationSideLogCommit::opcode;
                reqHdr->common.service =
                    WireFormat::MigrationSideLogCommit::service;

                reqHdr->sideLogPtr = reinterpret_cast<uintptr_t>(sideLog);
            }

            ~SideLogCommitRpc()
            {}

            void sendReply()
            {
                completed = true;
            }

            bool isReady()
            {
                return completed;
            }

            string getClientServiceLocator()
            {
                return localLocator;
            }

          PRIVATE:
            Tub<SideLog> *sideLog;

            bool completed;

            const string localLocator;

            friend class Migration;
            DISALLOW_COPY_AND_ASSIGN(SideLogCommitRpc);
        };

        Tub<SideLogCommitRpc> sideLogCommitRpc;

        uint64_t totalReplayedBytes;
        uint32_t numReplicas;
        uint32_t numCompletedReplicas;

        uint64_t migrationStartTS;
        uint64_t migrationEndTS;
        double migratedMegaBytes;
        uint64_t sideLogCommitStartTS;
        uint64_t sideLogCommitEndTS;

        std::unique_ptr<FinishNotifier> finishNotifier;

        Tub<ObjectManager::TombstoneProtector> tombstoneProtector;

        int prepare();

        int pullAndReplay_main();

        int pullAndReplay_reapPullRpcs();

        int pullAndReplay_reapReplayRpcs();

        int pullAndReplay_sendPullRpcs();

        int pullAndReplay_sendReplayRpcs();

        int sideLogCommit();

        friend class MigrationTargetManager;
        DISALLOW_COPY_AND_ASSIGN(Migration);
    };

    Context *context;
    std::unordered_map<uint64_t, Migration *> migrations;
    std::vector<Migration *> migrationsInProgress;
    SpinLock lock;

    DISALLOW_COPY_AND_ASSIGN(MigrationTargetManager);
  PUBLIC:

    MigrationTargetManager(Context *context);

    int poll();

    void startMigration(uint64_t migrationId, Buffer *payload);

    bool isLocked(uint64_t migrationId, Key &key);

    void update(uint64_t migrationId,
                vector<WireFormat::MigrationIsLocked::Range> &ranges);

    Migration *getMigration(uint64_t migrationId);


    class RealFinishNotifier : public FinishNotifier {

        Tub<MigrationFinishedRpc> rpc;
      PUBLIC:

        RealFinishNotifier();

        virtual ~RealFinishNotifier();

        void notify(Migration *migration);

        bool isSent();

        bool isReady();

        bool wait();

        FinishNotifier *clone();
    };

    std::unique_ptr<FinishNotifier> finishNotifier;

    bool disableMigrationRecover;
    bool polling;


};

}


#endif //RAMCLOUD_MIGRATIONTARGETMANAGER_H
