#!/bin/bash

    # enum TabletState {
    #     /// The tablet is available.
    #     NORMAL = 0,
    #     /// The tablet is being re-constructed yet. (eg. migration and recovery)
    #     NOT_READY = 1,
    #     /// Migration of tablet is requested. Cannot take new writes.
    #     LOCKED_FOR_MIGRATION = 2,
    #     /// The tablet is being migrated over by rocksteady. It can take new
    #     /// writes and reads.
    #     ROCKSTEADY_MIGRATING = 3,
    # };
    
    # In Frocksteady, the target tablet is in ROCKSTEADY_MIGRATING state
    # // Add the tablet to the target. The tablet starts off in state
    # // ROCKSTEADY_MIGRATION.
    # tabletManager->addTablet(tableId, startKeyHash, endKeyHash,
    #         TabletManager::ROCKSTEADY_MIGRATING);

    # the source tablet is in LOCKED_FOR_MIGRATION state

    # logge is logged by struct timespec (clock_gettime(CLOCK_REALTIME, &now))
    # now.tv_sec, now.tv_nsec

    # In fulva, source tablet is in state MIGRATION_SOURCE (or NORMAL state for non-migrated data)
    # target tablet is in state ROCKSTEADY_MIGRATING
    
    # Notice the --seconds parameter, it means "For doWorkload based workloads, exit benchmarks after about this many seconds."

    # Clients send multiple requests but there is only single thread managing this (calling isReady()/wait())
    # So it is thread safe to maintain a hash map in performTask()

    # In performTask(), sourceVersion and targetVersion are used to handle the deleted object case
    # If an object is deleted on the target, target will also return STATUS_OBJECT_DOESNT_EXIST

    # The state of a MigrationReadTask is initially set to be INIT.
    # We first check if the requested key value falls into the migrating tablet
    # If yes, we construct two RPCs and change the state of the MigrationReadTask to be MIGRATING
    # If not, the state of the ReadTask is set to NORMAL.
    # Notice that both isReady() and wait() will repeatedly invoke the ReadTask and use that state

    # Initially, all the requests will go to the source.
    # If the source tells me it is migrating via the respHdr (RamCloud.cc MigrationReadRpc::wait)
    # Then we put the tablet
    # (1. clean the table info on the client; 2. ask the coordinator about the tablet info of the key/value I'm requesting for)
    # Here we get the migrating tablet and add it to the tableMap.
    # Before sending a request, we check if the requested key falls into tableMap.
    # When finished, there are two cases: source replies the client STATUS_UNKNOWN_TABLET
    # or target tells the client migrating=false via the respHdr.
    # Then we remove the migrating tablet from tableMap.

    # We should let the target tell client that migration is finished instead of source
    # because during the migration the clients might always only send requests to the target thanks to the migration progress tracking

python ./scripts/backupMigration.py -r 0 --servers=4 --clients=1 --dpdkPort=0 -T basic+dpdk --superuser