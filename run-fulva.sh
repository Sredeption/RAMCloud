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
    
    # In rocksteady, the target tablet is in ROCKSTEADY_MIGRATING state
    # // Add the tablet to the target. The tablet starts off in state
    # // ROCKSTEADY_MIGRATION.
    # tabletManager->addTablet(tableId, startKeyHash, endKeyHash,
    #         TabletManager::ROCKSTEADY_MIGRATING);

    # the source tablet is in LOCKED_FOR_MIGRATION state


    # logge is logged by struct timespec (clock_gettime(CLOCK_REALTIME, &now))
    # now.tv_sec, now.tv_nsec

python ./scripts/backupMigration.py -r 0 --servers=4 --clients=1 --dpdkPort=0 -T basic+dpdk --superuser