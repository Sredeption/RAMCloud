#ifndef RAMCLOUD_MIGRATIONCLIENT_H
#define RAMCLOUD_MIGRATIONCLIENT_H

#include "Common.h"
#include "Key.h"
#include "Tablet.h"

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

    struct TabletWithLocator {
        /// Details about the tablet.
        Tablet tablet;

        /// Used to find the server that stores the tablet.
        string sourceLocator;
        string targetLocator;

        /// Session corresponding to serviceLocator. This is a cache to avoid
        /// repeated calls to TransportManager; NULL means that we haven't
        /// yet fetched the session from TransportManager.
        Transport::SessionRef sourceSession;
        Transport::SessionRef targetSession;


        TabletWithLocator(Tablet tablet, string sourceLocator,
                          string targetLocator)
            : tablet(tablet), sourceLocator(sourceLocator),
              targetLocator(targetLocator), sourceSession(NULL),
              targetSession(NULL)
        {}
    };

    RamCloud *ramcloud;
    std::map<TabletKey, TabletWithLocator> tableMap;

    DISALLOW_COPY_AND_ASSIGN(MigrationClient);
  PUBLIC:

    MigrationClient(RamCloud *ramcloud);

    void putTablet(Tablet &tablet, uint32_t sourceId, uint32_t targetId);
};

}

#endif //RAMCLOUD_MIGRATIONCLIENT_H
