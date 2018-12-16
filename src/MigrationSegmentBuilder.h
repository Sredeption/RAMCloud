
#ifndef RAMCLOUD_MIGRATIONSEGMENTBUILDER_H
#define RAMCLOUD_MIGRATIONSEGMENTBUILDER_H

#include "MigrationPartition.pb.h"
#include "Common.h"
#include "Segment.h"
#include "Key.h"

namespace RAMCloud {

class MigrationSegmentBuilder {
  PUBLIC:

    static void build(const void *buffer, uint32_t length,
                      const SegmentCertificate &certificate,
                      int numPartitions,
                      const ProtoBuf::MigrationPartition &partitions,
                      Segment *recoverySegments);

    static bool extractDigest(const void *buffer, uint32_t length,
                              const SegmentCertificate &certificate,
                              Buffer *digestBuffer, Buffer *tableStatsBuffer);

  PRIVATE:

    static bool isEntryAlive(const LogPosition &position,
                             const ProtoBuf::Tablets::Tablet *tablet);

    static const ProtoBuf::Tablets::Tablet *
    whichPartition(uint64_t tableId, KeyHash keyHash,
                   const ProtoBuf::MigrationPartition &partitions);

    // Disallow construction.
    MigrationSegmentBuilder()
    {}
};


}
#endif //RAMCLOUD_MIGRATIONSEGMENTBUILDER_H
