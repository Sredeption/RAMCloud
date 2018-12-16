/* Copyright (c) 2009-2016 Stanford University
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

#include "MigrationSegmentBuilder.h"
#include "SegmentIterator.h"
#include "ShortMacros.h"
#include "ParticipantList.h"
#include "Object.h"
#include "RpcResult.h"
#include "PreparedOp.h"
#include "TxDecisionRecord.h"

namespace RAMCloud {

void
MigrationSegmentBuilder::build(const void *buffer, uint32_t length,
                               const SegmentCertificate &certificate,
                               int numPartitions,
                               const ProtoBuf::MigrationPartition &partitions,
                               Segment *recoverySegments)
{
    SegmentIterator it(buffer, length, certificate);
    it.checkMetadataIntegrity();

    // Buffer must be retained for iteration to provide storage for header.
    Buffer headerBuffer;
    const SegmentHeader *header = NULL;
    for (; !it.isDone(); it.next()) {
        LogEntryType type = it.getType();

        if (type == LOG_ENTRY_TYPE_SEGHEADER) {
            it.appendToBuffer(headerBuffer);
            header = headerBuffer.getStart<SegmentHeader>();
            continue;
        }
        if (type != LOG_ENTRY_TYPE_OBJ && type != LOG_ENTRY_TYPE_OBJTOMB
            && type != LOG_ENTRY_TYPE_SAFEVERSION
            && type != LOG_ENTRY_TYPE_RPCRESULT
            && type != LOG_ENTRY_TYPE_PREP
            && type != LOG_ENTRY_TYPE_PREPTOMB
            && type != LOG_ENTRY_TYPE_TXDECISION
            && type != LOG_ENTRY_TYPE_TXPLIST)
            continue;

        if (header == NULL) {
            RAMCLOUD_DIE("Found log entry before header while "
                         "building recovery segments");
        }

        Buffer entryBuffer;
        it.appendToBuffer(entryBuffer);

        uint64_t tableId = -1;
        KeyHash keyHash = -1;
        if (type == LOG_ENTRY_TYPE_SAFEVERSION) {
            // Copy SAFEVERSION to all the partitions for safeVersion recovery
            // on all recovery masters
            LogPosition position(header->segmentId, it.getOffset());
            for (int i = 0; i < numPartitions; i++) {
                if (!recoverySegments[i].append(type, entryBuffer)) {
                        LOG(WARNING, "Failure appending to a recovery segment "
                                     "for a replica of <%s,%lu>",
                            ServerId(header->logId).toString().c_str(),
                            header->segmentId);
                    throw SegmentRecoveryFailedException(HERE);
                }
            }
            continue;
        }

        if (type == LOG_ENTRY_TYPE_TXPLIST) {
            // Copy ParticipantLists all partitions that should own the entry.
            ParticipantList plist(entryBuffer);
            for (uint32_t i = 0; i < plist.getParticipantCount(); ++i) {
                tableId = plist.participants[i].tableId;
                keyHash = plist.participants[i].keyHash;
                const auto *partition =
                    whichPartition(tableId, keyHash, partitions);
                if (partition) {
                    uint64_t partitionId = partition->user_data();

                    LogPosition position(header->segmentId, it.getOffset());
                    if (!recoverySegments[partitionId].append(type,
                                                              entryBuffer)) {
                            LOG(WARNING,
                                "Failure appending to a recovery segment "
                                "for a replica of <%s,%lu>",
                                ServerId(header->logId).toString().c_str(),
                                header->segmentId);
                        throw SegmentRecoveryFailedException(HERE);
                    }
                }
            }
            continue;
        }

        if (type == LOG_ENTRY_TYPE_OBJ) {
            Object object(entryBuffer);
            tableId = object.getTableId();
            keyHash = Key::getHash(tableId,
                                   object.getKey(), object.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            ObjectTombstone tomb(entryBuffer);
            tableId = tomb.getTableId();
            keyHash = Key::getHash(tableId,
                                   tomb.getKey(), tomb.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_RPCRESULT) {
            RpcResult rpcResult(entryBuffer);
            tableId = rpcResult.getTableId();
            keyHash = rpcResult.getKeyHash();
        } else if (type == LOG_ENTRY_TYPE_PREP) {
            PreparedOp op(entryBuffer, 0, entryBuffer.size());
            tableId = op.object.getTableId();
            keyHash = Key::getHash(tableId,
                                   op.object.getKey(),
                                   op.object.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_PREPTOMB) {
            PreparedOpTombstone opTomb(entryBuffer, 0);
            tableId = opTomb.header.tableId;
            keyHash = opTomb.header.keyHash;
        } else if (type == LOG_ENTRY_TYPE_TXDECISION) {
            TxDecisionRecord decisionRecord(entryBuffer);
            tableId = decisionRecord.getTableId();
            keyHash = decisionRecord.getKeyHash();
        } else {
                LOG(WARNING, "Unknown LogEntry (id=%u)", type);
            throw SegmentRecoveryFailedException(HERE);
        }

        const auto *partition = whichPartition(tableId, keyHash, partitions);
        if (!partition) {
            // This log record doesn't belong to any of the current
            // partitions. This can happen when it takes several passes
            // to complete a recovery: each pass will recover only a subset
            // of the data.
            TEST_LOG("Couldn't place object");
            continue;
        }
        uint64_t partitionId = partition->user_data();

        LogPosition position(header->segmentId, it.getOffset());
        if (!isEntryAlive(position, partition)) {
                LOG(NOTICE, "Skipping object with <tableId, keyHash> of "
                            "<%lu,%lu> because it appears to have existed prior "
                            "to this tablet's creation.", tableId, keyHash);
            continue;
        }

        if (!recoverySegments[partitionId].append(type, entryBuffer)) {
                LOG(WARNING, "Failure appending to a recovery segment "
                             "for a replica of <%s,%lu>",
                    ServerId(header->logId).toString().c_str(),
                    header->segmentId);
            throw SegmentRecoveryFailedException(HERE);
        }
    }
}

bool MigrationSegmentBuilder::extractDigest(const void *buffer,
                                            uint32_t length,
                                            const SegmentCertificate &certificate,
                                            Buffer *digestBuffer,
                                            Buffer *tableStatsBuffer)
{
    // If the Segment is malformed somehow, just ignore it. The
    // coordinator will have to deal.
    SegmentIterator it(buffer, length, certificate);
    bool foundDigest = false;
    bool foundTableStats = false;
    try {
        it.checkMetadataIntegrity();
    } catch (SegmentIteratorException &e) {
            LOG(NOTICE,
                "Replica failed integrity check; skipping extraction of "
                "log digest: %s", e.str().c_str());
        return false;
    }
    while (!it.isDone()) {
        if (it.getType() == LOG_ENTRY_TYPE_LOGDIGEST) {
            digestBuffer->reset();
            it.appendToBuffer(*digestBuffer);
            foundDigest = true;
        }
        if (it.getType() == LOG_ENTRY_TYPE_TABLESTATS) {
            tableStatsBuffer->reset();
            it.appendToBuffer(*tableStatsBuffer);
            foundTableStats = true;
        }
        if (foundDigest && foundTableStats) {
            return true;
        }
        it.next();
    }
    return foundDigest;
}

bool MigrationSegmentBuilder::isEntryAlive(
    const LogPosition &position,
    const ProtoBuf::Tablets::Tablet *tablet)
{
    LogPosition minimum(tablet->ctime_log_head_id(),
                        tablet->ctime_log_head_offset());
    return position >= minimum;
}

const ProtoBuf::Tablets::Tablet *
MigrationSegmentBuilder::whichPartition(uint64_t tableId,
                                        KeyHash keyHash,
                                        const ProtoBuf::MigrationPartition &partitions)
{
    for (int i = 0; i < partitions.tablet_size(); i++) {
        const ProtoBuf::Tablets::Tablet &tablet(partitions.tablet(i));
        if (tablet.table_id() == tableId &&
            (tablet.start_key_hash() <= keyHash &&
             tablet.end_key_hash() >= keyHash)) {
            return &tablet;
        }
    }
    return NULL;
}
}
