/* Copyright (c) 2011-2014 Stanford University
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

#include <iostream>
#include "Cycles.h"
#include "CycleCounter.h"
#include "Common.h"
#include "MasterClient.h"
#include "Context.h"
#include "RamCloud.h"
#include "ShortMacros.h"

using namespace RAMCloud;

void makeKey(int value, uint32_t length, char *dest)
{
    memset(dest, 'x', length);
    *(reinterpret_cast<int *>(dest)) = value;
}

string status = "status";
string filling = "filling";
string ending = "ending";

class BasicClient {

  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId targetServerId;

        Migration(uint64_t tableId, uint64_t firstKey,
                  uint64_t lastKey, ServerId targetServerId)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              targetServerId(targetServerId)
        {
        }
    };

    BasicClient(RamCloud *ramcloud, int clientIndex, Migration *migration,
                uint64_t controlHubId,
                uint16_t keyLength, uint32_t valueLength, uint32_t numObjects)
        : ramcloud(ramcloud), clientIndex(clientIndex), migration(migration),
          controlHubId(controlHubId), keyLength(keyLength),
          valueLength(valueLength), numObjects(numObjects), totalLatency(0),
          totalOps(0), startTime(0), finishTime(0), lastQuery(0), lastUpload(0),
          firstTimestamp(-1), lastTimestamp(-1), migrationId(),
          generateWorkload(true), key(NULL), value(NULL)
    {
        key = new char[keyLength];
        value = new char[valueLength];
    }

    ~BasicClient()
    {
        delete[] key;
        delete[] value;
    }

    void doWorkload(bool generateWorkload = true)
    {
        uint64_t start = Cycles::rdtsc();
        this->generateWorkload = generateWorkload;
        RAMCLOUD_LOG(NOTICE, "start benchmark");
        while (true) {
            uint64_t latency = randomOp();
            totalLatency += latency;
            totalOps += 1;
            uint64_t current = Cycles::rdtsc();

            if (startTime == 0 && clientIndex == 0 &&
                Cycles::toSeconds(current - start) > 1) {
                startMigration();
            }

            if (startTime != 0 && finishTime == 0 && clientIndex == 0 &&
                Cycles::toMicroseconds(current - start) >
                static_cast<uint64_t>(lastQuery + 1) * 100000) {
                lastQuery += 1;
                if (lastQuery > 10 && isFinished()) {
                    finishTime = Cycles::rdtsc();
                    RAMCLOUD_LOG(NOTICE, "Migration finish, waiting 1 second");
                }
            }

            if (Cycles::toMicroseconds(current - start) >
                static_cast<uint64_t>(lastUpload + 1) * 100000) {
                lastUpload += 1;
                uploadMetric();
            }

            if (finishTime != 0 &&
                Cycles::toSeconds(current - finishTime) > 1) {
                RAMCLOUD_LOG(NOTICE, "break benchmark");
                break;
            }
        }

        finish();
    }

    uint64_t migrationDuration()
    {
        return finishTime - startTime;
    }

  PRIVATE:
    RamCloud *ramcloud;
    int clientIndex;
    Migration *migration;
    uint64_t controlHubId;
    uint16_t keyLength;
    uint32_t valueLength;
    uint32_t numObjects;
    uint64_t totalLatency;
    uint64_t totalOps;
    uint64_t startTime;
    uint64_t finishTime;
    int lastQuery;
    int lastUpload;
    int firstTimestamp;
    int lastTimestamp;
    uint64_t migrationId;
    bool generateWorkload;

    char *key;
    char *value;

    uint64_t randomOp()
    {
        if (!generateWorkload) {
            usleep(100000);
            return 0;
        }
        int choice = static_cast<int>(generateRandom() % 2);
        switch (choice) {
            case 0:
                return randomRead();
            case 1:
                return randomWrite();
            default:
                return 0;
        }
    }

    uint64_t randomWrite()
    {
        memset(value, 'x', valueLength);
        uint64_t start = Cycles::rdtsc();
        makeKey(downCast<int>(generateRandom() % numObjects), keyLength, key);
        ramcloud->write(migration->tableId, key, keyLength, value, valueLength,
                        NULL, NULL, false);
        uint64_t interval = Cycles::rdtsc() - start;
        return interval;
    }

    uint64_t randomRead()
    {
        Buffer value;
        uint64_t start = Cycles::rdtsc();
        makeKey(downCast<int>(generateRandom() % numObjects), keyLength, key);
        bool exists;
        ramcloud->readMigrating(migration->tableId, key, keyLength, &value,
                                NULL, NULL, &exists);
        uint64_t interval = Cycles::rdtsc() - start;

        return interval;
    }

    bool isFinished()
    {

        if (clientIndex == 0) {
            bool isFinished = ramcloud->migrationQuery(migrationId);
            if (isFinished) {
                ramcloud->write(controlHubId, status.c_str(),
                                static_cast<uint16_t>(status.length()),
                                ending.c_str(),
                                static_cast<uint32_t>(ending.length()));
            }
            return isFinished;
        } else {
            Buffer statusValue;
            ramcloud->read(controlHubId, status.c_str(),
                           static_cast<uint16_t>(status.length()),
                           &statusValue);
            string currentStatus = string(
                reinterpret_cast<const char *>(
                    statusValue.getRange(0, statusValue.size())),
                statusValue.size());
            return currentStatus == ending;
        }
    }

    void uploadMetric()
    {
        string timestampKey = "timestamp";
        int timestamp;

        if (!generateWorkload)
            return;

        if (clientIndex == 0) {
            timestamp = lastUpload;
            ramcloud->write(controlHubId, timestampKey.c_str(),
                            static_cast<uint16_t>(timestampKey.length()),
                            &timestamp, sizeof(timestamp));
        } else {
            Buffer value;
            bool exists = false;
            while (!exists)
                ramcloud->read(controlHubId, timestampKey.c_str(),
                               static_cast<uint16_t>(timestampKey.length()),
                               &value, NULL, NULL, &exists);
            value.copy(0, sizeof32(timestamp), &timestamp);
        }

        if (firstTimestamp == -1)
            firstTimestamp = timestamp;
        lastTimestamp = timestamp;

        string latencyKey = format("L:%d:%d", clientIndex, timestamp);
        string throughputKey = format("T:%d:%d", clientIndex, timestamp);


        float latency =
            static_cast<float>(Cycles::toMicroseconds(totalLatency)) /
            static_cast<float>(totalOps);
        RAMCLOUD_LOG(NOTICE, "%d:<latency:%f, throughput:%f/ms>",
                     timestamp, latency, static_cast<float>(totalOps) / 100.f);

        ramcloud->write(controlHubId, latencyKey.c_str(),
                        static_cast<uint16_t>(latencyKey.length()),
                        &totalLatency, sizeof(totalLatency));

        ramcloud->write(controlHubId, throughputKey.c_str(),
                        static_cast<uint16_t>(throughputKey.length()),
                        &totalOps, sizeof(totalOps));

        totalLatency = 0;
        totalOps = 0;
    }

    void finish()
    {
        RAMCLOUD_LOG(NOTICE, "Benchmark finish, writing metadata");
        string firstTimestampKey = format("FirstTimestamp:%d", clientIndex);
        string lastTimestampKey = format("LastTimestamp:%d", clientIndex);

        ramcloud->write(controlHubId, firstTimestampKey.c_str(),
                        static_cast<uint16_t>(firstTimestampKey.length()),
                        &firstTimestamp, sizeof(firstTimestamp));

        ramcloud->write(controlHubId, lastTimestampKey.c_str(),
                        static_cast<uint16_t>(lastTimestampKey.length()),
                        &lastTimestamp, sizeof(lastTimestamp));
    }

    void startMigration()
    {
        startTime = Cycles::rdtsc();
        RAMCLOUD_LOG(NOTICE, "Issuing migration request:");
        RAMCLOUD_LOG(NOTICE, "  table (%lu)", migration->tableId);
        RAMCLOUD_LOG(NOTICE, "  first key %lu", migration->firstKey);
        RAMCLOUD_LOG(NOTICE, "  last key  %lx", migration->lastKey);
        RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                     migration->targetServerId.indexNumber());

        migrationId = ramcloud->backupMigrate(migration->tableId,
                                              migration->firstKey,
                                              migration->lastKey,
                                              migration->targetServerId);
    }


    DISALLOW_COPY_AND_ASSIGN(BasicClient)

};


int
main(int argc, char *argv[])
try
{
    Context context(true);
    int clientIndex;
    int numClients;
    string tableName;
    string testHub;
    uint64_t firstKey;
    uint64_t lastKey;
    uint32_t newOwnerMasterId;

    uint32_t objectCount = 0;
    uint32_t objectSize = 0;
    uint32_t otherObjectCount = 0;

    OptionsDescription migrateOptions("Migrate");
    migrateOptions.add_options()
        ("clientIndex",
         ProgramOptions::value<int>(&clientIndex)->
             default_value(0),
         "Index of this client (first client is 0)")
        ("numClients",
         ProgramOptions::value<int>(&numClients)->
             default_value(1),
         "Total number of clients running")
        ("table,t",
         ProgramOptions::value<string>(&tableName)->
             default_value("benchmark"),
         "name of the table to migrate.")
        ("firstKey,f",
         ProgramOptions::value<uint64_t>(&firstKey)->
             default_value(0),
         "First key of the tablet range to migrate")
        ("lastKey,z",
         ProgramOptions::value<uint64_t>(&lastKey)->
             default_value((~0ul) / 2),
         "Last key of the tablet range to migrate")
        ("recipient,r",
         ProgramOptions::value<uint32_t>(&newOwnerMasterId)->
             default_value(2),
         "ServerId of the master to migrate to")
        ("objectCount",
         ProgramOptions::value<uint32_t>(&objectCount)->
             default_value(3000000),
         "Number of objects to pre-populate in the table to be migrated")
        ("objectSize",
         ProgramOptions::value<uint32_t>(&objectSize)->
             default_value(1000),
         "Size of objects to pre-populate in tables")
        ("otherObjectCount",
         ProgramOptions::value<uint32_t>(&otherObjectCount)->
             default_value(0),
         "Number of objects to pre-populate in the table to be "
         "NOT TO BE migrated");

    OptionParser optionParser(migrateOptions, argc, argv);
    if (tableName == "") {
        RAMCLOUD_DIE("error: please specify the table name");
        exit(1);
    }
    if (newOwnerMasterId == 0) {
        RAMCLOUD_DIE("error: please specify the recipient's ServerId");
        exit(1);
    }

    string coordinatorLocator = optionParser.options.getCoordinatorLocator();
    testHub = "testHub";
    RAMCLOUD_LOG(NOTICE, "client: Connecting to coordinator %s",
                 coordinatorLocator.c_str());

    RamCloud client(&context, coordinatorLocator.c_str());

    uint64_t tableId = 0;
    uint64_t controlHubId = 0;
    const uint64_t totalBytes = objectCount * objectSize;
    if (clientIndex == 0) {
        ServerId server1 = ServerId(1u, 0u);
        ServerId server3 = ServerId(3u, 0u);
        tableId = client.createTableToServer(tableName.c_str(), server1);
        controlHubId = client.createTableToServer(testHub.c_str(), server3);
        client.testingFill(tableId, "", 0, objectCount, objectSize);

        client.splitTablet(tableName.c_str(), lastKey + 1);

        client.write(controlHubId, status.c_str(),
                     static_cast<uint16_t>(status.length()),
                     filling.c_str(), static_cast<uint32_t>(filling.length()));
        RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);

    } else {

        while (true) {
            try {
                controlHubId = client.getTableId(testHub.c_str());
                tableId = client.getTableId(tableName.c_str());
                break;
            } catch (TableDoesntExistException &e) {
            }
        }

        Buffer statusValue;
        bool exists = false;
        while (true) {
            client.read(controlHubId, status.c_str(),
                        static_cast<uint16_t>(status.length()),
                        &statusValue, NULL, NULL, &exists);

            if (exists) {
                statusValue.size();
                string currentStatus = string(
                    reinterpret_cast<const char *>(
                        statusValue.getRange(0, statusValue.size())),
                    statusValue.size());

                RAMCLOUD_CLOG(WARNING, "status:%s", currentStatus.c_str());
                if (currentStatus == filling)
                    break;
            }

            RAMCLOUD_CLOG(WARNING, "wait for filling");
        }

    }


    const uint16_t keyLength = 30;
    const uint32_t valueLength = objectSize;
    BasicClient::Migration migration(tableId, firstKey, lastKey,
                                     ServerId(newOwnerMasterId, 0));
    BasicClient basicClient(&client, clientIndex, &migration, controlHubId,
                            keyLength, valueLength, objectCount);
    basicClient.doWorkload(false);

    if (clientIndex == 0) {
        double seconds = Cycles::toSeconds(basicClient.migrationDuration());
        RAMCLOUD_LOG(NOTICE, "Migration took %0.2f MB/s",
                     double(totalBytes) / seconds / double(1 << 20));

    }

    return 0;
} catch (ClientException &e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception &e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
