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
#include "ServerList.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "Common.h"
#include "MasterClient.h"
#include "Context.h"
#include "RamCloud.h"
#include "ShortMacros.h"
#include "WorkloadGenerator.h"

using namespace RAMCloud;


Tub<RamCloud> client;

uint64_t controlHubId = 0;
string testControlHub = "testControlHub";

int clientIndex;
int numClients;

string tableName;
uint64_t firstKey;
uint64_t lastKey;
uint32_t newOwnerMasterId;

uint32_t objectCount = 0;
uint32_t objectSize = 0;
uint32_t otherObjectCount = 0;

void makeKey(int value, uint32_t length, char *dest)
{
    memset(dest, 'x', length);
    *(reinterpret_cast<int *>(dest)) = value;
}

string status = "status";
string filling = "filling";
string ending = "ending";

class BasicClient : public RAMCloud::WorkloadGenerator::Client {

  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId targetServerId;
        bool skipMaster;

        Migration(uint64_t tableId, uint64_t firstKey,
                  uint64_t lastKey, ServerId targetServerId, bool skipMaster)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              targetServerId(targetServerId), skipMaster(skipMaster)
        {
        }
    };

    BasicClient(RamCloud *ramcloud, int clientIndex, Migration *migration,
                uint16_t keyLength, uint32_t valueLength, uint32_t numObjects)
        : ramcloud(ramcloud), clientIndex(clientIndex), migration(migration),
          controlHubId(), keyLength(keyLength),
          valueLength(valueLength), numObjects(numObjects),
          experimentStartTime(0), migrationId(), migrationStartTime(0),
          migrationFinishTime(0), pressTableId(), generateWorkload(true)
    {
    }

    ~BasicClient()
    {
    }

    void setup(uint32_t objectCount, uint32_t objectSize)
    {
        if (clientIndex == 0) {
            ServerId server1 = ServerId(1u, 0u);
            ServerId server3 = ServerId(3u, 0u);
            pressTableId = client->createTableToServer(tableName.c_str(),
                                                       server1);
            migration->tableId = pressTableId;
            controlHubId = client->createTableToServer(testControlHub.c_str(),
                                                       server3);
            client->testingFill(pressTableId, "", 0, objectCount, objectSize);

            client->splitTablet(tableName.c_str(), lastKey + 1);

            client->write(controlHubId, status.c_str(),
                          static_cast<uint16_t>(status.length()),
                          filling.c_str(),
                          static_cast<uint32_t>(filling.length()));
            RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);

        } else {

            while (true) {
                try {
                    controlHubId = client->getTableId(testControlHub.c_str());
                    pressTableId = client->getTableId(tableName.c_str());
                    break;
                } catch (TableDoesntExistException &e) {
                }
            }

            Buffer statusValue;
            bool exists = false;
            while (true) {
                client->read(controlHubId, status.c_str(),
                             static_cast<uint16_t>(status.length()),
                             &statusValue, NULL, NULL, &exists);

                if (exists) {
                    statusValue.size();
                    string currentStatus = string(
                        reinterpret_cast<const char *>(
                            statusValue.getRange(0, statusValue.size())),
                        statusValue.size());

                    RAMCLOUD_LOG(WARNING, "status:%s", currentStatus.c_str());
                    if (currentStatus == filling)
                        break;
                }

                RAMCLOUD_CLOG(WARNING, "wait for filling");
            }

        }
    }

    void read(const char *key, uint64_t keyLen)
    {
        Buffer value;
        bool exists;
        ramcloud->readMigrating(pressTableId, key,
                                static_cast<uint16_t>(keyLen), &value,
                                NULL, NULL, &exists);
        if (!exists) {
            RAMCLOUD_CLOG(WARNING, "......");
        }
    }

    void write(const char *key, uint64_t keyLen, char *value, uint32_t valueLen)
    {
        ramcloud->write(pressTableId, key, static_cast<uint16_t>(keyLen), value,
                        valueLen);
    }

    void startMigration()
    {
        RAMCLOUD_LOG(NOTICE, "Issuing migration request:");
        RAMCLOUD_LOG(NOTICE, "  table (%lu)", migration->tableId);
        RAMCLOUD_LOG(NOTICE, "  first key %lu", migration->firstKey);
        RAMCLOUD_LOG(NOTICE, "  last key  %lx", migration->lastKey);
        RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                     migration->targetServerId.indexNumber());

        migrationId = ramcloud->backupMigrate(migration->tableId,
                                              migration->firstKey,
                                              migration->lastKey,
                                              migration->targetServerId,
                                              migration->skipMaster);
        migrationStartTime = Cycles::rdtsc();
    }


    bool isFinished()
    {
        if (clientIndex == 0) {
            if (migrationStartTime == 0)
                return false;
            if (migrationFinishTime == 0) {
                if (Cycles::toMicroseconds(
                    Cycles::rdtsc() - migrationStartTime) < 100000)
                    return false;
                bool isFinished = ramcloud->migrationQuery(migrationId);
                if (isFinished) {
                    migrationFinishTime = Cycles::rdtsc();
                }
                return false;
            } else {
                if (Cycles::toSeconds(Cycles::rdtsc() - migrationFinishTime)
                    > 7) {
                    ramcloud->write(controlHubId, status.c_str(),
                                    static_cast<uint16_t>(status.length()),
                                    ending.c_str(),
                                    static_cast<uint32_t>(ending.length()));
                    RAMCLOUD_LOG(NOTICE, "migration finish");
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            Buffer statusValue;
            ramcloud->read(controlHubId, status.c_str(),
                           static_cast<uint16_t>(status.length()),
                           &statusValue);
            string currentStatus = string(
                reinterpret_cast<const char *>(
                    statusValue.getRange(0, statusValue.size())),
                statusValue.size());
            RAMCLOUD_CLOG(WARNING, "finish status:%s", currentStatus.c_str());
            return currentStatus == ending;
        }
    }


    uint64_t migrationDuration()
    {
        return migrationFinishTime - migrationStartTime;
    }

  PRIVATE:
    RamCloud *ramcloud;
    int clientIndex;
    Migration *migration;
    uint64_t controlHubId;
    uint16_t keyLength;
    uint32_t valueLength;
    uint32_t numObjects;
    uint64_t experimentStartTime;
    uint64_t migrationId;
    uint64_t migrationStartTime;
    uint64_t migrationFinishTime;
    uint64_t pressTableId;
    bool generateWorkload;

    DISALLOW_COPY_AND_ASSIGN(BasicClient)

};

void basic()
{
    uint64_t tableId = 0;

    const uint16_t keyLength = 30;
    const uint32_t valueLength = 100;
    BasicClient::Migration migration(tableId, firstKey, lastKey,
                                     ServerId(newOwnerMasterId, 0), false);
    BasicClient basicClient(client.get(), clientIndex, &migration,
                            keyLength, valueLength, objectCount);
    RAMCloud::WorkloadGenerator workloadGenerator(
        "YCSB-B", 100000, objectCount, objectSize, &basicClient);

    bool issueMigration = false;
    if (clientIndex == 0)
        issueMigration = true;
    workloadGenerator.run(issueMigration);

    std::vector<RAMCloud::WorkloadGenerator::TimeDist> result;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> readResult;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> writeResult;
    workloadGenerator.statistics(result, RAMCloud::WorkloadGenerator::ALL);
    workloadGenerator.statistics(readResult, RAMCloud::WorkloadGenerator::READ);
    workloadGenerator.statistics(writeResult,
                                 RAMCloud::WorkloadGenerator::WRITE);
    RAMCLOUD_LOG(WARNING,
                 "time: all median, 99th | read median, 99th | write median, 99th");
    for (uint64_t i = 0; i < result.size(); i++) {
        RAMCLOUD_LOG(NOTICE,
                     "%lu:%lu, %lu, %lf | %lu, %lu, %lf | %lu, %lu, %lf", i,
                     result[i].p50, result[i].p999,
                     static_cast<double>(result[i].bandwidth) / 100.,
                     readResult[i].p50, readResult[i].p999,
                     static_cast<double>(readResult[i].bandwidth) / 100.,
                     writeResult[i].p50, writeResult[i].p999,
                     static_cast<double>(writeResult[i].bandwidth) / 100.);
    }
}

class RocksteadyClient : public RAMCloud::WorkloadGenerator::Client {
  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId sourceServerId;
        ServerId targetServerId;
        bool skipMaster;

        Migration(uint64_t tableId, uint64_t firstKey, uint64_t lastKey,
                  ServerId sourceServerId, ServerId targetServerId,
                  bool skipMaster)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              sourceServerId(sourceServerId), targetServerId(targetServerId),
              skipMaster(skipMaster)
        {
        }
    };

    RocksteadyClient(RamCloud *ramcloud, int clientIndex, Migration *migration,
                     uint16_t keyLength, uint32_t valueLength,
                     uint32_t numObjects, uint64_t time)
        : ramcloud(ramcloud), clientIndex(clientIndex), migration(migration),
          controlHubId(), keyLength(keyLength), valueLength(valueLength),
          numObjects(numObjects), experimentStartTime(0), migrationId(),
          migrationStartTime(0), migrationFinishTime(0), pressTableId(),
          generateWorkload(true), time(time), rocksteadyMigration()
    {
    }

    ~RocksteadyClient()
    {

    }

    void setup(uint32_t objectCount, uint32_t objectSize)
    {
        if (clientIndex == 0) {
            ServerId server1 = ServerId(1u, 0u);
            ServerId server3 = ServerId(3u, 0u);
            pressTableId = client->createTableToServer(tableName.c_str(),
                                                       server1);
            migration->tableId = pressTableId;
            controlHubId = client->createTableToServer(testControlHub.c_str(),
                                                       server3);
            client->testingFill(pressTableId, "", 0, objectCount, objectSize);

            client->splitTablet(tableName.c_str(), lastKey + 1);

            client->write(controlHubId, status.c_str(),
                          static_cast<uint16_t>(status.length()),
                          filling.c_str(),
                          static_cast<uint32_t>(filling.length()));
            RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);

        } else {

            while (true) {
                try {
                    controlHubId = client->getTableId(testControlHub.c_str());
                    pressTableId = client->getTableId(tableName.c_str());
                    break;
                } catch (TableDoesntExistException &e) {
                }
            }

            Buffer statusValue;
            bool exists = false;
            while (true) {
                client->read(controlHubId, status.c_str(),
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
    }

    void read(const char *key, uint64_t keyLen)
    {
        Buffer value;
        bool exists;
        ramcloud->read(pressTableId, key, static_cast<uint16_t>(keyLen), &value,
                       NULL, NULL, &exists);
    }

    void write(const char *key, uint64_t keyLen, char *value, uint32_t valueLen)
    {
        ramcloud->write(pressTableId, key, static_cast<uint16_t>(keyLen), value,
                        valueLen, NULL, NULL, true);
    }

    void startMigration()
    {
        RAMCLOUD_LOG(WARNING, "Issuing migration request:");
        RAMCLOUD_LOG(NOTICE, "  table (%lu)", migration->tableId);
        RAMCLOUD_LOG(NOTICE, "  first key %lu", migration->firstKey);
        RAMCLOUD_LOG(NOTICE, "  last key  %lx", migration->lastKey);
        RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                     migration->targetServerId.indexNumber());

        rocksteadyMigration.construct(ramcloud, migration->tableId,
                                      migration->firstKey, migration->lastKey,
                                      migration->sourceServerId,
                                      migration->targetServerId);
        rocksteadyMigration->wait();
        rocksteadyMigration.destroy();
        migrationStartTime = Cycles::rdtsc();
    }


    bool isFinished()
    {
        if (clientIndex == 0) {
            if (migrationStartTime == 0 || Cycles::toSeconds(
                Cycles::rdtsc() - migrationStartTime) < time)
                return false;
            else {
                RAMCLOUD_LOG(WARNING, "finish");
                ramcloud->write(controlHubId, status.c_str(),
                                static_cast<uint16_t>(status.length()),
                                ending.c_str(),
                                static_cast<uint32_t>(ending.length()));
                return true;
            }
        } else {
            Buffer statusValue;
            ramcloud->read(controlHubId, status.c_str(),
                           static_cast<uint16_t>(status.length()),
                           &statusValue);
            string currentStatus = string(
                reinterpret_cast<const char *>(
                    statusValue.getRange(0, statusValue.size())),
                statusValue.size());
            RAMCLOUD_CLOG(WARNING, "finish status:%s", currentStatus.c_str());
            return currentStatus == ending;
        }
    }

    uint64_t migrationDuration()
    {
        return migrationFinishTime - migrationStartTime;
    }

  PRIVATE:
    RamCloud *ramcloud;
    int clientIndex;
    Migration *migration;
    uint64_t controlHubId;
    uint16_t keyLength;
    uint32_t valueLength;
    uint32_t numObjects;
    uint64_t experimentStartTime;
    uint64_t migrationId;
    uint64_t migrationStartTime;
    uint64_t migrationFinishTime;
    uint64_t pressTableId;
    bool generateWorkload;
    uint64_t time;
    Tub<RocksteadyMigrateTabletRpc> rocksteadyMigration;

    DISALLOW_COPY_AND_ASSIGN(RocksteadyClient)
};

void rocksteadyBasic()
{
    uint64_t tableId = 0;

    const uint16_t keyLength = 30;
    const uint32_t valueLength = 100;
    RocksteadyClient::Migration
        migration(tableId, firstKey, lastKey, ServerId(1, 0),
                  ServerId(newOwnerMasterId, 0), false);
    RocksteadyClient basicClient(client.get(), clientIndex, &migration,
                                 keyLength, valueLength, objectCount, 7);
    RAMCloud::WorkloadGenerator workloadGenerator(
        "YCSB-B", 100000, objectCount, objectSize, &basicClient);

    bool issueMigration = false;
    if (clientIndex == 0)
        issueMigration = true;
    workloadGenerator.run(issueMigration);

    std::vector<RAMCloud::WorkloadGenerator::TimeDist> result;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> readResult;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> writeResult;
    workloadGenerator.statistics(result, RAMCloud::WorkloadGenerator::ALL);
    workloadGenerator.statistics(readResult, RAMCloud::WorkloadGenerator::READ);
    workloadGenerator.statistics(writeResult,
                                 RAMCloud::WorkloadGenerator::WRITE);
    RAMCLOUD_LOG(WARNING,
                 "time: all median, 99th | read median, 99th | write median, 99th");
    for (uint64_t i = 0; i < result.size(); i++) {
        RAMCLOUD_LOG(NOTICE,
                     "%lu:%lu, %lu, %lf | %lu, %lu, %lf | %lu, %lu, %lf", i,
                     result[i].p50, result[i].p999,
                     static_cast<double>(result[i].bandwidth) / 100.,
                     readResult[i].p50, readResult[i].p999,
                     static_cast<double>(readResult[i].bandwidth) / 100.,
                     writeResult[i].p50, writeResult[i].p999,
                     static_cast<double>(writeResult[i].bandwidth) / 100.);
    }
}
//void backupEvaluation()
//{
//    string migrationTableName = "mt";
//    uint64_t migrationTableId = 0;
//    string pressTableName = "pt";
//    uint64_t pressTableId = 0;
//    if (clientIndex == 0) {
//        ServerId server1 = ServerId(1u, 0u);
//        ServerId server2 = ServerId(2u, 0u);
//        ServerId server3 = ServerId(3u, 0u);
//        migrationTableId = client->createTableToServer(
//            migrationTableName.c_str(), server1);
//
//        controlHubId = client->createTableToServer(testControlHub.c_str(),
//                                                   server3);
//        client->testingFill(migrationTableId, "", 0, objectCount, objectSize);
//
//        client->splitTablet(migrationTableName.c_str(), lastKey + 1);
//
//        pressTableId = client->createTableToServer(
//            pressTableName.c_str(), server2);
//
//        client->write(controlHubId, status.c_str(),
//                      static_cast<uint16_t>(status.length()),
//                      filling.c_str(), static_cast<uint32_t>(filling.length()));
//        RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);
//
//    } else {
//
//    }
//
//    const uint16_t keyLength = 30;
//    const uint32_t valueLength = objectSize;
//    BasicClient::Migration migration(migrationTableId, firstKey, lastKey,
//                                     ServerId(newOwnerMasterId, 0), true);
//}

struct BenchmarkInfo {
    const char *name;

    void (*func)();
};

BenchmarkInfo tests[] = {
    {"basic", basic}
};

int
main(int argc, char *argv[])
try
{
    Context context(true);


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
    RAMCLOUD_LOG(NOTICE, "client: Connecting to coordinator %s",
                 coordinatorLocator.c_str());
    client.construct(&context, coordinatorLocator.c_str());

    ProtoBuf::ServerList protoServerList;
    CoordinatorClient::getServerList(&context, &protoServerList);

    ServerList serverList(&context);
    serverList.applyServerList(protoServerList);

//    rocksteadyBasic();
    basic();

    return 0;
} catch (ClientException &e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception &e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
