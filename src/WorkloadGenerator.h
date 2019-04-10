#ifndef RAMCLOUD_WORKLOADGENERATOR_H
#define RAMCLOUD_WORKLOADGENERATOR_H

#include <string>
#include <stdexcept>
#include <sstream>
#include <cmath>
#include <vector>
#include <algorithm>

#include "Cycles.h"
#include "RamCloud.h"

namespace RAMCloud {


class WorkloadGenerator {
  public:
    class Client {
      public:

        virtual ~Client()
        {
        }

        virtual void setup(uint32_t objectCount, uint32_t objectSize) = 0;

        virtual void read(const char *key, uint64_t keyLen) = 0;

        virtual void
        write(const char *key, uint64_t keyLen, char *value,
              uint32_t valueLen) = 0;

        virtual void startMigration() = 0;

        virtual bool isFinished() = 0;

        virtual RamCloud *getRamCloud() = 0;

        virtual uint64_t getTableId() = 0;
    };

    struct TimeDist {
        uint64_t min;
        uint64_t avg;
        uint64_t p50;
        uint64_t p90;
        uint64_t p99;
        uint64_t p999;
        uint64_t p9999;
        uint64_t p99999;
        uint64_t bandwidth;
    };

    enum SampleType {
        READ, WRITE, ALL
    };

  private:
    class ZipfianGenerator {
      public:
        explicit ZipfianGenerator(uint64_t n, double theta = 0.99)
            : n(n), theta(theta), alpha(1 / (1 - theta)), zetan(zeta(n, theta)),
              eta((1 - std::pow(2.0 / static_cast<double>(n), 1 - theta)) /
                  (1 - zeta(2, theta) / zetan))
        {}

        uint64_t nextNumber()
        {
            double u =
                static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
            double uz = u * zetan;
            if (uz < 1)
                return 0;
            if (uz < 1 + std::pow(0.5, theta))
                return 1;
            return 0 + static_cast<uint64_t>(static_cast<double>(n) *
                                             std::pow(eta * u - eta + 1.0,
                                                      alpha));
        }

      private:
        const uint64_t n;
        const double theta;
        const double alpha;
        const double zetan;
        const double eta;

        static double zeta(uint64_t n, double theta)
        {
            double sum = 0;
            for (uint64_t i = 0; i < n; i++) {
                sum = sum + 1.0 / (std::pow(i + 1, theta));
            }
            return sum;
        }
    };

    struct Sample {
        uint64_t startTicks;
        uint64_t endTicks;
        SampleType type;

        Sample(uint64_t startTicks,
               uint64_t endTicks,
               SampleType type)
            : startTicks{startTicks}, endTicks{endTicks}, type{type}
        {}

    };

    std::string workloadName;
    uint64_t targetOps;
    uint32_t objectCount;
    uint32_t objectSize;
    int readPercent;
    ZipfianGenerator *generator;
    Client *client;
    uint64_t experimentStartTime;

    std::vector<Sample> samples;

    void getDist(std::vector<uint64_t> &times, TimeDist *dist)
    {
        int count = static_cast<int>(times.size());
        std::sort(times.begin(), times.end());
        dist->avg = 0;
        dist->min = 0;
        uint64_t last = 0;
        uint64_t sum = 0;

        for (uint64_t time: times) {
            sum += Cycles::toMicroseconds(time);
        }

        if (count > 0) {
            dist->avg = sum / count;
            dist->min = Cycles::toMicroseconds(times[0]);
            last = times.back();
        }


        dist->bandwidth = times.size();
        int index = count / 2;
        if (index < count) {
            dist->p50 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p50 = last;
        }
        index = count - (count + 5) / 10;
        if (index < count) {
            dist->p90 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p90 = last;
        }
        index = count - (count + 50) / 100;
        if (index < count) {
            dist->p99 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p99 = last;
        }
        index = count - (count + 500) / 1000;
        if (index < count) {
            dist->p999 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p999 = last;
        }
        index = count - (count + 5000) / 10000;
        if (index < count) {
            dist->p9999 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p9999 = last;
        }
        index = count - (count + 50000) / 100000;
        if (index < count) {
            dist->p99999 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p99999 = last;
        }
    }

  public:

    explicit WorkloadGenerator(std::string workloadName, uint64_t targetOps,
                               uint32_t objectCount,
                               uint32_t objectSize, Client *client);

    std::string debugString() const;

    void run(bool issueMigration);

    void asyncRun(bool issueMigration);

    void statistics(std::vector<TimeDist> &result, SampleType type);

    WorkloadGenerator(const WorkloadGenerator &) = delete;

    WorkloadGenerator &operator=(const WorkloadGenerator &) = delete;
};

}

#endif //RAMCLOUD_WORKLOADGENERATOR_H
