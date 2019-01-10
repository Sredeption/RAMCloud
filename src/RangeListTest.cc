#include "TestUtil.h"
#include "RangeList.h"

namespace RAMCloud {

struct RangeListTest : public ::testing::Test {

    RangeListTest()
    {

    }

  private:
    DISALLOW_COPY_AND_ASSIGN(RangeListTest);
};

TEST_F(RangeListTest, initialization)
{
    std::cout << "-----------" << std::endl;
    RangeList rangeList(0, 100);
    std::cout << "-----------" << std::endl;
    rangeList.lock(0);
    rangeList.lock(1);
    rangeList.print();
    std::cout << "-----------" << std::endl;
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 6; j++) {
            rangeList.lock(static_cast<uint64_t>(i * 20 + j));
        }
    }

    rangeList.print();
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 6; j++) {
            rangeList.unlock(static_cast<uint64_t>(i * 20 + j));
        }
    }
    std::cout << "-----------" << std::endl;
    rangeList.print();
}

}

