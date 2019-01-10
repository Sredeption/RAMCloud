#ifndef RAMCLOUD_RANGETREE_H
#define RAMCLOUD_RANGETREE_H

#include "Common.h"

namespace RAMCloud {
class RangeNode {
  PUBLIC:
    uint64_t start, end;

    RangeNode(uint64_t start, uint64_t end);

    virtual ~RangeNode()
    {}

    virtual bool operator<(uint64_t hash);

    virtual bool operator>(uint64_t hash);

};

class RangeList {
  PUBLIC:

    class LockNode : public RangeNode {
      PUBLIC:
        uint64_t lockNumber;

        std::vector<LockNode *> forward, backward;
        int level;
        bool max, min;

        LockNode(uint64_t start, uint64_t end, int level, bool max = false,
                 bool min = false);

        bool operator<(uint64_t hash);

        bool operator>(uint64_t hash);

        std::string toString();
    };

  PRIVATE:

    static const int MAX_LEVEL = 16;


    LockNode *head, *unlockHead;
    LockNode *BEGIN, *END;

    int randomLevel();

    LockNode *find(uint64_t hash, vector<LockNode *> &update);

    void insert(std::vector<LockNode *> &update, LockNode *node);

    void remove(LockNode *node);

  PUBLIC:

    RangeList(uint64_t start, uint64_t end);

    ~RangeList();

    void lock(uint64_t hash);

    void unlock(uint64_t hash);

    LockNode *getRanges();

    void print();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RangeList)
};

class AvailableRangeList {
  PUBLIC:

    AvailableRangeList();

    void isLocked(uint64_t hash);

    void insert(uint64_t start, uint64_t end);
};

}

#endif //RAMCLOUD_RANGELIST_H
