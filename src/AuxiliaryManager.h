#ifndef RAMCLOUD_AUXILIARYMANAGER_H
#define RAMCLOUD_AUXILIARYMANAGER_H

#include <boost/lockfree/spsc_queue.hpp>

#include "Dispatch.h"
#include "Transport.h"

namespace RAMCloud {

class AuxiliaryManager : Dispatch::Poller {

  public:
    AuxiliaryManager(Context *context);

    ~AuxiliaryManager();

    int poll();

    void sendReply(Transport::ServerRpc *rpc);

  private:
    boost::lockfree::spsc_queue<Transport::ServerRpc *> replyQueue;

};

}

#endif //RAMCLOUD_AUXILIARYMANAGER_H
