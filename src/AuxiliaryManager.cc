#include "AuxiliaryManager.h"

namespace RAMCloud {

AuxiliaryManager::AuxiliaryManager(Context *context)
    : Dispatch::Poller(context->auxDispatch, "AuxiliaryManager"),
      replyQueue(100)
{

}

AuxiliaryManager::~AuxiliaryManager()
{

}

int AuxiliaryManager::poll()
{
    int workDone = 0;
    while (!replyQueue.empty()) {
        replyQueue.front()->sendReply();
        replyQueue.pop();
        workDone = 1;
    }
    return workDone;
}

void AuxiliaryManager::sendReply(Transport::ServerRpc *rpc)
{
    replyQueue.push(rpc);
}

}
