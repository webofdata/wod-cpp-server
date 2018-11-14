

#ifndef WEBOFDATA_ISTOREUPDATE_H
#define WEBOFDATA_ISTOREUPDATE_H

#include <string>
#include "ChangeHandler.h"

namespace webofdata {

    using namespace std;
    using ulong = unsigned long;

    class ChangeHandler;

    class IStoreUpdate {
        public:
            virtual ulong WriteChangesToHandler(string dataset, ulong from, int count, int shard, shared_ptr<ChangeHandler> handler) {
                // no op
            }

            virtual string GetMetadataEntity(){
                // no op
            }
    };
}

#endif