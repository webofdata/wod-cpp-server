#ifndef WEBOFDATA_CHANGEHANDLER_H
#define WEBOFDATA_CHANGEHANDLER_H

#include <string>
#include "IStoreUpdate.h"
#include <memory>

namespace webofdata {

    using namespace std;

    class IStoreUpdate;

    class ChangeHandler {
        public:
            virtual bool ProcessEntity(shared_ptr<string> entity) = 0;
    };
}

#endif