
#ifndef WEBOFDATA_PIPELOGIC_H
#define WEBOFDATA_PIPELOGIC_H

#include <string>
#include "IStoreUpdate.h"

namespace webofdata {
    class PipeLogic {
        public:
            virtual bool ProcessEntity(std::shared_ptr<std::string> entityJson, std::shared_ptr<IStoreUpdate> context) = 0;
    };
}

#endif
