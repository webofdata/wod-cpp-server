#ifndef WEBOFDATA_ENTITYEVENTHANDLER_H
#define WEBOFDATA_ENTITYEVENTHANDLER_H

#include <string>

namespace webofdata {

    using namespace std;

    class EntityEventHandler {
        private:
            long _offset;

        public:
            void CatchUp() {

            }

            EntityEventHandler(long offset) {
                
            }

            int HandleEntity(shared_ptr<string> id, shared_ptr<string> data);
    };
}

#endif