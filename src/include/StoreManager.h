
#ifndef WEBOFDATA_STOREMANAGER_H
#define WEBOFDATA_STOREMANAGER_H

#include "Store.h"
#include <boost/filesystem.hpp>
#include "spdlog/spdlog.h"

namespace webofdata {

    using namespace std;

    class StoreManager {
        string _baseLocation;
        vector<shared_ptr<Store>> _stores;
        shared_ptr<spdlog::logger> _logger;
    public:
        StoreManager(string baseLocation);
        void LoadStores();
        shared_ptr<Store> CreateStore(string name);
        shared_ptr<Store> GetStore(string name);
        void DeleteStore(string name);
        void Close();
        vector<shared_ptr<Store>> GetStores();
    };
}

#endif //WEBOFDATA_STOREMANAGER_H
