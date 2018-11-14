#include "StoreManager.h"
#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>

#include <utility>

namespace webofdata {

    using namespace std;

    StoreManager::StoreManager(string baseLocation) {
        _baseLocation = std::move(baseLocation);
        _logger = spdlog::get("wod_service_log");
        LoadStores();
    }

    void StoreManager::Close() {
        // do something...
    }

    void StoreManager::LoadStores(){
        auto storeFolders = boost::filesystem::directory_iterator(_baseLocation);
        for (auto &entry : boost::make_iterator_range(storeFolders, {})) {
            if (boost::filesystem::is_directory(entry.path())) {
                auto name = entry.path().filename().string();
                auto dirName = _baseLocation + "/" + name;
                if (boost::filesystem::exists(dirName)) {
                    auto store = make_shared<Store>(name, dirName);
                    store->OpenRocksDb(dirName);
                    _stores.push_back(store);
                }
            }
        }
    }

    shared_ptr<Store> StoreManager::CreateStore(string name) {
        // TODO: add mutex
        auto dirName = _baseLocation + "/" + name;
        if (!boost::filesystem::exists(dirName)) {
            boost::filesystem::create_directories(dirName);
            auto store = make_shared<Store>(name, dirName);
            store->OpenRocksDb(dirName);

            string md("{ \"id\" : \"wod:" + name + "\"}");
            store->StoreMetadataEntity(md);

            _stores.push_back(store);
            return store;
        } else {
            return nullptr;
        }
    }

    shared_ptr<Store> StoreManager::GetStore(string name) {
        for (auto store : _stores) {
            if (store->GetName() == name) {
                return store;
            }
        }

        return nullptr;
    }

    void StoreManager::DeleteStore(string name) {
        auto store = GetStore(name);
        if (store != nullptr) {
            _stores.erase(std::remove(_stores.begin(), _stores.end(), store), _stores.end());

            // tidy up all files
            auto dirName = _baseLocation + "/" + name;
            boost::filesystem::remove_all(boost::filesystem::path(dirName));
        }
    }

    vector<shared_ptr<Store>> StoreManager::GetStores() {
        vector<shared_ptr<Store>> stores;
        for (auto const &s : _stores) {
            stores.push_back(s);
        }
        return stores;
    }
}
