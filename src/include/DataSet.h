#ifndef WEBOFDATA_DATASET_H
#define WEBOFDATA_DATASET_H

#include <mutex>
#include <rocksdb/db.h>
#include "Store.h"

namespace webofdata {

    using namespace std;
    using namespace rocksdb;

    using byte = unsigned char;
    using ulong = unsigned long;

    class Store;
    class Pipe;

    class DataSet {

    private:
        string _name;
        shared_ptr<Store> _store;
        int _id;
        ulong _nextSeqId;
        std::mutex log_seq_mutex;
        vector<shared_ptr<Pipe>> _pipes;

        ColumnFamilyHandle *_resourceSizeColumnFamily;
        ColumnFamilyHandle *_resourceStoreColumnFamily;
        ColumnFamilyHandle *_resourceLogColumnFamily;

        ColumnFamilyHandle *_resourceOutRefsColumnFamily;
        ColumnFamilyHandle *_resourceInRefsColumnFamily;

        void AssertColumnFamilies();

        void LookupNextSeqId();

    public:

        DataSet(shared_ptr<Store> store, string name, int id) {
            _name = name;
            _id = id;
            _store = store;
            AssertColumnFamilies();
            LookupNextSeqId();
        }

        void RegisterPipe(shared_ptr<Pipe> pipe){
            _pipes.push_back(pipe);
        }

        vector<ColumnFamilyHandle*> GetColumnFamilies() {
            vector<ColumnFamilyHandle*> cfs;
            cfs.push_back(_resourceSizeColumnFamily);
            cfs.push_back(_resourceStoreColumnFamily);
            cfs.push_back(_resourceLogColumnFamily);
            cfs.push_back(_resourceOutRefsColumnFamily);
            cfs.push_back(_resourceInRefsColumnFamily);
            return cfs;
        }

        ColumnFamilyHandle *GetSizeColumnFamily() {
            return _resourceSizeColumnFamily;
        }

        ColumnFamilyHandle *GetStoreColumnFamily() {
            return _resourceStoreColumnFamily;
        }

        ColumnFamilyHandle *GetLogColumnFamily() {
            return _resourceLogColumnFamily;
        }

        ColumnFamilyHandle *GetOutRefsColumnFamily() {
            return _resourceOutRefsColumnFamily;
        }

        ColumnFamilyHandle *GetInRefsColumnFamily() {
            return _resourceInRefsColumnFamily;
        }

        ulong GetNextSequenceId() {
            std::lock_guard<std::mutex> lock(log_seq_mutex);
            _nextSeqId++;
            return _nextSeqId;
        }

        ulong GetCurrentSequenceId() {
            return _nextSeqId;
        }

        int GetId() {
            return _id;
        }

        string GetName() {
            return _name;
        }
    };
}

#endif //WEBOFDATA_DATASET_H
