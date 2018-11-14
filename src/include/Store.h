#ifndef WEBOFDATA_STORE_H
#define WEBOFDATA_STORE_H

#define RAPIDJSON_PARSE_ERROR_NORETURN(parseErrorCode,offset) throw ParseException(parseErrorCode, #parseErrorCode, offset)
#include <stdexcept>               // std::runtime_error
#include "rapidjson/error/error.h" // rapidjson::ParseResult
#include "rapidjson/error/en.h"

struct ParseException : std::runtime_error, rapidjson::ParseResult {
     ParseException(rapidjson::ParseErrorCode code, const char* msg, size_t offset)
         : std::runtime_error(msg), ParseResult(code, offset) {}
     };

#include "rapidjson/reader.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <string>
#include <iostream>
#include <sstream>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/istreamwrapper.h>
#include <rocksdb/db.h>
#include "DataSet.h"
#include "PipeLogic.h"
#include "ChangeHandler.h"
#include "IStoreUpdate.h"
#include <mutex>
#include <EntityStreamWriter.h>
#include "spdlog/spdlog.h"

namespace webofdata {

    using namespace std;
    using namespace rocksdb;
    using namespace rapidjson;

    using byte = unsigned char;
    using ulong = unsigned long;

    template<typename T>
    class ArrayDeleter {
    public:
        void operator()(T *d) const {
            delete[] d;
        }
    };

    // interface for getting and storing pipe state
    class PipeStateManager {
        public:
            virtual ulong getOffset(string pipeId) = 0;
            virtual void storeOffset(string pipeId) = 0;        
    };

    class IdentityTransformPipeLogic : public PipeLogic {
        private:
            shared_ptr<Store> _store;
            string _targetDatasetName;
        public:
            IdentityTransformPipeLogic(shared_ptr<Store> store, string targetDataset) 
            : _store(store), _targetDatasetName(targetDataset) {
            }
            bool ProcessEntity(shared_ptr<string> entityJson, shared_ptr<IStoreUpdate> context) override;
    };

    class Pipe : public ChangeHandler, public std::enable_shared_from_this<Pipe> {
        private:
            shared_ptr<DataSet> _dataset;
            shared_ptr<Store> _store;
            ulong _lastSeen;
            shared_ptr<PipeLogic> _pipeLogic;
            int _parallelisation;
            bool _running;
        public:
            Pipe(shared_ptr<Store> store, shared_ptr<DataSet> dataset, ulong lastSeen, shared_ptr<PipeLogic> pipeLogic) 
            : _lastSeen(lastSeen), _dataset(dataset), _store(store), _pipeLogic(pipeLogic)
            {
            }

            bool ProcessEntity(shared_ptr<string> entityJson) override {
                auto _ref = dynamic_pointer_cast<IStoreUpdate>(_store);
                return _pipeLogic->ProcessEntity(entityJson, _ref);
            }

            void Run();

            void RunOnce();

            void Pause() {
                _running = false;
            }
    };

    class StoreException : public IStoreUpdate, public exception {
    private:
        string _msg;

    public:
        StoreException(string msg) : _msg(msg) {};
    };

    class Store : public std::enable_shared_from_this<Store>, public IStoreUpdate {

    private:

        shared_ptr<spdlog::logger> _logger;
        shared_ptr<string> _namespacesJson;
        shared_ptr<Document> _namespacesJsonDocument;
        string _storeLocation;
        string _name;

        std::vector<ColumnFamilyDescriptor> _columnFamilies;
        std::vector<ColumnFamilyHandle *> _handles;

        std::map<string, ColumnFamilyHandle *> _handlesByName;
        std::map<string, shared_ptr<DataSet>> _datasets;

        unordered_map<string, int> _namespaceToIdIndex;
        unordered_map<int, string> _idToNamespaceIndex;

        unordered_map<string, int> _propertyToIdIndex;
        unordered_map<int, string> _idToPropertyIndex;

        rocksdb::DB *_database;

        int _nextNamespaceId;
        int _nextDataSetId;
        int _nextPropertyId;

        std::mutex assert_column_family_mutex;
        std::mutex assert_namespace_mutex;
        std::mutex assert_dataset_mutex;
        std::mutex assert_property_mutex;

        ColumnFamilyHandle* _globalStateColumnFamily;
        ColumnFamilyHandle* _namespacesColumnFamily;
        ColumnFamilyHandle* _pipeState;

    public:

        Store(string name, string location);
        ~Store();

        // virtual ulong getOffset(string pipeId);
        // virtual void storeOffset(string pipeId);        

        string GetName() { return _name; }
        void OpenRocksDb(string location);
        ColumnFamilyHandle *AssertColumnFamily(string name);
        rocksdb::DB *GetDatabase();
        void StoreMetadataEntity(std::string data);
        void StoreDatasetMetadataEntity(std::string dataset, std::string data);
        string GetMetadataEntity() override;
        string GetDatasetMetadataEntity(string dataset);
        void StoreEntity(string dataset, shared_ptr<std::string> data);
        long StoreEntities(std::istream &data, string dataset);
        void DeleteDataSet(string dataset);
        void ClearDataSet(string dataset);
        shared_ptr<vector<string>> GetChanges(string dataset, ulong sequence, int count);

        void WriteChangesToStream(string dataset, long sequence, int count, int shard, EntityStreamWriter &stream);

        ulong WriteChangesToHandler(string dataset, ulong from, int count, int shard, shared_ptr<ChangeHandler> handler) override;

        shared_ptr<string> GetEntities(string dataset, string lastId, int count, int shard, shared_ptr<vector<shared_ptr<string>>> result);

        void WriteEntitiesToStream(string dataset, string lastId, int count, int shard, EntityStreamWriter &stream);

        shared_ptr<vector<string>> GetDataSetShardTokens(string dataset, int shardCount);

        shared_ptr<vector<string>> GetChangesShardTokens(string dataset, int shardCount);

        static bool StrPtrComp (shared_ptr<string> lhs, shared_ptr<string> rhs) {
            return lhs->compare(*rhs);
        }

        // Methods for getting and writing entities and merged entities
        void WriteEntityToStream(string si, const vector<string> &datasets, EntityStreamWriter &stream);

        void WriteCompleteEntityToStream(string si, const vector<string> &datasets, EntityStreamWriter &stream);

        shared_ptr<string> GetEntity(string si, const vector<string> &datasets);

        void WriteRelatedEntitiesToStream(string si, string property, bool inverse, long skip, int count, const vector<string> &datasets, EntityStreamWriter &stream);

        shared_ptr<vector<shared_ptr<string>>> GetRelatedEntities(string si, string property, bool inverse, int count, const vector<string> &datasets);

        // internal use
        void WriteBatch(string& dataset, long lastOffset, shared_ptr<rocksdb::WriteBatch> writeBatch);

        void WriteEntity(shared_ptr<rocksdb::WriteBatch> writeBatch, const shared_ptr<DataSet> &dataset, string &json,
                         string id, vector<pair<string, string>> &outrefs);

        int AssertNamespace(string ns);

        int AssertProperty(string ns_name);

        shared_ptr<DataSet> AssertDataSet(string name);

        shared_ptr<DataSet> GetDataSet(string name);

        vector<shared_ptr<DataSet>> GetDataSets();

        string GetResourceId(string uri);

        void Delete();

        void Close();

        void Compact();

        shared_ptr<string> GetNamespacesJson();

        // void CreatePipe(string id, string dataset, shared_ptr<Pipe> pipe);
        // void CreatePipe(string id, string dataset, string cpp, string schedule);
        // void GetPipes();

    };
}

#endif //WEBOFDATA_STORE_H
