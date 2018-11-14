#include "Store.h"
#include "EntityHandler.h"
#include "DataSet.h"
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include <rocksdb/table_properties.h>
#include <rocksdb/cache.h>

#include <rapidjson/document.h>
#include <boost/filesystem.hpp>
#include <utility>
#include <sstream>
#include <set>

extern "C" {
    #include "xxhash.h"
}

namespace webofdata {

    using namespace rapidjson;
    using namespace std;

    using byte = unsigned char;
    using ulong = unsigned long;


    //----------------------------------------------------
    // Pipe 
    //----------------------------------------------------

    void Pipe::Run() {
        _running = true;
        while (_running){
            if (_dataset->GetCurrentSequenceId() > _lastSeen) {
                // todo: catch processing exception                        
                auto lastSeen = _store->WriteChangesToHandler(_dataset->GetName(), _lastSeen, 100, -1, shared_from_this());

                // store last seen
                // _store->storeOffset(_id);

                // update local
                _lastSeen = lastSeen;
            }
        }
    }

    void Pipe::RunOnce() {
        if (_dataset->GetCurrentSequenceId() > _lastSeen) {
            // todo: catch processing exception                        
            auto lastSeen = _store->WriteChangesToHandler(_dataset->GetName(), _lastSeen, 100, -1, shared_from_this());
            // update local
            _lastSeen = lastSeen;
        }
        cout << "last seen: " << _lastSeen;
    }

    bool IdentityTransformPipeLogic::ProcessEntity(shared_ptr<string> entityJson, shared_ptr<IStoreUpdate> context) {
        _store->StoreEntity(_targetDatasetName, entityJson);
        return true;
    }

    //----------------------------------------------------
    // Dataset 
    //----------------------------------------------------

    void DataSet::AssertColumnFamilies() {
        string store_prefix("dataset::store::");
        _resourceStoreColumnFamily = _store->AssertColumnFamily(store_prefix + std::to_string(_id));

        string size_prefix("dataset::size::");
        _resourceSizeColumnFamily = _store->AssertColumnFamily(size_prefix + std::to_string(_id));

        string log_prefix("dataset::log::");
        _resourceLogColumnFamily = _store->AssertColumnFamily(log_prefix + std::to_string(_id));

        string outrefs_prefix("dataset::outrefs::");
        _resourceOutRefsColumnFamily = _store->AssertColumnFamily(outrefs_prefix + std::to_string(_id));

        string inrefs_prefix("dataset::inrefs::");
        _resourceInRefsColumnFamily = _store->AssertColumnFamily(inrefs_prefix + std::to_string(_id));
    }

    void DataSet::LookupNextSeqId() {
        // iterate to the end of the resource log for the dataset id
        auto logIter = _store->GetDatabase()->NewIterator(ReadOptions(), _resourceLogColumnFamily);
        if (!logIter->status().ok()) {
            throw StoreException("Unable to get iterator over _resourceLogColumnFamily");
        }

        logIter->SeekToLast();
        ulong seq = 0;
        if (logIter->Valid()) {
            int checkDsId;
            memcpy((char *) &seq, logIter->key().data(), sizeof(seq));
        } else {
            seq = 0;
        }
        delete logIter;
        _nextSeqId = seq;
    }

    rocksdb::DB *Store::GetDatabase() {
        return _database;
    }

    Store::Store(string name, string location) {
        _name = std::move(name);
        _storeLocation = location;
        _logger = spdlog::get("wod_service_log");
        _namespacesJson = make_shared<string>("{}");
        _namespacesJsonDocument = make_shared<Document>();
        _namespacesJsonDocument->SetObject();
    }

    shared_ptr<string> Store::GetNamespacesJson() {
        return _namespacesJson;
    }

    void Store::Close() {
        delete _database;
    }

    void Store::Compact() {
        CompactionOptions options;
        std::vector<std::string> input_file_names;
        int output_level = 0;
        Status s = _database->CompactFiles(options, input_file_names, output_level);
    }

    void Store::StoreMetadataEntity(std::string data) {
        Slice val(data.data(), data.length());
        auto s = _database->Put(rocksdb::WriteOptions(), _globalStateColumnFamily, "store_entity", val);
        if (!s.ok()) {
             throw StoreException("Unable to store global state. Key: store_entity");
        }
    }

    void Store::StoreDatasetMetadataEntity(std::string dataset, std::string data) {
        string key("dataset_entity_" + dataset);
        Slice val(data.data(), data.length());
        auto s = _database->Put(rocksdb::WriteOptions(), _globalStateColumnFamily, key, val);
        if (!s.ok()) {
            throw StoreException("Unable to store global state. Key: " + key);
        }
    }

    string Store::GetMetadataEntity() {
        string value;
        auto status = _database->Get(ReadOptions(), _globalStateColumnFamily, "store_entity", &value);
        if (status.ok()) {
            return value;
        } else if (status.kNotFound) {
            return "{}";
        } else {
            throw StoreException("Error in get metadata entity");
        }
    }

    string Store::GetDatasetMetadataEntity(string dataset) {
        string value;
        string key("dataset_entity_" + dataset);
        auto status = _database->Get(ReadOptions(), _globalStateColumnFamily, key, &value);
        if (status.ok()) {
            return value;
        } else if (status.kNotFound) {
            return "{}";
        } else {
            throw StoreException("Error in get metadata entity");
        }
    }

    ColumnFamilyHandle *Store::AssertColumnFamily(string name) {
        std::lock_guard<std::mutex> lock(assert_column_family_mutex);
        auto res = _handlesByName.find(name);
        if (res != _handlesByName.end()) {
            return res->second;
        } else {

            ColumnFamilyHandle *cf;
            ColumnFamilyOptions cfoptions;
            cfoptions.compression = CompressionType::kLZ4Compression;

            auto s = _database->CreateColumnFamily(cfoptions, name, &cf);

            if (!s.ok()) {
                throw StoreException("Unable to create column family " + name);
            }

            _handlesByName[name] = cf;
            return cf;
        }
    }

    int Store::AssertNamespace(string ns) {

        if (ns.empty()) throw StoreException("Namespace cannot be empty");

        std::lock_guard<std::mutex> lock(assert_namespace_mutex);

        auto iter = _namespaceToIdIndex.find(ns);
        if (iter != _namespaceToIdIndex.end()) {
            return iter->second;
        }

        string value;
        auto status = _database->Get(ReadOptions(), _namespacesColumnFamily, ns, &value);
        if (status.ok()) {
            int val;
            memcpy((char *) &val, value.data(), sizeof(val));
            _namespaceToIdIndex[ns] = val;
            return val;
        } else if (status.kNotFound) {
            // wrap this is write batch
            _nextNamespaceId++;
            shared_ptr<char> databuffer(new char[sizeof(_nextNamespaceId)], ArrayDeleter<char>());
            memcpy(databuffer.get(), (const char *) &_nextNamespaceId, sizeof(_nextNamespaceId));

            Slice val(databuffer.get(), sizeof(_nextNamespaceId));
            auto s = _database->Put(rocksdb::WriteOptions(), _globalStateColumnFamily, "_next_namespace_id", val);
            if (!s.ok()) {
                throw StoreException("Unable to store global state. Key: _next_namespace_id");
            }

            // add the namespace entry, we only need one direction as we keep these in memory in both directions
            s = _database->Put(rocksdb::WriteOptions(), _namespacesColumnFamily, ns, val);
            if (!s.ok()) {
                throw StoreException("Unable to store namespace in _namespacesColumnFamily. Key: " + ns);
            }

            // update local cache
            _namespaceToIdIndex[ns] = _nextNamespaceId;
            _idToNamespaceIndex[_nextNamespaceId] = ns;

            // update namespace json
            auto keyStr = string("ns" + to_string(_nextNamespaceId));
            Value key(keyStr.data(), _namespacesJsonDocument->GetAllocator());
            // _namespacesJsonDocument->AddMember(key, Value(StringRef(ns.data())), _namespacesJsonDocument->GetAllocator());
            _namespacesJsonDocument->AddMember(key, Value().SetString(ns.data(), (int) ns.length(), _namespacesJsonDocument->GetAllocator()), _namespacesJsonDocument->GetAllocator());

            StringBuffer buffer;
            Writer<StringBuffer> writer(buffer);
            _namespacesJsonDocument->Accept(writer);
            _namespacesJson = make_shared<string>(buffer.GetString());

            return _nextNamespaceId;
        } else {
            throw StoreException("Error accessing _namespacesColumnFamily. Status was " + status.ToString());
        }
    }

    void Store::OpenRocksDb(string location) {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.compression = CompressionType::kLZ4Compression;

        vector<string> cfnames;
        DB::ListColumnFamilies(options, location, &cfnames);
        // status here is a bit bogus as it completes correctly for new stores but returns an IOError

        ColumnFamilyOptions cfoptions;
        cfoptions.compression = CompressionType::kLZ4Compression;

        if (cfnames.empty()) {
            _columnFamilies.push_back(ColumnFamilyDescriptor("default", ColumnFamilyOptions()));
        } else {
            for (const auto &cfname : cfnames) {
                _columnFamilies.push_back(ColumnFamilyDescriptor(cfname, ColumnFamilyOptions()));
            }
        }

        auto dbOpenStatus = DB::Open(options, location, _columnFamilies, &_handles, &_database);
        if (!dbOpenStatus.ok()) {
            throw StoreException("Unable to open database. Status: " + dbOpenStatus.ToString());
        }

        for (auto cfh : _handles) {
            auto id = cfh->GetID();
            auto name = cfh->GetName();
            _handlesByName[cfh->GetName()] = cfh;
        }

        // assert core column families
        _globalStateColumnFamily = AssertColumnFamily("global_state");
        _namespacesColumnFamily = AssertColumnFamily("namespaces");
        _pipeState = AssertColumnFamily("pipe_state");

        // load next dataset id
        string nextDataSetIdBytes;
        rocksdb::Status s = _database->Get(rocksdb::ReadOptions(), _globalStateColumnFamily, "_next_dataset_id",
                                           &nextDataSetIdBytes);
        if (s.ok()) {
            memcpy((char *) &_nextDataSetId, nextDataSetIdBytes.data(), sizeof(_nextDataSetId));
        } else {
            if (s.kNotFound) {
                _nextDataSetId = 0;
            } else {
                throw StoreException(
                        "Unable to read _next_dataset_id from _globalStateColumnFamily. Status: " + s.ToString());
            }
        }

        // load next namespace id
        string nextNamespaceIdBytes;
        s = _database->Get(rocksdb::ReadOptions(), _globalStateColumnFamily, "_next_namespace_id",
                           &nextNamespaceIdBytes);
        if (s.ok()) {
            memcpy((char *) &_nextNamespaceId, nextNamespaceIdBytes.data(), sizeof(_nextNamespaceId));
        } else {
            if (s.kNotFound) {
                _nextNamespaceId = 0;
            } else {
                throw StoreException(
                        "Unable to read _next_namespace_id from _globalStateColumnFamily. Status: " + s.ToString());
            }
        }

        // load existing namespace mappings
        auto nsiter = _database->NewIterator(ReadOptions(), _namespacesColumnFamily);
        for (nsiter->SeekToFirst(); nsiter->Valid(); nsiter->Next()) {
            auto key = nsiter->key().ToString();
            int val;
            memcpy((char *) &val, nsiter->value().data(), sizeof(val));

            _namespaceToIdIndex[key] = val;
            _idToNamespaceIndex[val] = key;

            auto keyStr = string("ns" + to_string(val));
            Value key1(keyStr.data(), _namespacesJsonDocument->GetAllocator());
            _namespacesJsonDocument->AddMember(key1, Value(StringRef(key.data())), _namespacesJsonDocument->GetAllocator());
        }

        // update json document
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        _namespacesJsonDocument->Accept(writer);
        _namespacesJson = make_shared<string>(buffer.GetString());

        // load next property id
        string nextPropertyIdBytes;
        s = _database->Get(rocksdb::ReadOptions(), _globalStateColumnFamily, "_next_property_id", &nextPropertyIdBytes);
        if (s.ok()) {
            memcpy((char *) &_nextPropertyId, nextPropertyIdBytes.data(), sizeof(_nextPropertyId));
        } else {
            if (s.kNotFound) {
                _nextPropertyId = 0;
            } else {
                throw StoreException(
                        "Unable to read _next_property_id from _globalStateColumnFamily. Status: " + s.ToString());
            }
        }

        // dataset keys begin dataset_name : dataset_id
        auto iter = _database->NewIterator(ReadOptions(), _globalStateColumnFamily);
        for (iter->Seek("dataset_"); iter->Valid() && iter->key().starts_with("dataset_"); iter->Next()) {

            auto dataset_name = iter->key().ToString().substr(8);
            // int dataset_id = atoi(iter->value().data());
            int dataset_id;
            memcpy(&dataset_id, (const char *) iter->value().data(), sizeof(dataset_id));

            shared_ptr<DataSet> ds = make_shared<DataSet>(shared_from_this(), dataset_name, dataset_id);
            _datasets[dataset_name] = ds;
        }
        delete iter;
    }

    Store::~Store() {
    }

    void Store::Delete() {
        try {
            delete _database;
            boost::filesystem::remove_all(boost::filesystem::path(_storeLocation));
        } catch (const exception &ex) {
            throw StoreException("Unable to delete store : " + string(ex.what()));
        }
    }

    void Store::StoreEntity(string datasetName, shared_ptr<string> data) {
        try {
            auto ds = AssertDataSet(std::move(datasetName));
            EntityHandler handler(shared_from_this(), ds, make_shared<rocksdb::WriteBatch>());
            Reader reader;
            StringStream ss(data->data());
            reader.Parse(ss, handler);
            handler.Flush();
        } catch (const ParseException &ex) {
            throw StoreException(
                    "Error parsing entity : error at position: " + std::to_string(ex.Offset()) + " parse error was: " +
                    string(GetParseError_En(ex.Code())));
        } catch (const StoreException &ex) {
            throw ex;
        } catch (const exception &ex) {
            throw StoreException("Error storing entity " + string(ex.what()));
        }
    }

    long Store::StoreEntities(std::istream &data, string dataset) {
        try {
            cout << "in store entities - assert dataset " << dataset << endl;
            auto ds = AssertDataSet(std::move(dataset));
            EntityHandler handler(shared_from_this(), ds, make_shared<rocksdb::WriteBatch>());
            Reader reader;
            IStreamWrapper isw(data);
            reader.Parse(isw, handler);
            handler.Flush();
            return handler.GetEntityCount();
        } catch (const ParseException &ex) {
            cout << "parsing error" << ex.what() << endl;
            cout << "Error parsing entity : error at position: " + std::to_string(ex.Offset()) + " parse error was: " +
                    string(GetParseError_En(ex.Code()));
            cout << endl;
            throw StoreException(
                    "Error parsing entity : error at position: " + std::to_string(ex.Offset()) + " parse error was: " +
                    string(GetParseError_En(ex.Code())));
        } catch (const StoreException &ex) {
            cout << "parsing error inner store exception" << ex.what() << endl;
            throw ex;
        } catch (const exception &ex) {
            cout << "storage exception" << ex.what() << endl; 
            throw StoreException("Error storing entity " + string(ex.what()));
        }
    }

    void Store::WriteBatch(string& dataset,long firstOffset, shared_ptr<rocksdb::WriteBatch> writeBatch) {
        auto result = _database->Write(WriteOptions(), writeBatch.get());
        if (!result.ok()) {
            throw StoreException("Unable to write batch. Error: " + result.ToString());
        }
    }

    shared_ptr<DataSet> Store::GetDataSet(string name) {

        std::lock_guard<std::mutex> lock(assert_dataset_mutex);

        auto iter = _datasets.find(name);
        if (iter != _datasets.end()) {
            return iter->second;
        }
        return nullptr;
    }

    void Store::DeleteDataSet(string dataset) {
        // TODO: need to schedule a job for a background thread to remove data from store.
        // TODO: fix scope of this lock.
        std::lock_guard<std::mutex> lock(assert_dataset_mutex);
        auto iter = _datasets.find(dataset);
        if (iter != _datasets.end()) {
            _datasets.erase(iter);
        }

        if (iter == _datasets.end()) return; // no dataset
        auto ds = iter->second;

        // delete all the column families
        auto cfs = ds->GetColumnFamilies();
        for (auto cf : cfs) {
            _database->DropColumnFamily(cf);
            _database->DestroyColumnFamilyHandle(cf);
        }

        // delete global info about dataset
        std::string key = string("dataset_") + ds->GetName();
        _database->Delete(rocksdb::WriteOptions(), _globalStateColumnFamily, key);

    }

    shared_ptr<DataSet> Store::AssertDataSet(string name) {

        std::lock_guard<std::mutex> lock(assert_dataset_mutex);

        auto iter = _datasets.find(name);
        if (iter != _datasets.end()) {
            return iter->second;
        }

        _nextDataSetId++;
        shared_ptr<char> buffer(new char[4], ArrayDeleter<char>());
        memcpy(buffer.get(), (const char *) &_nextDataSetId, sizeof(_nextDataSetId));
        Slice val(buffer.get(), sizeof(_nextDataSetId));

        auto s = _database->Put(rocksdb::WriteOptions(), _globalStateColumnFamily, "_next_dataset_id", val);
        if (!s.ok()) {
            throw StoreException("Unable to write to write _next_dataset_id to _globalStateColumnFamily");
        }

        // store the new name and id
        std::string key = string("dataset_") + name;
        s = _database->Put(rocksdb::WriteOptions(), _globalStateColumnFamily, key, val);
        if (!s.ok()) {
            throw StoreException("Unable to write to write " + key + " to _globalStateColumnFamily");
        }

        // add dataset to collection
        auto ds = make_shared<DataSet>(shared_from_this(), name, _nextDataSetId);
        _datasets[name] = ds;
        return ds;
    }

    vector<shared_ptr<DataSet>> Store::GetDataSets() {
        vector<shared_ptr<DataSet>> datasets;
        for (auto const &kv : _datasets) {
            datasets.push_back(kv.second);
        }
        return datasets;
    }

    // This is called from the parser handler and should probably be a friend method.
    void Store::WriteEntity(shared_ptr<rocksdb::WriteBatch> writeBatch, const shared_ptr<DataSet> &dataset,
                            string &data, string id, vector<pair<string, string>> &outrefs) {
        // lookup existing entity
        // if the same load existing entity bytes and compare JSON documents
        auto idlen = id.length();

        shared_ptr<char> buffer(new char[8], ArrayDeleter<char>());
        long dataLength = data.length();
        memcpy(buffer.get(), (char *) &dataLength, sizeof(dataLength));
        Slice dataLengthVal(buffer.get(), sizeof(dataLength));

        PinnableSlice existingLengthValue;
        bool isUpdate = false;
        bool isInsert = false;
        auto status = _database->Get(ReadOptions(), dataset->GetSizeColumnFamily(), id, &existingLengthValue);
        if (status.ok()) {
            // entity exists and has a size of value
            long existingDataSize;
            memcpy((char *) &existingDataSize, existingLengthValue.data(), sizeof(existingDataSize));

            if (data.length() != existingDataSize) {
                isUpdate = true;
            } else {
                // load and compare objects
                PinnableSlice existingData;
                auto getDataStatus = _database->Get(ReadOptions(),
                                                    dataset->GetStoreColumnFamily(),
                                                    id,
                                                    &existingData);

                if (getDataStatus.ok()) {
                    Document currentData;
                    string exdat(existingData.data(), existingData.size());
                    currentData.Parse((char *) existingData.data(), existingData.size());

                    Document newData;
                    newData.Parse((char *) data.data());

                    if (newData == currentData) { // TODO: test this
                        // do nothing
                        return;
                    } else {
                        isUpdate = true;
                    }
                } else {
                    throw StoreException("Data integrity issue in WriteEntity");
                }
            }
        } else if (status.kNotFound) {
            isInsert = true;
        } else {
            throw StoreException("Unable to WriteEntity. Error : " + status.ToString());
        }

        // -------------------------------------------------------------------------------------
        // Write data
        // id=>data, id=>data length
        // -------------------------------------------------------------------------------------

        writeBatch->Put(dataset->GetSizeColumnFamily(), id, dataLengthVal);
        writeBatch->Put(dataset->GetStoreColumnFamily(), id, data);

        // -------------------------------------------------------------------------------------
        // Write log entry
        // seq:timestamp -> entityid
        // -------------------------------------------------------------------------------------

        ulong logSeqId = dataset->GetNextSequenceId();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        auto sizeLogBuffer = sizeof(logSeqId) + sizeof(ms);
        shared_ptr<char> seqbuffer(new char[sizeLogBuffer], ArrayDeleter<char>());

        memcpy(seqbuffer.get(), (char *) &logSeqId, sizeof(logSeqId));
        memcpy(seqbuffer.get() + sizeof(logSeqId), (char *) &ms, sizeof(ms));

        Slice seqkey(seqbuffer.get(), sizeLogBuffer);
        writeBatch->Put(dataset->GetLogColumnFamily(), seqkey, id);

        // -------------------------------------------------------------------------------------
        // If update, then find and remove refs (do a diff)
        // -------------------------------------------------------------------------------------

        if (isUpdate) {
            // outrefs:  idsize : entityId : propidsize : propid : relatedentityidsize : relatedentityid -> relatedentityid
            int size_id = id.length();
            auto searchKeyBufferSize = sizeof(size_id) + size_id;
            shared_ptr<char> searchKeyBuffer(new char[searchKeyBufferSize], ArrayDeleter<char>());

            // idlen : id
            memcpy(searchKeyBuffer.get(), (char *) &size_id, sizeof(size_id));
            memcpy(searchKeyBuffer.get() + sizeof(size_id), id.data(), id.length());

            string key = string(searchKeyBuffer.get(), searchKeyBufferSize);

            rocksdb::Iterator *it = _database->NewIterator(rocksdb::ReadOptions(), dataset->GetOutRefsColumnFamily());
            for (it->Seek(key);
                 it->Valid() && it->key().starts_with(key);
                 it->Next()) {

                string relatedEntityId(it->value().data());
                string relatedPropertyId;

                // read property id length
                int propIdLength;
                memcpy((char *) &propIdLength, it->key().data() + searchKeyBufferSize, sizeof(propIdLength));

                // get property id
                memcpy((char *) &relatedPropertyId, it->key().data() + searchKeyBufferSize + sizeof(propIdLength), propIdLength);

                // try and find and remove it from outrefs
                auto p = pair<string, string>(relatedPropertyId, relatedEntityId);
                auto position = std::find(outrefs.begin(), outrefs.end(), p);
                if (position != outrefs.end()) {
                    outrefs.erase(position);
                } else {
                    // remove ref
                    writeBatch->Delete(dataset->GetOutRefsColumnFamily(), it->key());

                    // remove inverse ref as well
                    // auto invrefKeyBufferSize = inverseEntityDatasetId.length() + id.length() + sizeof(relatedPropertyId);
                    int size_property = relatedPropertyId.length();
                    int size_relatedEntityId = relatedEntityId.length();

                    auto refKeyBufferSize = sizeof(size_id) + sizeof(size_property) + sizeof(size_relatedEntityId)
                                            + size_id + size_property + size_relatedEntityId;
                    shared_ptr<char> refKeyBuffer(new char[refKeyBufferSize], ArrayDeleter<char>());

                    memcpy(refKeyBuffer.get(), (char *) &size_relatedEntityId, sizeof(size_relatedEntityId));
                    memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId), relatedEntityId.data(),
                           relatedEntityId.length());

                    // proplen : prop
                    memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + relatedEntityId.length(),
                           (char *) &size_property, sizeof(size_property));
                    memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + relatedEntityId.length() +
                           sizeof(size_property),
                           relatedPropertyId.data(), relatedPropertyId.length());

                    // relatedidlen : relatedid
                    memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + id.length() + sizeof(size_property) +
                           relatedPropertyId.length(),
                           (char *) &size_id, sizeof(size_id));
                    memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + id.length() + sizeof(size_property) +
                           relatedPropertyId.length() + sizeof(size_id),
                           id.data(), id.length());

                    string invkey(refKeyBuffer.get(), refKeyBufferSize);

                    writeBatch->Delete(dataset->GetInRefsColumnFamily(), invkey);
                }
            }
            delete it;
        }

        // insert the remaining refs
        for (auto ref : outrefs) {
            // make ref key and value
            // entityid datasetid propertyid -> relatedentity
            auto propId = ref.first;
            auto relatedEntityId = ref.second;

            int size_id = id.length();
            int size_property = propId.length();
            int size_relatedEntityId = relatedEntityId.length();

            // key
            auto refKeyBufferSize = sizeof(size_id) + sizeof(size_property) + sizeof(size_relatedEntityId)
                                    + size_id + size_property + size_relatedEntityId;
            shared_ptr<char> refKeyBuffer(new char[refKeyBufferSize], ArrayDeleter<char>());

            // idlen : id
            memcpy(refKeyBuffer.get(), (char *) &size_id, sizeof(size_id));
            memcpy(refKeyBuffer.get() + sizeof(size_id), id.data(), id.length());

            // proplen : prop
            memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length(), (char *) &size_property, sizeof(size_property));
            memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length() + sizeof(size_property),
                   propId.data(), propId.length());

            // relatedidlen : relatedid
            memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length() + sizeof(size_property) + propId.length(),
                   (char *) &size_relatedEntityId, sizeof(size_relatedEntityId));
            memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length() + sizeof(size_property) + propId.length() +
                   sizeof(size_relatedEntityId),
                   relatedEntityId.data(), relatedEntityId.length());

            string key(refKeyBuffer.get(), refKeyBufferSize);

            // insert
            writeBatch->Put(dataset->GetOutRefsColumnFamily(), key, relatedEntityId);


            // AND the inverse key
            // same size as the out key so reuse the buffer from above

            memcpy(refKeyBuffer.get(), (char *) &size_relatedEntityId, sizeof(size_relatedEntityId));
            memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId), relatedEntityId.data(), relatedEntityId.length());

            // proplen : prop
            memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + relatedEntityId.length(),
                   (char *) &size_property, sizeof(size_property));
            memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + relatedEntityId.length() + sizeof(size_property),
                   propId.data(), propId.length());

            // relatedidlen : relatedid
            memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + relatedEntityId.length() +
                   sizeof(size_property) + propId.length(),
                   (char *) &size_id, sizeof(size_id));
            memcpy(refKeyBuffer.get() + sizeof(size_relatedEntityId) + relatedEntityId.length() +
                   sizeof(size_property) + propId.length() + sizeof(size_id),
                   id.data(), id.length());

            string invkey(refKeyBuffer.get(), refKeyBufferSize);

            writeBatch->Put(dataset->GetInRefsColumnFamily(), invkey, id);
        }
    }

    string Store::GetResourceId(string resourceUri) {
        std::string httpStart("http");

        if (resourceUri.compare(0, httpStart.length(), httpStart) == 0) {
            // find last hash or slash
            string name;
            string prefix;

            auto foundAt = resourceUri.find_last_of('#');
            if (foundAt != std::string::npos) {
                prefix = resourceUri.substr(0, foundAt + 1);
                name = resourceUri.substr(foundAt + 1);
            } else {
                foundAt = resourceUri.find_last_of('/');
                if (foundAt != std::string::npos) {
                    prefix = resourceUri.substr(0, foundAt + 1);
                    name = resourceUri.substr(foundAt + 1);
                } else {
                    throw StoreException("No hash or slash found in URL");
                }
            }

            // lookup expansion in namespace index
            int nsid = -1;
            auto iter = _namespaceToIdIndex.find(prefix);
            if (iter != _namespaceToIdIndex.end()) {
                nsid = iter->second;
            } else {
                throw StoreException("Namespace prefix not found. Prefix is: " + prefix);
            }

            return string("ns" + std::to_string(nsid) + string(":") + name);

        } else {
            throw StoreException("Resource URL doesnt start with http ");
        }
    }

    int Store::AssertProperty(string ns_name) {
        std::lock_guard<std::mutex> lock(assert_property_mutex);

        // wrap this in mutex or use Intel TBB concurrent map
        auto iter = _propertyToIdIndex.find(ns_name);
        if (iter != _propertyToIdIndex.end()) {
            return iter->second;
        }

        // lookup in rocksb
        string value;
        auto cf = AssertColumnFamily("property_index"); // this should just be a member variable

        auto status = _database->Get(ReadOptions(), cf, ns_name, &value);
        if (status.ok()) {
            int propertyId;
            memcpy((char *) &propertyId, value.data(), sizeof(propertyId));
            return propertyId;
        } else if (status.kNotFound) {
            // wrap this is write batch
            _nextPropertyId++;
            shared_ptr<char> buffer(new char[8], ArrayDeleter<char>());
            memcpy(buffer.get(), (char *) &_nextPropertyId, sizeof(_nextPropertyId));
            Slice val(buffer.get(), sizeof(_nextPropertyId));

            auto s = _database->Put(rocksdb::WriteOptions(), _globalStateColumnFamily, "_next_property_id", val);
            if (!s.ok()) {
                throw StoreException("Error writing _next_property_id to _globalStateColumnFamily " + s.ToString());
            }

            // add entry
            s = _database->Put(rocksdb::WriteOptions(), cf, ns_name, val);
            if (!s.ok()) {
                throw StoreException("Error writing entry " + s.ToString());
            }

            // add inverse entry
            s = _database->Put(rocksdb::WriteOptions(), cf, val, ns_name);
            if (!s.ok()) {
                throw StoreException("Error writing inverse entry " + s.ToString());
            }

            // update local cache
            _propertyToIdIndex[ns_name] = _nextPropertyId;

            return _nextPropertyId;
        } else {
            throw StoreException("Error in assert property " + status.ToString());
        }
    }

    shared_ptr<string> Store::GetEntity(string id, const vector<string> &datasets) {
        string value;

        if (datasets.size() == 1) {
            auto ds = GetDataSet(datasets[0]);
            auto status = _database->Get(ReadOptions(), ds->GetStoreColumnFamily(), id, &value);
            if (status.ok()) {
                return make_shared<string>(value);
            }

            return make_shared<string>("{ \"@id\" : \"" + id + "\" }");
        }
        
        vector<shared_ptr<string>> partials;
        if (datasets.empty()) {
            // look in all
            for (auto const& ds: _datasets) {
                // do this in parallel? - TODO: ADD NEW INDEX for this.
                // auto p = _database->Get(ReadOptions(), ds.second->GetStoreColumnFamily()
                shared_ptr<string> entityValue = make_shared<string>();
                auto status = _database->Get(ReadOptions(), ds.second->GetStoreColumnFamily(), id, entityValue.get());
                partials.push_back(entityValue);
            }
        } else {
            for (auto const &dsname : datasets) {
                auto ds = this->GetDataSet(dsname);
                shared_ptr<string> entityValue = make_shared<string>();
                auto status = _database->Get(ReadOptions(), ds->GetStoreColumnFamily(), id, entityValue.get());
                partials.push_back(entityValue);
            }
        }

        // merge partials
        if (partials.size() > 0) {
            // merge entities
            Document result;
            result.ParseInsitu((char*) partials[0]->data());
            Value::AllocatorType& alloc = result.GetAllocator();

            // we create each document and keep a reference to it
            vector<shared_ptr<Document>> entityDocuments;
            for (int i=1;i<partials.size(); i++) {
                auto d = make_shared<Document>();
                d->ParseInsitu((char*) partials[i]->data());

                // iterate each property and see if it exists on the main result:
                for (auto& m : d->GetObject()) {
                    // m.name.GetString(), kTypeNames[m.value.GetType()]);
                    auto iter = result.FindMember(m.name.GetString());
                    if (iter != result.MemberEnd()) {
                        // property with name exists so need to turn into array or add to array
                        if (iter->value.GetType() == Type::kArrayType) {
                            auto listMember = iter->value.FindMember(m.value);
                            if (listMember == iter->value.MemberEnd()) {
                                // it doesnt exist so add it
                                iter->value.PushBack(m.value.Move(), alloc);
                            }
                        } else {
                            // turn into array if values are different
                            if (m.value != iter->value) {
                                // create array add existing value, add new value, replace old value with array
                                Value newArray;
                                newArray.SetArray();
                                newArray.PushBack(iter->value.Move(), alloc);
                                newArray.PushBack(m.value.Move(), alloc);
                                result.RemoveMember(m.name);
                                result.AddMember(m.name, newArray.Move(), alloc);
                            }
                        }
                    } else {
                        // no existing property so just add
                        result.AddMember(m.name, m.value.Move(), alloc);
                    }
                }
            }

            // write it out to a string
            StringBuffer buffer;
            Writer<StringBuffer> writer(buffer);
            result.Accept(writer);
            return make_shared<string>(buffer.GetString(), buffer.GetLength());        
        } else {
            // if we havent found any then return the stub        
            return make_shared<string>("{ \"@id\" : \"" + id + "\" }");
        }
    }

    void Store::WriteEntityToStream(string id, const vector<string> &datasets, EntityStreamWriter &stream) {
        auto entityJson = this->GetEntity(id, datasets);
        stream.WriteJson(",");
        stream.WriteJson(entityJson->data(), entityJson->size());
    }

    void Store::WriteCompleteEntityToStream(string sid, const vector<string> &datasets, EntityStreamWriter &stream) {
        auto rid = GetResourceId(sid);
        auto entityJson = this->GetEntity(rid, datasets);
        stream.WriteJson("[ { \"@id\" : \"@context\", \"namespaces\" : ");
        stream.WriteJson(_namespacesJson->data(), (int) _namespacesJson->length());
        stream.WriteJson("},");
        stream.WriteJson(entityJson->data(), entityJson->size());
        stream.WriteJson("]");
    }

    shared_ptr<vector<shared_ptr<string>>> Store::GetRelatedEntities(string si, string property, bool inverse, int count, const vector<string> &datasetNames) {

        vector<shared_ptr<DataSet>> datasets;
        string id = GetResourceId(si);
        auto results = make_shared<vector<shared_ptr<string>>>();
        int written = 0;

        // this can be optimised
        if (datasetNames.empty()) {
            for (auto const &ds : GetDataSets()) {
                datasets.push_back(ds);
            }
        } else {
            // lookup datasets
            for (auto const &name : datasetNames) {
                auto ds = GetDataSet(name);
                if (ds != nullptr) {
                    datasets.push_back(ds);
                }
            }
        }

        std::set<shared_ptr<string>,bool(*)(shared_ptr<string> , shared_ptr<string>)> hits(Store::StrPtrComp);

        for (auto const &ds : datasets) {

            ColumnFamilyHandle* refsColumnFamilyHandle = nullptr;
            if (inverse) {
                refsColumnFamilyHandle = ds->GetInRefsColumnFamily();
            } else {
                refsColumnFamilyHandle = ds->GetOutRefsColumnFamily();
            }

            string key; // index search value
            if (property.empty()) {
                // make prefix for search
                int size_id = id.length();
                auto refKeyBufferSize = sizeof(size_id) + size_id;
                shared_ptr<char> refKeyBuffer(new char[refKeyBufferSize], ArrayDeleter<char>());
                // idlen : id
                memcpy(refKeyBuffer.get(), (char *) &size_id, sizeof(size_id));
                memcpy(refKeyBuffer.get() + sizeof(size_id), id.data(), id.length());
                key = string(refKeyBuffer.get(), refKeyBufferSize);
            } else {
                // make prefix for search including property
                int size_id = id.length();
                int size_prop = property.length();
                auto refKeyBufferSize = sizeof(size_id) + size_id + sizeof(size_prop) + size_prop;
                shared_ptr<char> refKeyBuffer(new char[refKeyBufferSize], ArrayDeleter<char>());

                // idlen : id : proplen : property
                memcpy(refKeyBuffer.get(), (char *) &size_id, sizeof(size_id));
                memcpy(refKeyBuffer.get() + sizeof(size_id), id.data(), id.length());

                memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length(), (char *) &size_prop, sizeof(size_prop));
                memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length() + sizeof(size_prop), property.data(), property.length());

                key = string(refKeyBuffer.get(), refKeyBufferSize);
            }

            // search and iterate keys
            rocksdb::Iterator *it = _database->NewIterator(rocksdb::ReadOptions(), refsColumnFamilyHandle);

            for (it->Seek(key);
                    it->Valid() && it->key().starts_with(key);
                    it->Next()) {

                shared_ptr<string> val = make_shared<string>(it->value().data(), it->value().size());

                // if we can insert the result id into the hits set then add the entity to result
                if (hits.insert(val).second) {                        
                    // this adds the merged entity into the result
                    results->push_back(this->GetEntity(*val, datasetNames)); // TODO: optimise this to not copy the string id, use shared pointer?
                    if (count > -1 && written == count) {
                        // paged result limit hit so break iterator loop
                        break;
                    }
                }
            }
            delete it;
        }

        return results;
    }

    void
    Store::WriteRelatedEntitiesToStream(string si, string property, bool inverse, long skip, int count, const vector<string> &datasetNames,
                                        EntityStreamWriter &stream) {

        stream.WriteJson("[ { \"@id : \"@context\", \"namespaces\" : ");
        stream.WriteJson(_namespacesJson->data(), (int) _namespacesJson->length());
        stream.WriteJson("},");

        vector<shared_ptr<DataSet>> datasets;
        string id = GetResourceId(si);
        auto results = make_shared<vector<shared_ptr<string>>>();
        int written = 0;

        // this can be optimised
        if (datasetNames.empty()) {
            for (auto const &ds : GetDataSets()) {
                datasets.push_back(ds);
            }
        } else {
            // lookup datasets
            for (auto const &name : datasetNames) {
                auto ds = GetDataSet(name);
                if (ds != nullptr) {
                    datasets.push_back(ds);
                }
            }
        }

        std::set<shared_ptr<string>,bool(*)(shared_ptr<string> , shared_ptr<string>)> hits(Store::StrPtrComp);

        for (auto const &ds : datasets) {

            ColumnFamilyHandle* refsColumnFamilyHandle = nullptr;
            if (inverse) {
                refsColumnFamilyHandle = ds->GetInRefsColumnFamily();
            } else {
                refsColumnFamilyHandle = ds->GetOutRefsColumnFamily();
            }

            string key; // index search value
            if (property.empty()) {
                // make prefix for search
                int size_id = id.length();
                auto refKeyBufferSize = sizeof(size_id) + size_id;
                shared_ptr<char> refKeyBuffer(new char[refKeyBufferSize], ArrayDeleter<char>());
                // idlen : id
                memcpy(refKeyBuffer.get(), (char *) &size_id, sizeof(size_id));
                memcpy(refKeyBuffer.get() + sizeof(size_id), id.data(), id.length());
                key = string(refKeyBuffer.get(), refKeyBufferSize);
            } else {
                // make prefix for search including property
                int size_id = id.length();
                int size_prop = property.length();
                auto refKeyBufferSize = sizeof(size_id) + size_id + sizeof(size_prop) + size_prop;
                shared_ptr<char> refKeyBuffer(new char[refKeyBufferSize], ArrayDeleter<char>());

                // idlen : id : proplen : property
                memcpy(refKeyBuffer.get(), (char *) &size_id, sizeof(size_id));
                memcpy(refKeyBuffer.get() + sizeof(size_id), id.data(), id.length());

                memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length(), (char *) &size_prop, sizeof(size_prop));
                memcpy(refKeyBuffer.get() + sizeof(size_id) + id.length() + sizeof(size_prop), property.data(), property.length());

                key = string(refKeyBuffer.get(), refKeyBufferSize);
            }

            // search and iterate keys
            rocksdb::Iterator *it = _database->NewIterator(rocksdb::ReadOptions(), refsColumnFamilyHandle);

            long skipped = skip;
            for (it->Seek(key);
                    it->Valid() && it->key().starts_with(key);
                    it->Next()) {

                if (skipped > 0) {
                    skipped = skipped - 1;
                } else {
                    shared_ptr<string> relatedEntityId = make_shared<string>(it->value().data(), it->value().size());

                    // if we can insert the result id into the hits set then add the entity to result
                    if (hits.insert(relatedEntityId).second) {                        
                        // this adds the merged entity into the result
                        auto entityJson = this->GetEntity(*relatedEntityId, datasetNames);
                        stream.WriteJson(",");
                        stream.WriteJson(entityJson->data(), entityJson->size());

                        if (count > -1 && written == count) {
                            // paged result limit hit so break iterator loop
                            break;
                        }
                    }
                }
            }
            delete it; // needs to be in finally

            // write next token
            if (written > 0 && written == count) {                
                auto token = string("{ \"datasets\" : [ ");

                bool first = true;
                for (auto const &ds : datasetNames ){
                    if (first) {
                        token = token + "\"" + ds + "\"";
                        first = false;
                    } else {
                        token = token + ", \"" + ds + "\"";
                    }
                }
                token = token + "]";

                 // to_string(shard) + "_" + lastId + "_1";
                string tokenTemplate(", { \"@id\" : \"@continuation\" , \"wod:next-data\" : \"" + token + "\" }");
                stream.WriteJson(tokenTemplate); 
            }

            stream.WriteJson("]");
        }
    }

    shared_ptr<string> Store::GetEntities(string dataset, string lastId, int count, int shard, shared_ptr<vector<shared_ptr<string>>> result) {
        auto ds = GetDataSet(std::move(dataset)); 
        int written = 0;

        // auto readOptions = rocksdb::ReadOptions();
        // readOptions.fill_cache = false;
        auto cf = ds->GetStoreColumnFamily();
        rocksdb::Iterator *it = _database->NewIterator(rocksdb::ReadOptions(), cf);
        Slice lastWrittenKey;

        Slice lastIdSlice(lastId);
        if (lastId.empty()) {
            it->SeekToFirst();
        } else {
            it->Seek(lastIdSlice);
            if (it->Valid()) {
                it->Next();
            }
        }

        for (; it->Valid(); it->Next()) {
            if (shard == -1) {
                lastWrittenKey = it->key();
                result->push_back(make_shared<string>(it->value().data(), it->value().size()));
                written++;
            } else {
                if (shard == XXH64(it->key().data(), it->key().size(), 0) % 4) {
                    lastWrittenKey = it->key();
                    result->push_back(make_shared<string>(it->value().data(), it->value().size()));
                    written++;
                }
            }

            if (count > 0 && written == count) {
                break;
            }
        }

        delete it;

        lastId = lastWrittenKey.ToString();
        return make_shared<string>(lastId);
    }

    shared_ptr<vector<string>> Store::GetDataSetShardTokens(string dataset, int shardCount) {
        auto tokens = make_shared<vector<string>>();
        for (int x=0;x<shardCount;x++) {
            ostringstream ss;
            ss << x << "_";
            tokens->push_back(ss.str());
        }
        return tokens;
    }

    void Store::WriteEntitiesToStream(string dataset, string lastId, int count, int shard, EntityStreamWriter &stream) {
        auto ds = GetDataSet(std::move(dataset)); // TODO: check it exists

        stream.WriteJson("[ { \"@id\" : \"@context\", \"namespaces\" : ");
        stream.WriteJson(_namespacesJson->data(), _namespacesJson->length());
        stream.WriteJson("}");

        int written = 0;

        auto readOptions = rocksdb::ReadOptions();
        readOptions.fill_cache = false;
        // readOptions.readahead_size = 256;
        auto cf = ds->GetStoreColumnFamily();
        rocksdb::Iterator *it = _database->NewIterator(readOptions, cf);
        Slice lastWrittenKey;

        Slice lastIdSlice(lastId);
        if (lastId.empty()) {
            it->SeekToFirst();
        } else {
            it->Seek(lastIdSlice);
            if (it->Valid()) {
                it->Next();
            }
        }

        for (; it->Valid(); it->Next()) {
            if (shard == -1) {
                lastWrittenKey = it->key();
                stream.WriteJson(",", 1);
                auto data = it->value().data();
                auto size = it->value().size();
                stream.WriteJson(data, size); // TODO: check this size thing out
                written++;
            } else {
                if (shard == XXH64(it->key().data(), it->key().size(), 0) % 4) {
                    lastWrittenKey = it->key();
                    stream.WriteJson(",", 1);
                    auto data = it->value().data();
                    auto size = it->value().size();
                    stream.WriteJson(data, size); // TODO: check this size thing out
                    written++;
                }
            }

            if (count > 0 && written == count) {
                break;
            }
        }

        delete it;

        // write continuation entity
        if (count > 0 && written == count) {
            auto lastId = lastWrittenKey.ToString();
            if (!lastId.empty()) {
                string nextContinuationToken(lastId + ":" + to_string(shard));
                // write out continuation token entity
                auto token = to_string(shard) + "_" + lastId;
                string tokenTemplate(", { \"@id\" : \"@continuation\" , \"wod:next-data\" : \"" + token + "\" }");
                stream.WriteJson(tokenTemplate); 
            }
        }

        stream.WriteJson("]");
    }

    shared_ptr<vector<string>> Store::GetChanges(string dataset, ulong sequence, int count) {
        auto ds = GetDataSet(std::move(dataset));
        auto result = make_shared<vector<string>>();

        // get iterator over the log
        auto seqBufferSize = sizeof(sequence);
        shared_ptr<char> seqBuffer(new char[seqBufferSize], ArrayDeleter<char>());
        memcpy(seqBuffer.get(), (char *) &sequence, sizeof(sequence));
        Slice seqSlice(seqBuffer.get(), seqBufferSize);

        int takenCount = 0;
        string entityId;

        if (sequence <= ds->GetCurrentSequenceId()) {
            auto readOptions = rocksdb::ReadOptions();
            readOptions.fill_cache = false;
            rocksdb::Iterator *it = _database->NewIterator(readOptions, ds->GetLogColumnFamily());
            for (it->Seek(seqSlice); it->Valid(); it->Next()) {
                takenCount++;
                result->push_back(it->value().ToString());
                // thats enough for now
                if (takenCount == count) { break; }
            }
            delete it;
        }

        return result;
    }

    shared_ptr<vector<string>> Store::GetChangesShardTokens(string dataset, int shardCount) {
        auto tokens = make_shared<vector<string>>();
        for (int x=0;x<shardCount;x++) {
            ostringstream ss;
            // shard, seq, generation
            ss << x << "_" << -1 << "_" << "1";
            tokens->push_back(ss.str());
        }
        return tokens;
    }

    ulong Store::WriteChangesToHandler(string dataset, ulong from, int count, int shard, shared_ptr<ChangeHandler> handler) {
        auto ds = GetDataSet(std::move(dataset));

        // get iterator over the log
        auto seqBufferSize = sizeof(from);
        shared_ptr<char> seqBuffer(new char[seqBufferSize], ArrayDeleter<char>());
        memcpy(seqBuffer.get(), (char *) &from, sizeof(from));

        ulong lastWrittenSequence = from;
        int written = 0;

        rocksdb::Iterator *it = _database->NewIterator(rocksdb::ReadOptions(), ds->GetLogColumnFamily());

        Slice seqSlice(seqBuffer.get(), seqBufferSize);
        if (from == 0) {
            it->SeekToFirst();
        } else {
            it->Seek(seqSlice);
            if (it->Valid()) {
                it->Next();
            }
        }
        auto storeColumnFamily = ds->GetStoreColumnFamily();
        ReadOptions readOptions;

        for (; it->Valid(); it->Next()) {
            long itemSequence = 0;
            memcpy((char*)&itemSequence, it->key().data(), sizeof(itemSequence));

            if (shard == -1) {
                lastWrittenSequence = itemSequence;
                PinnableSlice value;
                auto status = _database->Get(readOptions, storeColumnFamily, it->value(), &value);
                if (status.ok()) {
                    auto entity = make_shared<string>(value.data(), value.size());
                    handler->ProcessEntity(entity);    
                    written++;
                } else {
                    // do something...
                    throw StoreException("Error expected entity not found in dataset");
                }
            } else {
                if (shard == itemSequence % 4) {
                    lastWrittenSequence = itemSequence;
                    PinnableSlice value;
                    auto status = _database->Get(readOptions, storeColumnFamily, it->value(), &value);
                    if (status.ok()) {
                        auto entity = make_shared<string>(value.data(), value.size());
                        handler->ProcessEntity(entity);    
                        written++;
                    } else {
                        // do something...
                    }
                }
            }

            // thats quite enough for now
            if (count > 0 && written == count) { break; }
        }
        delete it;

        // return last sequence id written
        return lastWrittenSequence;
    }

    void Store::WriteChangesToStream(string dataset, long from, int count, int shard, EntityStreamWriter &writer)
    {
        auto ds = GetDataSet(std::move(dataset));

        // get iterator over the log
        auto seqBufferSize = sizeof(from);
        shared_ptr<char> seqBuffer(new char[seqBufferSize], ArrayDeleter<char>());
        memcpy(seqBuffer.get(), (char *) &from, sizeof(from));

        long lastWrittenSequence = from;
        int written = 0;

        rocksdb::Iterator *it = _database->NewIterator(rocksdb::ReadOptions(), ds->GetLogColumnFamily());

        Slice seqSlice(seqBuffer.get(), seqBufferSize);
        if (from == -1) {
            it->SeekToFirst();
        } else {
            it->Seek(seqSlice);
            if (it->Valid()) {
                it->Next();
            }
        }

        writer.WriteJson("[ { \"@id\" : \"@context\", \"namespaces\" : ");
        writer.WriteJson(_namespacesJson->data(), _namespacesJson->length());
        writer.WriteJson("}");

        auto storeColumnFamily = ds->GetStoreColumnFamily();
        ReadOptions readOptions;

        for (; it->Valid(); it->Next()) {
            long itemSequence = 0;
            memcpy((char*)&itemSequence, it->key().data(), sizeof(itemSequence));

            if (shard == -1) {
                lastWrittenSequence = itemSequence;
                PinnableSlice value;
                auto status = _database->Get(readOptions, storeColumnFamily, it->value(), &value);
                if (status.ok()) {
                    writer.WriteJson(",");
                    writer.WriteJson(value.data(), (int) value.size());
                    written++;
                } else {
                    // do something...
                    throw StoreException("Error expected entity not found in dataset");
                }
            } else {
                if (shard == itemSequence % 4) {
                    lastWrittenSequence = itemSequence;
                    writer.WriteJson(",");
                    PinnableSlice value;
                    auto status = _database->Get(readOptions, storeColumnFamily, it->value(), &value);
                    if (status.ok()) {
                        writer.WriteJson(value.data(), (int) value.size());
                        written++;
                    } else {
                        // do something...
                    }
                }
            }

            // thats quite enough for now
            if (count > 0 && written == count) { break; }
        }
        delete it;

        // write continuation token
        auto lastId = to_string(lastWrittenSequence);
        if (!lastId.empty()) {
            // shard, sequence, generation
            auto token = to_string(shard) + "_" + lastId + "_1";
            string tokenTemplate(", { \"@id\" : \"@continuation\" , \"wod:next-data\" : \"" + token + "\" }");
            writer.WriteJson(tokenTemplate); 
        }

        writer.WriteJson("]");
    }

    /* void Store::RegisterPipe(string dataset, shared_ptr<Pipe> livepipe) {
        auto ds = GetDataSet(dataset);
        ds->RegisterLivePipe(livepipe);
    } */

}


