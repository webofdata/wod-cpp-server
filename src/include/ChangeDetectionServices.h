//
// Experimental
//

#ifndef WEBOFDATA_CHANGEDECTIONSERVICES_H
#define WEBOFDATA_CHANGEDECTIONSERVICES_H

#include <stream.h>
#include <bosma/Scheduler.h>
#include <mutex>

// interface for change detection
class ChangeDetectionService {
public:
    virtual ~ChangeDetectionServices();
    virtual void BeginSync(bool full) = 0;
    virtual void CompleteSync() = 0;
    virtual void ProcessEntity(string json) = 0;
    virtual void WriteChangesSinceToStream(string sinceToken, ostream& stream) = 0;
};


class InMemoryChangeDetectionService : ChangeDetectionService {

public:
    ~InMemoryChangeDetectionService();
    void BeginSync(bool full) {

    }

    void CompleteSync() {

    }

    void ProcessEntity(string json) {

    }

    void WriteChangesSinceToStream(string sinceToken, ostream& stream) {

    }
};


class RocksDbChangeDetectionService : ChangeDetectionService {
private:

public:
    ~RocksDbChangeDetectionServices() {}
    RocksDbChangeDetectionServices() {}

    void BeginSync(bool full) {
        if (full) {
            // clear some state
        }
    }

    void CompleteSync() {
        // iterate over the existing keys and see if they are in the seen keys
        // mark the entity as deleted if its not in the seen keys
    }

    void ProcessEntity(string json) {
        // add key to seen
        // compare with current version
    }

    void WriteChangesSinceToStream(string sinceToken, ostream& stream) {
    }
};


class DataSourceServer {
private:
    vector<shared_ptr<DataSource>> _datasources;
    Bosma::Scheduler scheduler();
    map<string,shared_ptr<ChangeDetectionService>> _changeDetectionServices;

public:
    DataSourceServer() {
    }

    ~DataSourceServer();

    void Configure(string config) {
        // create a datasource for each config item
    }

    void Start() {

    }
};




class DataSource {
public:
    ~DataSource();
    virtual string Id() = 0;
    virtual void WriteToStream(string token, ostream& stream) = 0;
};

class PostgreSqlDataSource : DataSource {
private:
    shared_ptr<ChangeDetectionServices> _changeDetectionService;
    string schedule;
    string query;
    string table;
    vector<string> whitelist;
    vector<string> blacklist;
    string _type;
    string _propertyPrefix;
    map<string, string> _fieldValueNamespacePrefixes;
    map<string, string> _prefixDefinitions;

public:
    string Id() { return ""; }
    void WriteToStream(string token, ostream& stream) = 0;
    void DoFullSync();
    void DoIncrementalSync();
};

// read service and write services
// a server supports some or both. a sink service supports write / a hub service supports both.



#endif //WEBOFDATA_CHANGEDECTIONSERVICES_H
