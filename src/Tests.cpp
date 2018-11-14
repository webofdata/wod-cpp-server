#include <iostream>

#include <Server.h>

#include <Store.h>
#include "EntityHandler.h"
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <ctime>
#include <chrono>
#include <thread>
#include <dlfcn.h>

extern "C" {
    #include "xxhash.h"
}

using namespace std;
using namespace webofdata;

string MakeGuid() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    auto strguid = boost::lexical_cast<string>(uuid);
    return strguid;
}

int testCreateStore() {
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);

    auto md = s->GetMetadataEntity();
    assert(md == "");

    s->Delete();
    return 1;
}

int testAssertDataSet() {
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    assert(ds != nullptr);

    // get metadata about dataset
    auto md = s->GetDatasetMetadataEntity("people");
    assert(md == "{ \"id\" : \"wod:people\"}");

    s->Delete();
    return 1;
}

int testCreateDeleteDataset() {
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);

    auto ds = s->AssertDataSet("people");
    assert(ds != nullptr);
    auto dsets = s->GetDataSets();

    assert(dsets.size() == 1);

    s->DeleteDataSet("people");

    dsets = s->GetDataSets();
    assert(dsets.size() == 0);

    ds = s->AssertDataSet("people");
    assert(ds != nullptr);
    dsets = s->GetDataSets();

    assert(dsets.size() == 1);

    s->Delete();
    return 1;
}

int testDeleteDatasetWithData() {
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    s->StoreEntity( "people", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" }}, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<colin>\" } ]"));
    assert(ds != nullptr);

    auto rid = s->GetResourceId("http://things.myspace.com/gra");
    assert(rid == "ns2:gra");

    auto json = s->GetEntity(rid, vector<string>{"people"});

    s->DeleteDataSet("people");
    ds = s->AssertDataSet("people");

    json = s->GetEntity(rid, vector<string>{"people"});
    assert(*json == "{ \"@id\" : \"ns2:gra\" }");

    return 1;
}

int testAssertNamespace() {
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    Store s(storeName, storeLoc);
    auto nsid1 = s.AssertNamespace("http://example.org/people1");
    auto nsid2 = s.AssertNamespace("http://example.org/people2");
    auto nsid3 = s.AssertNamespace("http://example.org/people2");

    assert(nsid1==1);
    assert(nsid2==2);
    assert(nsid2==nsid3);

    s.Delete();
    return 1;
}

int testAssertedNamespacesReloaded() {
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    Store s(storeName, storeLoc);
    auto nsid1 = s.AssertNamespace("http://example.org/people1");
    auto nsid2 = s.AssertNamespace("http://example.org/people2");
    auto nsid3 = s.AssertNamespace("http://example.org/people3");

    assert(nsid1==1);
    assert(nsid2==2);
    assert(nsid3==3);

    s.Close();

    Store s1(storeName, storeLoc);
    auto nsid4 = s1.AssertNamespace("http://example.org/people3");
    auto nsid5 = s1.AssertNamespace("http://example.org/people4");

    assert(nsid4 == nsid3);
    assert(nsid5 == 4);

    return 1;
}


int testInsertEntity() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    s->StoreEntity("people", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" }}, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<colin>\" } ]"));
    assert(ds != nullptr);

    auto rid = s->GetResourceId("http://things.myspace.com/gra");
    assert(rid == "ns2:gra");

    auto json = s->GetEntity(rid, vector<string>{"people"});

    cout << json << endl;
    cout.flush();

    assert(json->length() > 0);

    s->Delete();
    return 1;
}

int testInsertEntityNoContext() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    s->StoreEntity( "people", make_shared<string>("[ { \"@id\" : \"gra\" , \"name\" : \"graham moore\", \"friend\" : \"<colin>\" } ]"));
    assert(ds != nullptr);

    auto rid = s->GetResourceId("http://test.webofdata.io/things/gra");
    assert(rid == "ns1:gra");

    auto json = s->GetEntity(rid, vector<string>{"people"});

    cout << json << endl;
    cout.flush();

    assert(json->length() > 0);

    s->Delete();
    return 1;
}

int testTrySaveBadJson() {
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    try {
        s->StoreEntity( "people", make_shared<string>("[ { \"@id : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" }}, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<colin>\" } ]"));
    } catch (const StoreException &ex) {
        cout << ex.what();
    }
    return 1;
}

int testGetEntity() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<colin>\" } ]"));
    assert(ds != nullptr);

    auto rid = s->GetResourceId("http://things.myspace.com/gra");
    assert(rid == "ns4:gra");

    auto json = s->GetEntity(rid, vector<string>{"people"});

    cout << json << endl;
    cout.flush();

    assert(json->length() > 0);

    s->Delete();
    return 1;
}

int testGetEntitiesSince() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"colin\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));

    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));

    auto since = s->GetChanges("people", 0, 5);
    assert(since->size() == 3);

    since = s->GetChanges("people", 0, 1);
    assert(since->size() == 1);

    since = s->GetChanges("people", 3, 1);
    assert(since->size() == 1);

    since = s->GetChanges("people", 4, 1);
    assert(since->empty());

    s->Delete();
    return 1;
}

int testReplaceEntity() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    auto ds = s->AssertDataSet("people");
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<colin>\" } ]"));
    assert(ds != nullptr);

    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham david moore\", \"foaf:friend\" : \"<colin>\" } ]"));

    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham david moore\", \"foaf:friend\" : \"<james>\" } ]"));

    s->Delete();
    return 1;
}

int TestStoreManagerCreateStore() {
    auto storeName = MakeGuid();
    auto sm = StoreManager("/tmp/stores");
    auto store = sm.CreateStore(storeName);
    assert(store != nullptr);
    return 1;
}

int TestStoreManagerDeleteStore() {
    auto storeName = MakeGuid();
    auto sm = StoreManager("/tmp/stores");
    auto store = sm.CreateStore(storeName);
    assert(store != nullptr);
    sm.DeleteStore(storeName);
    auto closedStore = sm.GetStore(storeName);
    assert(closedStore == nullptr);
    return 1;
}

int TestStoreManagerOpenStore() {
    auto storeName = MakeGuid();
    auto sm = make_shared<StoreManager>("/tmp/stores");
    auto store = sm->CreateStore(storeName);
    assert(store != nullptr);

    sm->Close();

    sm = make_shared<StoreManager>("/tmp/stores");

    store = sm->GetStore(storeName);
    assert(store != nullptr);
    return 1;
}

void bulkLoad(shared_ptr<Store> store, string dataset, string locationBase) {
    auto start = std::chrono::steady_clock::now();
    cout << "assert dataset " << endl;
    auto ds = store->AssertDataSet(dataset);
    char Buffer[4096];
    std::ifstream ifs;
    ifs.rdbuf()->pubsetbuf(Buffer, 4096);
    string fileLoc = locationBase + string("/data/sample1M.json");
    ifs.open(fileLoc, std::ifstream::in);
    cout << "store entities " << endl;
    store->StoreEntities(ifs, dataset);
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
}

int testBulkImport2(string locationBase) {

    cout << "starting bulk import 2" << endl;

    auto name = MakeGuid();
    auto storeLoc = locationBase + string("/stores/store_") + name;

    cout << "store name is " << name << endl;

    boost::filesystem::create_directory(storeLoc.c_str());

    cout << "create new store " << endl;

    auto s = shared_ptr<Store>(new Store(name, storeLoc));

    cout << "open rocksdb " << endl;

    s->OpenRocksDb(storeLoc);

    cout << "start bulk load " << endl;

    std::thread first(bulkLoad, s, "people", locationBase);


    std::thread second(bulkLoad, s, "companies", locationBase);
    std::thread third(bulkLoad, s, "products", locationBase);
    std::thread fourth(bulkLoad, s, "orders", locationBase);

    std::thread first1(bulkLoad, s, "people1", locationBase);
    std::thread second1(bulkLoad, s, "companies1", locationBase);
    std::thread third1(bulkLoad, s, "products1", locationBase);
    std::thread fourth1(bulkLoad, s, "orders1", locationBase);

    std::thread first2(bulkLoad, s, "people2", locationBase);
    std::thread second2(bulkLoad, s, "companies2", locationBase);
    std::thread third2(bulkLoad, s, "products2", locationBase);
    std::thread fourth2(bulkLoad, s, "orders2", locationBase);

    std::thread first3(bulkLoad, s, "people3", locationBase);
    std::thread second3(bulkLoad, s, "companies3", locationBase);
    std::thread third3(bulkLoad, s, "products3", locationBase);
    std::thread fourth3(bulkLoad, s, "orders3", locationBase);

    std::thread first4(bulkLoad, s, "people4", locationBase);
    std::thread second4(bulkLoad, s, "companies4", locationBase);
    std::thread third4(bulkLoad, s, "products4", locationBase);
    std::thread fourth4(bulkLoad, s, "orders4", locationBase);

    auto start = std::chrono::steady_clock::now();

    first.join();
    second.join();
    third.join();
    fourth.join();

    first1.join();
    second1.join();
    third1.join();
    fourth1.join();

    first2.join();
    second2.join();
    third2.join();
    fourth2.join();

    first3.join();
    second3.join();
    third3.join();
    fourth3.join();

    first4.join();
    second4.join();
    third4.join();
    fourth4.join();

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Load 20*1m objects " << elapsed.count() << " milliseconds." << std::endl;

    s->Compact();
    return 0;
}


int testBulkImport() {

    auto name = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + name;

    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = shared_ptr<Store>(new Store(name, storeLoc));
    s->OpenRocksDb(storeLoc);

    auto start = std::chrono::steady_clock::now();

    bulkLoad(s, "people", "/tmp");
    bulkLoad(s, "people", "/tmp");
    bulkLoad(s, "people", "/tmp");

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Load 1m objects " << elapsed.count() << " milliseconds." << std::endl;

    s->Delete();
    return 1;
}


int testWriteDatasetEntities() {

    auto name = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + name;

    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(name, storeLoc);
    s->OpenRocksDb(storeLoc);

    auto start = std::chrono::steady_clock::now();

    bulkLoad(s, "people", "/tmp");

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Load 1m objects " << elapsed.count() << " milliseconds." << std::endl;

    start = std::chrono::steady_clock::now();

    string fname = string("/tmp/outdata/") + name;
    FileStreamWriter fsw(fname);

    s->WriteEntitiesToStream("people", "", -1, -1, fsw);

    end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Write Out 1m objects " << elapsed.count() << " milliseconds." << std::endl;

    // s->Delete();
    return 1;
}

int testWriteDatasetShardedEntities() {

    auto name = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + name;

    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = shared_ptr<Store>(new Store(name, storeLoc));

    auto start = std::chrono::steady_clock::now();

    bulkLoad(s, "people", "/tmp");

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Load 1m objects " << elapsed.count() << " milliseconds." << std::endl;

    start = std::chrono::steady_clock::now();

    string fname1 = string("/tmp/outdata/") + name + "1";
    string fname2 = string("/tmp/outdata/") + name + "2";
    string fname3 = string("/tmp/outdata/") + name + "3";
    string fname4 = string("/tmp/outdata/") + name + "4";

    FileStreamWriter fsw1(fname1);
    FileStreamWriter fsw2(fname2);
    FileStreamWriter fsw3(fname3);
    FileStreamWriter fsw4(fname4);

    s->WriteEntitiesToStream("people", "ns1:obj100", -1, 0, fsw1);
//    auto lastId2 = s->WriteEntitiesToStream("people", "", -1, 1, fsw2);
//    auto lastId3 = s->WriteEntitiesToStream("people", "", -1, 2, fsw3);
//    auto lastId4 = s->WriteEntitiesToStream("people", "", -1, 3, fsw4);

    end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Write Out 1m objects " << elapsed.count() << " milliseconds." << std::endl;

    s->Delete();
    return 1;
}


int testWriteDatasetShardedChanges() {

    auto name = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + name;

    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = shared_ptr<Store>(new Store(name, storeLoc));

    auto start = std::chrono::steady_clock::now();

    bulkLoad(s, "people", "/tmp");

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Load 1m objects " << elapsed.count() << " milliseconds." << std::endl;

    start = std::chrono::steady_clock::now();

    string fname1 = string("/tmp/outdata/") + name + "1";
    string fname2 = string("/tmp/outdata/") + name + "2";
    string fname3 = string("/tmp/outdata/") + name + "3";
    string fname4 = string("/tmp/outdata/") + name + "4";

    FileStreamWriter fsw1(fname1);
    FileStreamWriter fsw2(fname2);
    FileStreamWriter fsw3(fname3);
    FileStreamWriter fsw4(fname4);

    s->WriteChangesToStream("people", -1, 10, 0, fsw1);
    s->WriteChangesToStream("people", -1, 10, 1, fsw2);
    s->WriteChangesToStream("people", -1, 10, 2, fsw3);
    s->WriteChangesToStream("people", -1, 10, 3, fsw4);

    end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Write Out 1m objects " << elapsed.count() << " milliseconds." << std::endl;

    s->Delete();
    return 1;
}



int TestSeqIdKeyPacking() {

    ulong logSeqId = 1;
    int dsId = 200;
    ulong id = 63373;

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto sizeLogBuffer = sizeof(logSeqId) + sizeof(dsId) + sizeof(id) + sizeof(ms);
    shared_ptr<char> seqbuffer(new char[sizeLogBuffer], ArrayDeleter<char>());

    memcpy(seqbuffer.get(), (char *) &logSeqId, sizeof(logSeqId));
    memcpy(seqbuffer.get() + sizeof(logSeqId), (char *) &dsId, sizeof(dsId));
    memcpy(seqbuffer.get() + sizeof(logSeqId) + sizeof(dsId), (char *) &id, sizeof(id));
    memcpy(seqbuffer.get() + sizeof(logSeqId) + sizeof(dsId) + sizeof(id), (char *) &ms, sizeof(ms));

    ulong logSeqIdOut = 0;
    int dsIdOut = 200;
    ulong idOut = 63373;
    long long msOut = 0;

    memcpy((char *) &logSeqIdOut, seqbuffer.get(), sizeof(logSeqIdOut));
    memcpy((char *) &dsIdOut, seqbuffer.get() + sizeof(logSeqIdOut), sizeof(dsIdOut));
    memcpy((char *) &idOut, seqbuffer.get() + sizeof(logSeqIdOut) + sizeof(dsIdOut), sizeof(idOut));
    memcpy((char *) &msOut, seqbuffer.get() + sizeof(logSeqIdOut) + sizeof(dsIdOut) + sizeof(idOut), sizeof(msOut));

    assert(logSeqId == logSeqIdOut);
    assert(dsId == dsIdOut);
    assert(id == idOut);
    assert(ms == msOut);

    return 1;
}

int testWriteEntitiesSmall() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    s->OpenRocksDb(storeLoc);

    auto ds = s->AssertDataSet("people");
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"@context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"@context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"@context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"colin\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));

    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"@context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"id\" : \"@context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"));

    auto rid = s->GetResourceId("http://things.myspace.com/gra");
    assert(rid == "ns4:gra");
    auto json = s->GetEntity(rid, vector<string>{"people"});

    string fname = string("/tmp/outdata/") + storeName;
    FileStreamWriter fsw(fname);

    s->WriteEntitiesToStream("people", 0, -1, -1, fsw);

    s->Delete();
    return 1;
}


int testGraphNavigation() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    s->OpenRocksDb(storeLoc);
    auto ds = s->AssertDataSet("people");
    s->StoreEntity( "people", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" } }, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" } }, { \"@id\" : \"james\" , \"foaf:name\" : \"colin\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" } }, { \"@id\" : \"colin\" , \"foaf:name\" : \"james\", \"foaf:friend\" : \"<bob>\" } ]"));

//    s->StoreEntity(string("[ { \"@id\" : \"@context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"), "people2");
//    s->StoreEntity(string("[ { \"@id\" : \"@context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]"), "people2");

    auto rid = s->GetResourceId("http://things.myspace.com/gra");
    // assert(rid == "ns4:gra");
    auto json = s->GetEntity(rid, vector<string>{"people"});

    vector<string> datasets;
    datasets.push_back("people");

    auto relatedEntities = s->GetRelatedEntities("http://things.myspace.com/bob",
                                                 "",
                                                 true,
                                                 -1,
                                                 datasets);
    assert(!relatedEntities->empty());
    assert(relatedEntities->size() == 3);

    s->Delete();
    return 1;
}

int testMerging() {

    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    s->OpenRocksDb(storeLoc);

    auto ds1 = s->AssertDataSet("staff");
    auto ds2 = s->AssertDataSet("customers");
    auto ds3 = s->AssertDataSet("people");

    s->StoreEntity( "staff", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" }}, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\", \"city\" : \"London\" } ]"));
    s->StoreEntity( "customers", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" }}, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\", \"age\" : 23 } ]"));
    s->StoreEntity( "people", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" }}, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\", \"city\" : \"Oslo\" } ]"));

    auto rid = s->GetResourceId("http://things.myspace.com/gra");
    vector<string> datasets;
    auto json = s->GetEntity(rid, datasets);

    // datasets.push_back("people");

    s->Delete();
    return 1;
}


void testXxHash() {
    std::cout << "test hash";
    auto val = string("ns1:object24");
    unsigned long long const seed = 0;   /* or any other value */
    unsigned long long const hash = XXH64(val.data(), val.length(), seed);
    std::cout << "hash " << hash << std::endl;
    auto rem = hash % 4;
    std::cout << "rem " << rem << std::endl;
}

void testDocumentCompare() {

    string data("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:name\" : \"graham moore\", \"foaf:friend\" : \"<bob>\" } ]");

    string data1("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:friend\" : \"<bob>\"     ,  \"foaf:name\" : \"graham moore\"} ]");

    string data2("[ { \"id\" : \"__context\" , \"_\" : \"http://schema.myspace.com/\",  \"id_base\" : \"http://things.myspace.com/\" , \"foaf\" : \"http://foaf.com/\" }, { \"id\" : \"james\" , \"foaf:name\" : \"james shadwell\", \"foaf:friend\" : \"<bob>\" } ]");

    Document currentData;
    currentData.ParseInsitu((char *) data.data());

    Document newData;
    newData.ParseInsitu((char *) data1.data());

    Document moreNewData;
    moreNewData.ParseInsitu((char *) data2.data());

    bool same = (newData == currentData);
    std::cout << same;

    same = (moreNewData == currentData);
    std::cout << same;

}

int testLivePipe() {

    cout << "start test live pipe \n";
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    s->OpenRocksDb(storeLoc);

    cout << "assert datasets \n";

    auto raw = s->AssertDataSet("source");
    auto refined = s->AssertDataSet("target");

    cout << "create logic \n";

    auto logic = make_shared<IdentityTransformPipeLogic>(s, "target");

    cout << "create pipe \n";

    auto p1 = make_shared<Pipe>(s, raw, 0, logic);

    cout << "store entities \n";

    // stores some entities
    s->StoreEntity( "source", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" } }, { \"@id\" : \"gra\" , \"foaf:name\" : \"graham\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "source", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" } }, { \"@id\" : \"james\" , \"foaf:name\" : \"colin\", \"foaf:friend\" : \"<bob>\" } ]"));
    s->StoreEntity( "source", make_shared<string>("[ { \"@id\" : \"@context\" , \"namespaces\" : { \"_\" : \"http://things.myspace.com/\", \"foaf\" : \"http://foaf.com/\" } }, { \"@id\" : \"colin\" , \"foaf:name\" : \"james\", \"foaf:friend\" : \"<bob>\" } ]"));

    cout << "run pipe \n";

    // fire the pipe
    p1->RunOnce();

    cout << "check dataset \n";

    auto result = make_shared<vector<shared_ptr<string>>>();
    auto lastId = s->GetEntities("target", "", -1, -1, result);
    assert(result->size() == 3);
}

int testDynaLoad() {
    cout << "start test live pipe \n";
    auto storeName = MakeGuid();
    auto storeLoc = string("/tmp/stores/store_") + storeName;
    boost::filesystem::create_directory(storeLoc.c_str());
    auto s = make_shared<Store>(storeName, storeLoc);
    s->OpenRocksDb(storeLoc);

    void* dynalib = dlopen("/home/wodsource/libdynalib.so",  RTLD_LAZY | RTLD_GLOBAL);
    if (!dynalib) {
        fprintf(stderr, "Could not open libdynalib.so\n");
        exit(1);
    }
    cout << "loaded lib\n";

    webofdata::PipeLogic*  (*fptr_createLogic) ();
    fptr_createLogic = (webofdata::PipeLogic*  (*) ()) dlsym(dynalib, "CreateLogic");

    auto logic = fptr_createLogic();
    auto d = make_shared<string>("data");

    logic->ProcessEntity(d, s);

    cout << "loaded and executed";
    dlclose(dynalib);
}

int main(int argc, char* argv[]) {

    std::cout << "testing...";
    auto baseLocation = argv[1];
    std::cout << baseLocation;

    // testDynaLoad();
    // testLivePipe();
    // testGraphNavigation(); 
    // testMerging();
    // string baseLocation(argv[1]);
    // testLanguage(); 
    // testWriteDatasetShardedChanges();
    // testDocumentCompare();
    // testCreateStore();
    // testAssertDataSet();
    // testDeleteDatasetWithData();
    // testCreateDeleteDataset();
    // testAssertNamespace();
    // testInsertEntity();
    // testInsertEntityNoContext();
    // testTrySaveBadJson();
    // testReplaceEntity();
    // TestStoreManagerCreateStore();
    // TestStoreManagerDeleteStore();
    // TestStoreManagerOpenStore();
    // TestSeqIdKeyPacking();
    // testGetEntitiesSince();
    // testAssertedNamespacesReloaded();
    // testGetEntity();
    // testGraphNavigation();
    // testWriteEntitiesSmall();
    // testBulkImport();
    testWriteDatasetEntities();
    // testBulkImport2(baseLocation);
    // testWriteDatasetEntities();
    std::cout << "Tests passed";

    return 0;
}





