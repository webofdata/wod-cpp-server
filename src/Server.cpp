//
// Created by Graham Moore on 23/07/17.
//

#include "Server.h"
#include <simple-web-server/server_http.hpp>
#include <simple-web-server/status_code.hpp>
#include <EntityStreamWriter.h>
#include <rapidjson/document.h>
#include "rapidjson/error/en.h"
#include <algorithm>
#include <fstream>
#include <vector>
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>


namespace webofdata {

    using namespace std;
    using namespace rapidjson;
    using namespace SimpleWeb;

    typedef SimpleWeb::Server<SimpleWeb::HTTP> HttpServer;

    template<typename Out>
    void split(const std::string &s, char delim, Out result) {
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, delim)) {
            *(result++) = item;
        }
    }

    string WodServer::MakeGuid() {
        boost::uuids::uuid uuid = boost::uuids::random_generator()();
        auto strguid = boost::lexical_cast<string>(uuid);
        return strguid;
    }

    string WodServer::GetRequestId(shared_ptr<HttpServer::Request> request) {
        string requestId = "";
        for (auto it = request->header.find("X-Request-ID"); it != request->header.end(); it++) {
            requestId = it->second;
        }

        if (requestId.empty()) {
            requestId = MakeGuid();
        }

        return requestId;
    }

    std::vector<std::string> splitstr(const std::string &s, char delim) {
        std::vector<std::string> elems;
        split(s, delim, std::back_inserter(elems));
        return elems;
    }

    void WodServer::ConfigureRoutes() {

        // get service info
        _server.resource["^/info"]["GET"] = [this](shared_ptr<HttpServer::Response> response,
                                                   shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-node" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;
            try {
                Document d;
                d.SetObject();
                d.AddMember("endpoint", Value(StringRef(_serviceId.data())), d.GetAllocator());
                Value obj(kObjectType);
                obj.AddMember("@id", Value(StringRef(_subjectIdentifier.data())), d.GetAllocator());
                d.AddMember("entity", obj, d.GetAllocator());
                StringBuffer buffer;
                Writer<StringBuffer> writer(buffer);
                d.Accept(writer);
                response->write(StatusCode::success_ok, buffer.GetString());
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-node-info", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // get-stores
        _server.resource["^/stores"]["GET"] = [this](shared_ptr<HttpServer::Response> response,
                                                     shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-stores" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                auto stores = _storeManager->GetStores();
                Document d;
                d.SetArray();
                for (auto const &store : stores) {
                    Value obj(kObjectType);
                    auto name = store->GetName();
                    obj.AddMember("name", Value().SetString(name.data(), (int) name.length(), d.GetAllocator()), d.GetAllocator());

                    // get store metadata
                    auto storeEntity = store->GetMetadataEntity();
                    Document ed(&d.GetAllocator());
                    ed.Parse(storeEntity.data(), storeEntity.length());
                    obj.AddMember("entity", ed.GetObject(), d.GetAllocator());
                    d.PushBack(obj, d.GetAllocator());
                }

                StringBuffer buffer;
                Writer<StringBuffer> writer(buffer);
                d.Accept(writer);
                response->write(StatusCode::success_ok, buffer.GetString());
                _logger->info(
                        R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-stores", "success" : "stores" }})",
                        _serviceId, requestId);
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-stores", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-stores", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // create-store
        _server.resource["^/stores"]["POST"] = [this](shared_ptr<HttpServer::Response> response,
                                                      shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-store" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;
            try {
                auto storeJson = request->content.string();
                Document d;
                d.ParseInsitu((char *) storeJson.data());
                if (d.HasParseError()) {
                    _logger->info(
                            R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-store", "error" : "unable to parse json or json missing" }})",
                            _serviceId, requestId);
                    response->write(StatusCode::client_error_bad_request, headers);
                    return;
                }

                if (!d.HasMember("name")) {
                    // return bad request
                    _logger->info(
                            R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-store", "error" : "missing name value in json" }})",
                            _serviceId, requestId);
                    response->write(StatusCode::client_error_bad_request, headers);
                } else {
                    auto storeName = string(d["name"].GetString());

                    // check if exists
                    if (_storeManager->GetStore(storeName) != nullptr) {
                        _logger->info(
                                R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-store", "error" : "store with this name already exists" }})",
                                _serviceId, requestId);
                        response->write(StatusCode::client_error_bad_request, "store name already exists", headers);
                    } else {
                        auto store = _storeManager->CreateStore(storeName);
                        auto location = string("/stores/") + storeName;

                        if (d.HasMember("entity")) {
                            StringBuffer buffer;
                            Writer<StringBuffer> writer(buffer);
                            d["entity"].Accept(writer);
                            string storeEntity(buffer.GetString(), buffer.GetLength());
                            store->StoreMetadataEntity(storeEntity);
                        }

                        // return store json
                        Document d;
                        d.SetObject();
                        d.AddMember("name", Value(StringRef(storeName.data())), d.GetAllocator());

                        auto storeEntity = store->GetMetadataEntity();
                        Document ed;
                        ed.ParseInsitu((char *) storeEntity.data());
                        d.AddMember("entity", ed, ed.GetAllocator());

                        StringBuffer buffer;
                        Writer<StringBuffer> writer(buffer);
                        d.Accept(writer);

                        headers.emplace("Location", location.data());
                        response->write(StatusCode::success_created, buffer.GetString() , headers);
                        _logger->info(
                                R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-store", "success" : "store created", "storename" : "{}" }})",
                                _serviceId, requestId, storeName);
                    }
                }
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-store", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-store", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // delete-store
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)$"]["DELETE"] = [this](shared_ptr<HttpServer::Response> response,
                                                                            shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-store" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                _storeManager->DeleteStore(storeName);
                response->write(StatusCode::success_ok);
                _logger->info(
                        R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-store", "success" : "store deleted", "storename" : "{}" }})",
                        _serviceId, requestId, storeName);
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-store", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-store", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // update-store-entity
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)$"]["PUT"] = [this](shared_ptr<HttpServer::Response> response,
                                                                         shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "update-store-entity" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;
            try {
                string storeName = request->path_match[1];
                auto storeEntityJson = request->content.string();

                _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "update-store-entity" }})", _serviceId, requestId);
                auto store = _storeManager->GetStore(storeName);
                if (store) {

                    // todo: parse this before storing it.
                    store->StoreMetadataEntity(storeEntityJson);

                    response->write(StatusCode::success_ok);
                    _logger->info(
                            R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "update-store-entity", "success" : "store entity updated" }})",
                            _serviceId, requestId, storeName);

                } else {
                    response->write(StatusCode::client_error_not_found);
                }
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "update-store-entity", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "update-store-entity", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // get-store
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)$"]["GET"] = [this](shared_ptr<HttpServer::Response> response,
                                                                         shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-store" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;
            try {
                string storeName = request->path_match[1];
                _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-store" }})", _serviceId, requestId);
                auto store = _storeManager->GetStore(storeName);
                if (store) {
                    Document d;
                    d.SetObject();
                    d.AddMember("name", Value(StringRef(storeName.data())), d.GetAllocator());

                    auto storeEntity = store->GetMetadataEntity();
                    Document ed;
                    ed.ParseInsitu((char *) storeEntity.data());
                    d.AddMember("entity", ed, ed.GetAllocator());

                    StringBuffer buffer;
                    Writer<StringBuffer> writer(buffer);
                    d.Accept(writer);
                    response->write(StatusCode::success_ok, buffer.GetString());
                    _logger->info(
                            R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-store", "success" : "store info returned", "storename" : "{}" }})",
                            _serviceId, requestId, storeName);

                } else {
                    response->write(StatusCode::client_error_not_found);
                }
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-store", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-store", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // get-datasets
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets$"]["GET"] = [this](
                shared_ptr<HttpServer::Response> response,
                shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-datasets" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                auto store = _storeManager->GetStore(storeName);

                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                } else {
                    Document d;
                    d.SetArray();
                    auto datasets = store->GetDataSets();
                    for (auto const &dataset : datasets) {
                        Value obj(kObjectType);
                        auto name = dataset->GetName();
                        obj.AddMember("name", Value().SetString(name.data(), (int) name.length(), d.GetAllocator()), d.GetAllocator());

                        // add metadata entity
                        auto datasetEntity = store->GetDatasetMetadataEntity(dataset->GetName());

                        Document ed(&d.GetAllocator());
                        ed.Parse(datasetEntity.data(), datasetEntity.length());
                        obj.AddMember("entity", ed.GetObject(), d.GetAllocator());

                        // add to array
                        d.PushBack(obj, d.GetAllocator());
                    }

                    StringBuffer buffer;
                    Writer<StringBuffer> writer(buffer);
                    d.Accept(writer);
                    response->write(StatusCode::success_ok, buffer.GetString());
                }
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-datasets", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-datasets", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // create-dataset
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets$"]["POST"] = [this](
                shared_ptr<HttpServer::Response> response,
                shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-dataset" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                auto datasetJson = request->content.string();

                // Retrieve JSON from body:
                Document d;
                d.ParseInsitu((char *) datasetJson.data());
                if (d.HasParseError()) {
                    response->write(StatusCode::client_error_bad_request, "JSON body parsing error", headers);
                    return;
                }

                if (!d.HasMember("name")) {
                    // return bad request
                    response->write(StatusCode::client_error_bad_request, headers);
                } else {
                    auto datasetName = d["name"].GetString();
                    auto store = _storeManager->GetStore(storeName);
                    if (store == nullptr) {
                        // no store by this name
                        response->write(StatusCode::client_error_bad_request, "store does not exist", headers);
                        return;
                    }

                    // check if dataset already exists
                    auto dataset = store->GetDataSet(datasetName);
                    if (dataset != nullptr) {
                        // no dataset by this name
                        response->write(StatusCode::client_error_bad_request, "dataset name already exists", headers);
                    } else {
                        dataset = store->AssertDataSet(datasetName);

                        if (d.HasMember("entity")) {
                            StringBuffer buffer;
                            Writer<StringBuffer> writer(buffer);
                            d["entity"].Accept(writer);
                            string datasetEntity(buffer.GetString(), buffer.GetLength());
                            store->StoreDatasetMetadataEntity(datasetName, datasetEntity);
                        }

                        Document d;
                        d.SetObject();
                        d.AddMember("name", Value(StringRef(datasetName)), d.GetAllocator());
                        auto datasetEntity = store->GetDatasetMetadataEntity(datasetName);

                        if (!datasetEntity.empty()) {
                            Document ed;
                            ed.ParseInsitu((char *) datasetEntity.data());
                            d.AddMember("entity", ed, ed.GetAllocator());
                        }

                        // make json response
                        StringBuffer buffer;
                        Writer<StringBuffer> writer(buffer);
                        d.Accept(writer);

                        auto location = string("/stores/") + storeName + string("/datasets/") + datasetName;
                        headers.emplace("Location", location.data());
                        response->write(StatusCode::success_created, buffer.GetString(), headers);
                    }
                }
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-dataset", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-dataset", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // update-dataset
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)$"]["PUT"] = [this](
                shared_ptr<HttpServer::Response> response,
                shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "update-dataset" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];
                auto datasetJson = request->content.string();

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                store->StoreDatasetMetadataEntity(datasetName, datasetJson);
                response->write(StatusCode::success_ok);
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-dataset", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "create-dataset", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // get-dataset
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)$"]["GET"]
                = [this](shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request) {
            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-dataset" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];

                // get the actual dataset entity
                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto dataset = store->GetDataSet(datasetName);
                if (dataset == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                Document d;
                d.SetObject();
                d.AddMember("name", Value(StringRef(datasetName.data())), d.GetAllocator());
                auto datasetEntity = store->GetDatasetMetadataEntity(datasetName);

                if (!datasetEntity.empty()) {
                    Document ed;
                    ed.ParseInsitu((char *) datasetEntity.data());
                    d.AddMember("entity", ed, ed.GetAllocator());
                }

                // make json response
                StringBuffer buffer;
                Writer<StringBuffer> writer(buffer);
                d.Accept(writer);
                response->write(StatusCode::success_ok, buffer.GetString());

            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-dataset", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-dataset", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // delete dataset
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)$"]["DELETE"] = [this](
                shared_ptr<HttpServer::Response> response,
                shared_ptr<HttpServer::Request> request) {

            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-dataset" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto dataset = store->GetDataSet(datasetName);
                if (dataset == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                store->DeleteDataSet(datasetName);
                response->write(StatusCode::success_ok);
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-dataset", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-dataset", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // stream entities from dataset
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)/entities$"]["GET"]
                = [this](shared_ptr<HttpServer::Response> response,
                         shared_ptr<HttpServer::Request> request) {

            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-entities" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto dataset = store->GetDataSet(datasetName);
                if (dataset == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                // check if there is an id query param
                auto queryParams = request->parse_query_string();
                auto idParam = queryParams.find("id");
                string entityId;
                if (idParam != queryParams.end()) {
                    entityId = string(idParam->second);
                }

                if (!entityId.empty()) {
                    headers.emplace("Content-Type", "application/json");
                    auto rid = store->GetResourceId(entityId);

                    response->write(StatusCode::success_ok, headers);
                    HttpResponseStreamWriter writer(response);

                    vector<string> datasets;
                    datasets.push_back(datasetName);
                    store->WriteEntityToStream(rid, datasets, writer);

                    *response << "0\r\n" << "\r\n";
                    writer.Flush();
                    response->flush();

                } else {

                    int take = -1;
                    auto takeCountParam = queryParams.find("take");
                    if (takeCountParam != queryParams.end()) {
                        take = std::stoi(takeCountParam->second);
                    }

                    auto continuationToken = queryParams.find("token"); // check token

                    headers.emplace("Transfer-Encoding", "chunked");
                    headers.emplace("Content-Type", "application/json");

                    response->write(StatusCode::success_ok, headers);
                    HttpResponseStreamWriter writer(response);

                    int shard = -1;
                    string lastid(""); 

                    if (continuationToken != queryParams.end()) {
                        // split token and get last id and shard id 
                        // token lastid:shardid
                        auto token = continuationToken->second;
                        auto bits = splitstr(token, '_');
                        shard = stoi(bits[0]);
                        if (bits.size() == 2) {
                            lastid = bits[1];
                        }
                    }

                    // write out data
                    store->WriteEntitiesToStream(datasetName, lastid, take, shard, writer);

                    // TODO: move this into the writer
                    *response << "0\r\n" << "\r\n";

                    writer.Flush();
                    response->flush();
                }

            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-entities", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-entities", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // get changes partitions
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)/changes/partitions$"]["GET"]
                = [this](shared_ptr<HttpServer::Response> response,
                         shared_ptr<HttpServer::Request> request) {

            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes-partitions" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto ds = store->GetDataSet(datasetName);
                if (ds == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                // make document
                Document d;
                d.SetArray();

                auto tokens = store->GetChangesShardTokens(datasetName, 4);
                for (auto const& token : *tokens) {
                    Value v;
                    v.SetString(token.data(), (int) token.length(), d.GetAllocator());
                    d.PushBack(v, d.GetAllocator());
                }

                // serialise document and return it
                StringBuffer buffer;
                Writer<StringBuffer> writer(buffer);
                d.Accept(writer);
                response->write(StatusCode::success_ok, buffer.GetString());

            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }

            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-entities-partitions", "status" : "completed" }})", _serviceId, requestId);
        };

        // get entities partitions
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)/entities/partitions$"]["GET"]
                = [this](shared_ptr<HttpServer::Response> response,
                         shared_ptr<HttpServer::Request> request) {

            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-entities-partitions", "status" : "started"  }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto ds = store->GetDataSet(datasetName);
                if (ds == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                // make document
                Document d;
                d.SetArray();

                auto tokens = store->GetDataSetShardTokens(datasetName, 4);
                for (auto const& token : *tokens) {
                    Value v;
                    v.SetString(token.data(), (int) token.length(), d.GetAllocator());
                    d.PushBack(v, d.GetAllocator());
                }

                // serialise document and return it
                StringBuffer buffer;
                Writer<StringBuffer> writer(buffer);
                d.Accept(writer);
                response->write(StatusCode::success_ok, buffer.GetString());

            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }

            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-entities-partitions", "status" : "completed" }})", _serviceId, requestId);

        };

        // get dataset changes
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)/changes$"]["GET"]
                = [this](shared_ptr<HttpServer::Response> response,
                         shared_ptr<HttpServer::Request> request) {

            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto ds = store->GetDataSet(datasetName);
                if (ds == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                // get query params for paging / requested range
                auto queryParams = request->parse_query_string();

                int take = -1;
                auto takeCountParam = queryParams.find("take");
                if (takeCountParam != queryParams.end()) {
                    take = std::stoi(takeCountParam->second);
                }

                long sequence = -1;
                int gen = -1;
                int shard = -1;

                auto continuationToken = queryParams.find("token");
                if (continuationToken != queryParams.end())  {
                    // shard_sequence_gen:
                    auto nextDataTokens = splitstr(continuationToken->second, '_');
                    if (nextDataTokens.size() != 3) {
                        response->write(StatusCode::client_error_bad_request);
                        return; 
                    }
                    shard = stoi(nextDataTokens[0]);
                    sequence = stol(nextDataTokens[1]);
                    gen = stoi(nextDataTokens[2]);
                }

                ulong dataset_gen = -1; // todo: get dataset generation
                if (dataset_gen != gen) {
                    // do a full sync
                    // add header for full sync
                    headers.emplace("x-wod-full-sync", "true");
                    // reset sequence
                }

                headers.emplace("Transfer-Encoding", "chunked");
                headers.emplace("Content-Type", "application/json");
                headers.emplace("x-wod-full-sync", "false");

                response->write(StatusCode::success_ok, headers);

                HttpResponseStreamWriter writer(response);

                store->WriteChangesToStream(datasetName, sequence, take, shard, writer);

                *response << "0\r\n" << "\r\n";
                writer.Flush();
                response->flush();

            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "get-changes", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // set entities by posting to /entities
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)/entities$"]["POST"]
                = [this](shared_ptr<HttpServer::Response> response,
                         shared_ptr<HttpServer::Request> request) {

            auto requestId = GetRequestId(request);
            CaseInsensitiveMultimap headers;

            auto start = std::chrono::steady_clock::now();

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];
                _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "store" : "{}", "dataset" : "{}" , "op" : "set-entities", "status" : "started" }})", _serviceId, requestId, storeName, datasetName);

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto ds = store->GetDataSet(datasetName);
                if (ds == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto count = store->StoreEntities(request->content, datasetName);

                auto end = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

                _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "store" : "{}", "dataset" : "{}" , "op" : "set-entities", "status" : "completed", "duration" : {}, "count" : {} }})", _serviceId, requestId, storeName, datasetName, elapsed.count(), count);

                response->write(StatusCode::success_ok);
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "set-entities", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "set-entities", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }
        };

        // delete all entities
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/datasets/([a-zA-Z0-9 ._-]*)/entities$"]["DELETE"]
                = [this](shared_ptr<HttpServer::Response> response,
                         shared_ptr<HttpServer::Request> request) {

            auto requestId = GetRequestId(request);
            _logger->info(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-entities" }})", _serviceId, requestId);
            CaseInsensitiveMultimap headers;

            try {
                string storeName = request->path_match[1];
                string datasetName = request->path_match[2];

                auto store = _storeManager->GetStore(storeName);
                if (store == nullptr) {
                    // no store by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                auto ds = store->GetDataSet(datasetName);
                if (ds == nullptr) {
                    // no dataset by this name
                    response->write(StatusCode::client_error_not_found);
                    return;
                }

                // TODO: delete dataset entities

                auto content = string("{\"version\": \"1.0\",\"service\": \"") + storeName + ":" + datasetName +
                               string("\" }");
                *response << "HTTP/1.1 200 OK\r\nContent-Length: " << content.length() << "\r\n\r\n" << content;
            } catch (const StoreException &sex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-entities", "error" : "{}" }})",
                               _serviceId, requestId, sex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            } catch (const exception &ex) {
                _logger->error(R"({{ "nodeid" : "{}" , "rid" : "{}" , "op" : "delete-entities", "error" : "{}" }})",
                               _serviceId, requestId, ex.what());
                response->write(StatusCode::server_error_internal_server_error, headers);
            }

        };

        // Query endpoint for accessing subjects and traversing the graph
        _server.resource["^/stores/([a-zA-Z0-9 ._-]*)/query$"]["GET"]
                = [this](shared_ptr<HttpServer::Response> response,
                         shared_ptr<HttpServer::Request> request) 
        {

            auto requestId = GetRequestId(request);
            auto start = std::chrono::steady_clock::now();

            string storeName = request->path_match[1];
            auto store = _storeManager->GetStore(storeName);

            // get query params
            auto params = request->parse_query_string();

            string subject;           // params.find("subject");
            string connected;         // params.find("connected");
            string incoming;          // params.find("incoming");
            string pageSize;          // params.find("pagesize");
            vector<string> datasets;  // params.find("dataset");
            string nextdata;          // params.find("nextdata");
            bool inverse = false;

            // process params
            for(auto it = params.begin(); it != params.end(); it++) {
                cout << it->first << '\t';
                cout << it->second << endl;

                if (it->first == "subject") {
                    subject = it->second;
                }

                if (it->first == "connected") {
                    connected = it->second;                    
                }

                if (it->first == "incoming") {
                    incoming = it->second;
                    if (incoming == "true") {
                        inverse = true;
                    }
                }

                if (it->first == "pagesize") {
                    pageSize = it->second;
                }

                if (it->first == "dataset") {
                    datasets.push_back(it->second);
                }

                if (it->first == "nextdata") {
                    nextdata = it->second;
                }
            }

            CaseInsensitiveMultimap headers;

            if (subject.empty()) {
                // bad request
                response->write(StatusCode::client_error_bad_request);
                return;
            }

            if (nextdata.empty()) {
                headers.emplace("Transfer-Encoding", "chunked");
                headers.emplace("Content-Type", "application/json");
                response->write(StatusCode::success_ok, headers);

                HttpResponseStreamWriter writer(response);

                if (connected.empty()) {
                    // just lookup the subject
                    store->WriteCompleteEntityToStream(subject, datasets, writer);
                } else {
                    store->WriteRelatedEntitiesToStream(subject, connected, inverse, 0, 50, datasets, writer);
                }

                *response << "0\r\n" << "\r\n";
                response->flush();                 
            } else {
                // shred the next data token into the same bits

            }
        };
    }

    void WodServer::Start() {
       _server.start();
    }
}