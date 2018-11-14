#ifndef WEBOFDATA_SERVER_H
#define WEBOFDATA_SERVER_H

#include "server_http.hpp"
#include <boost/asio.hpp>
#include "StoreManager.h"
#include "spdlog/spdlog.h"
#include "spdlog/fmt/bundled/ostream.h"
#include "Store.h"

namespace webofdata {

    using namespace std;
    typedef SimpleWeb::Server<SimpleWeb::HTTP> HttpServer;

    class ServiceException : public exception {
    private:
        string _msg;
    public:
        explicit ServiceException(string msg) {
            _msg = msg;
        }
    };

    class WodServer {
    private:
        HttpServer _server;
        shared_ptr<StoreManager> _storeManager;
        shared_ptr<spdlog::logger> _logger;
        string _serviceId;
        string _subjectIdentifier;

    public:

        WodServer(unsigned short port, string baseLocation, string serviceId, string subjectIdentifier) {
            _logger = spdlog::get("wod_service_log");
            _serviceId = serviceId;
            _subjectIdentifier = subjectIdentifier;
            _server.config.port = port;
            _server.config.thread_pool_size = 50;
            _storeManager = make_shared<StoreManager>(baseLocation);
        }

        WodServer(unsigned short port, shared_ptr<StoreManager> storeManager, string serviceId,
                  string subjectIdentifier) {
            _logger = spdlog::get("wod_service_log");
            _server.config.port = port;
            _server.config.thread_pool_size = 50;
            _serviceId = serviceId;
            _subjectIdentifier = subjectIdentifier;
            _storeManager = std::move(storeManager);
        }

        string MakeGuid();

        string GetRequestId(shared_ptr<HttpServer::Request> request);

        void ConfigureRoutes();

        void Start();
    };
}

#endif //WEBOFDATA_SERVER_H
