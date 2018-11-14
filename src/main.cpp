#include <iostream>
#include <Server.h>
#include "spdlog/spdlog.h"
#include "spdlog/common.h"

namespace spd = spdlog;

using namespace std;
using namespace webofdata;

void printHelp() {
    cout << "Usage: wodserver" << endl << "\t options: " << endl;
    cout << "\t\t" << "--storeslocation \"/tmp/stores\"" << endl;
    cout << "\t\t" << "--port 8888" << endl;
    cout << "\t\t" << "--jwtsecret \"a big secret!234\"" << endl;
    cout << "\t\t" << "--logtofile false" << endl;
    cout << "\t\t" << "--logtostdout true" << endl;
    cout << "\t\t" << "--loglevel \"INFO\"" << endl;
    cout << "\t\t" << "--name \"node1\"" << endl;
    cout << "\t\t" << "--subjectidentifier \"http://unknown.webofdata.io/node1\"" << endl;
    cout << "\t\t" << "--help" << endl << endl;
    cout.flush();
}

int main(int argc, char *argv[]) {
    // get command line params
    // --storelocation []
    // --port []
    // --jwtSecret []
    // --loglevel INFO | DEBUG | WARN | ERROR
    // --logtofile true | false
    // --logtostdout true | false
    // --help
    // --name []
    // --subjectidentifier []

    string storesLocation("/tmp/stores");
    string subjectIdentifier("http://undefined.webofdata.io/node1");
    unsigned short port = 8888;
    string jwtSecret("a secret");
    bool logToFile = true;
    bool logToStdOut = true;
    string loglevel = "INFO";
    string nodename = "node1";

    for (int i = 1; i < argc; i += 2) {
        string argName(argv[i]);
        if (argName == "--help") {
            printHelp();
            exit(1);
        }

        if (i+1 == argc) { // no value for option
            printHelp();
            exit(1);
        }

        string argValue(argv[i + 1]);

        if (argName == "storeslocation") {
            storesLocation = argValue;
        }

        if (argName == "subjectidentifier") {
            subjectIdentifier = argValue;
        }

        if (argName == "name") {
            nodename = argValue;
        }

        if (argName == "port") {
            port = (unsigned short) strtoul(argValue.data(), nullptr, 0);
        }

        if (argName == "logtofile") {
            if (argValue == "false") logToFile = false;
        }

        if (argName == "logtostdout") {
            if (argValue == "false") logToStdOut = false;
        }

        if (argName == "loglevel") {
            loglevel = argValue;
        }
    }

    // TODO: check that storeslocation exists


    // configure logging with 2 sinks
    std::vector<spdlog::sink_ptr> sinks;
    if (logToFile) {
        sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_mt>());
    }

    if (logToStdOut) {
        sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(storesLocation + "/wod-service.log", 1048576 * 5, 3));
    }

    auto service_logger = std::make_shared<spdlog::logger>("wod_service_log", begin(sinks), end(sinks));
    service_logger->set_pattern("%+", spd::pattern_time_type::utc);
    service_logger->flush_on(spd::level::err);
    spdlog::register_logger(service_logger);

    service_logger->info(R"({{ "node" : "{}",  "msg" : "{} {}" }})", nodename, "Starting Server on port", port);

    // start server
    WodServer s(port, storesLocation, nodename, subjectIdentifier);
    s.ConfigureRoutes();
    s.Start();
    return 0;
}



