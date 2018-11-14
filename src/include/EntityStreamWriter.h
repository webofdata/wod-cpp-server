
#ifndef WEBOFDATA_ENTITYSTREAMWRITER_H
#define WEBOFDATA_ENTITYSTREAMWRITER_H

#include <simple-web-server/server_http.hpp>
#include <fstream>

namespace webofdata {

    typedef SimpleWeb::Server<SimpleWeb::HTTP> HttpServer;

    class EntityStreamWriter {
    public:
        virtual ~EntityStreamWriter() {};

        virtual void WriteJson(const string& json) = 0;

        virtual void WriteJson(const char *json, int length) = 0;

        virtual void Flush() = 0;
    };

    class FileStreamWriter : public EntityStreamWriter {

    private:
        std::ofstream ofs;

    public:
        FileStreamWriter(string filename) : ofs(filename, std::ofstream::out) {
        }

        void WriteJson(const string& json) {
            ofs << json;
        }

        void WriteJson(const char *json, int length) {
            ofs.write(json, length);
        }

        void Flush() {
            ofs.flush();
        }
    };


    class HttpResponseStreamWriter : public EntityStreamWriter {
    private:
        shared_ptr <HttpServer::Response> _response;
        char* _buffer;
        int _buffered;
        int _bufferSize;
        int _flushCount;
    public:
        ~HttpResponseStreamWriter() {} // nothing to do

        HttpResponseStreamWriter(shared_ptr <HttpServer::Response> response) : _response(response) {
            _buffer = new char[1024*50];
            _buffered = 0;
            _bufferSize = 1024*50;
            _flushCount = 0;
        }

        void WriteJson(const string& json) {
            // writing out chunked transfer encoded content.
            // this means the length as a hex value (string representation) followed by CRLF,
            // content and CRLF
            *_response << std::hex << json.length() << "\r\n";
            _response->write(json.data(), json.length());
            *_response << "\r\n";
        }

        void WriteJson(const char *json, int length) {
            // add content to string
            if (_buffered + length < _bufferSize) {
                memcpy(_buffer, json, length);
                _buffered += length;
            } else {
                _flushCount++;
                *_response << std::hex << _buffered << "\r\n";
                _response->write(_buffer, _buffered);
                *_response << "\r\n";

                // reset and add latest
                _buffered = 0;
                memcpy(_buffer, json, length);
                _buffered += length;
            }
        }

        void Flush() {
            *_response << std::hex << _buffered << "\r\n";
            _response->write(_buffer, _buffered);
            *_response << "\r\n";            

            cout << "flush count : " << _flushCount;
        }
    };
}

#endif //WEBOFDATA_ENTITYSTREAMWRITER_H
