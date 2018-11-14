#include "EntityHandler.h"

namespace webofdata {

    std::string httpStart("http");

    string EntityHandler::AssertResourceId(string localResourceName) {

        auto it = _localResourceToIdIndex.find(localResourceName);
        if (it != _localResourceToIdIndex.end()) {
            return it->second;
        }

        if (localResourceName.compare(0, httpStart.length(), httpStart) == 0) {
            // find last hash or slash
            auto foundAt = localResourceName.find_last_of('#');
            if (foundAt != std::string::npos) {
                // get prefix
                auto prefix = localResourceName.substr(0, foundAt + 1);
                auto name = localResourceName.substr(foundAt + 1);
                // lookup expansion in namespace index
                int nsid = AssertPrefixId(prefix);

                auto rid = "ns" + std::to_string(nsid) + string(":") + name;
                _localResourceToIdIndex[localResourceName] = rid;
                return rid;
            }

            foundAt = localResourceName.find_last_of('/');
            if (foundAt != std::string::npos) {
                auto prefix = localResourceName.substr(0, foundAt + 1);
                auto name = localResourceName.substr(foundAt + 1);
                // lookup expansion in namespace index
                int nsid = AssertPrefixId(prefix);
                auto rid = "ns" + std::to_string(nsid) + string(":") + name;
                _localResourceToIdIndex[localResourceName] = rid;
                return rid;
            }
        }

        // check if it contains a :
        auto colonLocation = localResourceName.find_first_of(':');
        if (colonLocation != std::string::npos) {
            string prefix = localResourceName.substr(0, colonLocation);
            string name = localResourceName.substr(colonLocation);
            int nsid = AssertPrefixId(prefix);
            auto rid = "ns" + std::to_string(nsid) + string(":") + name;
            _localResourceToIdIndex[localResourceName] = rid;
            return rid;
        } else {
            // resolve against context
            int nsid = AssertPrefixId(this->_contextDefaultPrefix);
            auto rid = "ns" + std::to_string(nsid) + string(":") + localResourceName;
            _localResourceToIdIndex[localResourceName] = rid;
            return rid;
        }
    }

    int EntityHandler::AssertPrefixId(string prefix) {
        auto it = _localPrefixToIdIndex.find(prefix);
        if (it == _localPrefixToIdIndex.end()) {
            auto nsid = _store->AssertNamespace(prefix);
            _localPrefixToIdIndex[prefix] = nsid;
            return nsid;
        } else {
            return it->second;
        }
    }

    string EntityHandler::AssertPropertyId(string localProperty) {

        auto it = _localPropertyToIdIndex.find(localProperty);
        if (it != _localPropertyToIdIndex.end()) {
            return it->second;
        }

        if (localProperty.compare(0, httpStart.length(), httpStart) == 0) {
            // find last hash or slash
            auto foundAt = localProperty.find_last_of('#');
            if (foundAt != std::string::npos) {
                // get prefix
                auto prefix = localProperty.substr(0, foundAt);
                auto name = localProperty.substr(foundAt);
                // lookup expansion in namespace index
                int nsid = AssertPrefixId(prefix);
                auto pid = "ns" + std::to_string(nsid) + string(":") + name;
                _localPropertyToIdIndex[localProperty] = pid;
                return pid;
            }

            foundAt = localProperty.find_last_of('/');
            if (foundAt != std::string::npos) {
                auto prefix = localProperty.substr(0, foundAt);
                auto name = localProperty.substr(foundAt);
                // lookup expansion in namespace index
                int nsid = AssertPrefixId(prefix);
                auto pid = "ns" + std::to_string(nsid) + string(":") + name;
                _localPropertyToIdIndex[localProperty] = pid;
                return pid;
            }
        }

        // check if it contains a :
        auto colonLocation = localProperty.find_first_of(':');
        if (colonLocation != std::string::npos) {
            string prefix = localProperty.substr(0, colonLocation);
            string name = localProperty.substr(colonLocation + 1);
            int nsid = AssertPrefixId(prefix);
            auto rid = "ns" + std::to_string(nsid) + string(":") + name;
            _localPropertyToIdIndex[localProperty] = rid;
            return rid;
        } else {
            // resolve againt context
            int nsid = AssertPrefixId(this->_contextDefaultPrefix);
            auto rid = "ns" + std::to_string(nsid) + string(":") + localProperty;
            _localPropertyToIdIndex[localProperty] = rid;
            return rid;
        }
    }

    void EntityHandler::WriteKey() {
        _writer->Key(_currentNsKey.data());
    }

    bool EntityHandler::Null() {
        _writer->Null();
        return true;
    }

    bool EntityHandler::Bool(bool b) {
        _writer->Bool(b);
        return true;
    }

    bool EntityHandler::Int(int i) {
        _writer->Int(i);
        return true;
    }

    bool EntityHandler::Uint(unsigned u) {
        _writer->Uint(u);
        return true;
    }

    bool EntityHandler::Int64(int64_t i) {
        _writer->Int64(i);
        return true;
    }

    bool EntityHandler::Uint64(uint64_t u) {
        _writer->Uint64(u);
        return true;
    }

    bool EntityHandler::Double(double d) {
        _writer->Double(d);
        return true;
    }

    bool EntityHandler::String(const char *str, SizeType length, bool copy) {
        if (_inIdProperty) {
            if (strcmp(str, "@context") == 0) {
                _inContextEntity = true;
                _inIdProperty = false;

                // reset the writer
                _newJson->Clear();
                _writer->Reset(*_newJson);

                return true;
            } else {
                _currentRid = AssertResourceId(str);
                _writer->String(_currentRid.data());
                _inIdProperty = false;
                return true;
            }
        } else {
            if (_inContextEntity && _inNamespaceSection) {
                // take the expansion and assert it as a namespace in the store
                if (_currentKey == "_") {
                    _contextDefaultPrefix = str;
                } else {
                    auto nsid = _store->AssertNamespace(str);
                    _context[_currentKey] = nsid;
                }
            } else {
                if (str[0] == '<' && str[length - 1] == '>') {
                    // make reference value
                    auto refId = AssertResourceId(string(&str[1], length - 2));
                    auto refValue = string("<") + refId + string(">");
                    _writer->String(refValue.data());

                    if (_objDepth == 1) {
                        // add reference
                        _currentRefs.push_back(std::pair<string, string>(_currentNsKey, refId));
                    }
                } else {
                    _writer->String(str, length, copy);
                }
            }
        }
        return true;
    }

    bool EntityHandler::StartObject() {
        _objDepth++;
        _writer->StartObject();
        return true;
    }

    bool EntityHandler::Key(const char *str, SizeType length, bool copy) {
        _currentKey = string(str);
        if (strcmp(str, "@id") == 0) {
            _inIdProperty = true;
            _currentNsKey = "@id";
            WriteKey();
        } else {
            if (_inContextEntity && strcmp(str, "namespaces") == 0) {
                _inNamespaceSection = true;
                return true;
            } else if (_inContextEntity && _inNamespaceSection) {
                return true;
            } else {
                _currentNsKey = AssertPropertyId(_currentKey);
                WriteKey();
            }
        }

        return true;
    }

    bool EntityHandler::EndObject(SizeType memberCount) {
        _objDepth--;
        if (_inContextEntity && _inNamespaceSection) {

            if (_contextDefaultPrefix.empty()) {
                _contextDefaultPrefix = "http://data.wod.io/types/";
            }

            _inNamespaceSection = false;
            return true;
        }

        if (_inContextEntity && !_inNamespaceSection) {
            _inContextEntity = false;
            _newJson->Clear();
            _writer->Reset(*_newJson);
            return true;
        }

        _writer->EndObject();

        if (_objDepth == 0) {
            _entityCount++;
            _newJson->Flush();
            auto json = string(_newJson->GetString());
            _store->WriteEntity(_writeBatch, _dataset, json, _currentRid, _currentRefs);
            _batchCount++;
            _currentRefs.clear();

            if (this->_batchCount >= 100) {
                string name("");
                _store->WriteBatch(name, _entityCount, _writeBatch);
                _writeBatch->Clear();
                _batchCount = 0;
            }

            // reset state ready for next entity...
            _newJson->Clear();
            _writer->Reset(*_newJson);
        }

        return true;
    }

    void EntityHandler::Flush() {
        if (_batchCount > 0) {
            string name("");
            _store->WriteBatch(name, _entityCount, _writeBatch);
            _writeBatch->Clear();
            _batchCount = 0;
        }
    }

    bool EntityHandler::StartArray() {
        if (_state == 0) {
            _state = 1; // move into processing
            _arrayDepth = 0;
            return true;
        }

        _arrayDepth++;
        _writer->StartArray();
        return true;
    }

    bool EntityHandler::EndArray(SizeType elementCount) {
        _arrayDepth--;
        if (_arrayDepth < 0) return true; // this is the outer array.

        _writer->EndArray();
        return true;
    }
}