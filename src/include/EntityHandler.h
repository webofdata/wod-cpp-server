#ifndef WEBOFDATA_ENTITYHANDLER_H
#define WEBOFDATA_ENTITYHANDLER_H

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <string>
#include <iostream>
#include <sstream>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include "Store.h"

namespace webofdata {

	class Store;

	using namespace std;
	using namespace rapidjson;
	using byte = unsigned char;
	using ulong = unsigned long;

	class EntityHandler : public BaseReaderHandler<UTF8<>, EntityHandler> {

	private:

		shared_ptr<Store> _store;
		shared_ptr<DataSet> _dataset;
		int _batchSize;
		int _entityCount;

		map<string, string> _localResourceToIdIndex; // used per run to keep local ids to hand
		map<string, int> _localPrefixToIdIndex; // maps ns prefixes such as http://www.example.org => 1
		map<string, string> _localPropertyToIdIndex; // maps ns prefixes such as http://www.example.org => 1
		map<string, int> _context;
		string _contextDefaultPrefix = "http://test.webofdata.io/things/";

		StringBuffer *_newJson;
		Writer<StringBuffer> *_writer;

		bool _inIdProperty = false;
		bool _inContextEntity = false;
		bool _inNamespaceSection = false;
		bool _inDatatypesSection = false;

		string AssertResourceId(string uri);

		string AssertPropertyId(string uri);

		int AssertPrefixId(string prefix); // maps a uri such as http://example.org => 1

		int _state = 0; // used to indicate when we pass the opening '['
		int _objDepth = 0; // used to know when we are on the top level for creating refs
		string _currentKey; // current json key
		string _currentNsKey;
		string _currentRid; // current resource id

		vector<std::pair<string, string>> _currentRefs;

		int _arrayDepth = 0;

		shared_ptr<WriteBatch> _writeBatch;
		int _batchCount = 0;

		void WriteKey();

	public:
		EntityHandler(shared_ptr<Store> store, shared_ptr<DataSet> dataset, shared_ptr<WriteBatch> writeBatch) {
			_store = store;
			_dataset = dataset;
			_newJson = new StringBuffer();
			_writer = new Writer<StringBuffer>(*_newJson);
			_writeBatch = writeBatch;
			_entityCount = 0;
		}

		~EntityHandler() {
			delete _writer;
			delete _newJson;
		}

		long GetEntityCount() {
			return _entityCount;
		}

		void Flush();

		bool Null();

		bool Bool(bool b);

		bool Int(int i);

		bool Uint(unsigned u);

		bool Int64(int64_t i);

		bool Uint64(uint64_t u);

		bool Double(double d);

		bool String(const char *str, SizeType length, bool copy);

		bool StartObject();

		bool Key(const char *str, SizeType length, bool copy);

		bool EndObject(SizeType memberCount);

		bool StartArray();

		bool EndArray(SizeType elementCount);
	};
}

#endif //WEBOFDATA_ENTITYHANDLER_H
