
import json

# generate line item data


# generate order data


# generate sample of people working for a business with some friends.
def gendata(filename, count):
	with open(filename, 'w') as f:
		first = True
		f.write("[")

		for i in range(0,count):
			id = "obj" + str(i)
			d = { "@id" : id,
			      "name" : str("person ") + str(i), 
			      "address" : "oslo",
				  "description"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description1"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description2"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description3"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description4"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description5"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description6"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description7"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
				  "description8"    : "skfksfksg s lorem sjfnsj and the sun came up over the hills and it was nice",
			      "type" : "person",
				  "friend" : "<obj" + str(i + 1) + ">",
				  "friend1" : "<obj" + str(i + 2) + ">",
				  "friend2" : "<obj" + str(i + 3) + ">",
				  "friend3" : "<obj" + str(i + 4) + ">",
				  "friend4" : "<obj" + str(i + 5) + ">",
                  "company" : "<company" + str(i + 1) + ">",
                  "owner"   :  "<owner1>",
                  "department" : "<dept1443>"
                  }
			data = json.dumps(d)
			if first:
				f.write(data)        	
				first = False
			else:
				f.write(",")
				f.write(data)
		f.write("]")

gendata("/tmp/data/sample1.json", 1)
gendata("/tmp/data/sample100.json", 100)
gendata("/tmp/data/sample100k.json" , 100000)
gendata("/tmp/data/sample1m.json" , 1000000)
# gendata("/tmp/data/sample10m.json", 10000000)



