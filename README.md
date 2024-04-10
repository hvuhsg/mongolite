# mongolite 
Lite mongodb engine in python  

[![Run Tests](https://github.com/hvuhsg/mongolite/actions/workflows/test.yml/badge.svg)](https://github.com/hvuhsg/mongolite/actions/workflows/test.yml) 
[![BringThemBack](https://badge.yehoyada.com/)](https://www.standwithus.com/)  

---

```shell
pip install pymongolite
```

## Examples

#### simple 
```python
from pymongolite import MongoClient

with MongoClient(dirpath="~/my_db_dir", database="my_db") as client:
    db = client.get_default_database()
    collection = db.get_collection("users")

    collection.insert_one({"name": "yoyo"})
    collection.update_one({"name": "yoyo"}, {"$set": {"age": 20}})
    user = collection.find_one({"age": 20})
    print(user) # -> {"_id": ObjectId(...), "name": "yoyo", "age": 20}
```

```python
from pymongolite import MongoClient

client = MongoClient(dirpath="~/my_db_dir", database="my_db")

db = client.get_default_database()
collection = db.get_collection("users")

collection.insert_one({"name": "yoyo"})
collection.update_one({"name": "yoyo"}, {"$set": {"age": 20}})
user = collection.find_one({"age": 20})
print(user) # -> {"_id": ObjectId(...), "name": "yoyo", "age": 20}

client.close()
```

#### Indexes
```python
from pymongolite import MongoClient

client = MongoClient(dirpath="~/my_db_dir", database="my_db")

db = client.get_default_database()
collection = db.get_collection("users")

# Make query with name faster
collection.create_index({"name": 1})

collection.insert_one({"name": "yoyo"})
collection.update_one({"name": "yoyo"}, {"$set": {"age": 20}})
user = collection.find_one({"age": 20})
print(user) # -> {"_id": ObjectId(...), "name": "yoyo", "age": 20}

indexes = collection.get_indexes()
print(indexes)  # -> [{'id': UUID('8bb4cac8-ae52-4fff-9e69-9f36a99956cd'), 'field': 'age', 'type': 1, 'size': 1}]

client.close()
```

## Support
The goal of this project is to create sqlite version for mongodb

### For now the library is supporting:
#### actions:
- database
  - create_database
  - get_database
  - drop_database
- collection
  - create_collection
  - get_collection
  - drop_collection
  - get_collection_names
- index
  - create_index
  - delete_index
  - get_indexes
- document
  - insert_many / insert_one
  - update_many / update_one
  - find / find_one
  - replace_many / replace_one
#### filtering ops:
- field matching
- $eq / $ne
- $gt / $gte
- $lt / $lte
- $not
- $and / $or / $nor
- $exists
- $in / $nin
#### mutation ops:
- $set
- $unset
- $inc
- $addToSet
  - $each
- $push
  - $each
  - $sort
  - $slice
- $pull
