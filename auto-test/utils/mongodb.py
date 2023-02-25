import pymongo
import random


def gen_new_row(row):
    new_row = {}
    for k, v in row.items():
        new_row[k] = v
        if isinstance(v, str) and v != "":
            new_row[k] = v[0:len(v) - 1] + str(random.randint(0, 9))
        if isinstance(v, int):
            new_row[k] = random.randint(0, 1000)
    del (new_row["_id"])
    return new_row


class MongoDBUtil:
    connector = "mongodb"

    def __init__(self, config, table=None):
        uri = config["uri"]
        self.mongo_client = pymongo.MongoClient(uri)
        self.db = uri.split("/")[3].split("?")[0]
        self.table = table

    def __destroy__(self):
        try:
            self.mongo_client.close()
        except Exception as e:
            pass

    def load(self, data, drop=False, table=None):
        if table is None:
            table = self.table
        if drop:
            self.mongo_client[self.db][table].drop()
        try:
            self.mongo_client[self.db][table].insert_many(data)
        except Exception as e:
            return False
        return True

    def bench(self, table=None, number=1):
        if table is None:
            table = self.table
        exist_document = self.query()[0]
        while True:
            if number <= 0:
                break
            b = []
            bb = number
            if bb > 10000:
                bb = 10000
            for i in range(bb):
                b.append(gen_new_row(exist_document))
            self.mongo_client[self.db][table].insert_many(b)
            number = number - bb

    def insert(self, document=None, table=None):
        if table is None:
            table = self.table
        if document is None:
            exist_document = self.query()[0]
            document = gen_new_row(exist_document)
        return self.mongo_client[self.db][table].insert_one(document)

    def update(self, condition, document, table=None):
        if table is None:
            table = self.table
        return self.mongo_client[self.db][table].update_one(condition, document)

    def delete(self, condition={}, limit=1, table=None):
        if table is None:
            table = self.table
        if limit == 1:
            return self.mongo_client[self.db][table].delete_one(condition)
        return self.mongo_client[self.db][table].delete_many(condition)

    def query(self, condition={}, limit=1, table=None):
        if table is None:
            table = self.table
        return list(self.mongo_client[self.db][table].find(condition).limit(limit))
