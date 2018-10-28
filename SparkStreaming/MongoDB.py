import pymongo


class MongoDB():
    def __init__(self, mongoUrl, db):
        mymongo = pymongo.MongoClient(mongoUrl)
        self.db = mymongo[db]

    '''更新'''

    def update_one(self, collection, query, document):
        collec = self.db[collection]
        return collec.update_one(query, document).matched_count

    def update_many(self, collection, query, documents):
        collec = self.db[collection]
        return collec.update_many(query, documents).matched_count

    '''插入'''
    def insert_one(self, collection, document):
        collec = self.db[collection]
        return collec.insert_one(document).inserted_id

    def insert_many(self, collection, documents):
        collec = self.db[collection]
        return collec.insert_many(documents).inserted_ids

    '''查询'''
    def query_one(self, collection, query, showField={}):
        collec = self.db[collection]
        return collec.find_one(query, showField)

    def query_many(self, collection, query, showField={}):
        collec = self.db[collection]
        return collec.find(query, showField)

    '''删除 返回删除的条数'''
    def delete_one(self, collection, query):
        collec = self.db[collection]
        return collec.delete_one(query).deleted_count

    def delete_many(self, collection, query):
        collec = self.db[collection]
        return collec.delete_many(query).deleted_count

    '''排序 默认大到小'''
    def find_sort(self, collection, query, orderKey, asc=-1):
        collec = self.db[collection]
        return collec.find(query).sort(orderKey, asc)

    '''查找并修改全部'''
    def find_and_modify(self, collection, query, newValue):
        collec = self.db[collection]
        return collec.find_and_modify(query, newValue)

    '''查找一个并删除'''
    def find_one_delete(self, collection, query):
        collec = self.db[collection]
        return collec.find_one_and_delete(query)

    '''TopK'''
    def TopK(self, collection, query, orderKey, K, asc=-1):
        collec = self.db[collection]
        return collec.find(query).sort(orderKey, asc).limit(K)

    '''插入一条记录,如果存在就覆盖'''
    def insertOrUpdate(self, collection, query, newValue):
        collec = self.db[collection]
        # print(collec.find_one(query))
        if collec.find_one(query) is not None:
            print("hh")
            collec.find_and_modify(query, newValue)
        else:
            collec.insert_one(newValue)


if __name__ == '__main__':
    # mongourl='mongodb://localhost:27017/'
    # db = 'bili'
    # collection = 'bili'
    # mongo = MongoDB(mongourl, db)
    # mongo.insert_one('hh', {
    #     "name": 'chenhao',
    #     "body": {
    #         "height": 12,
    #         "width": 23
    #     }
    # })
    # query = {'name': 'chenhao'}
    # mongo.find_and_modify(db,query,{'age': 122})
    # print(mongo.insert_one('hh'))
