import pandas as pd
import json
from SparkStreaming.MongoDB import MongoDB

# for data in json_record:
#     print(data)
if __name__ == '__main__':
    mongourl='mongodb://localhost:27017/'
    db = 'bili'
    collection = 'bili'
    # mongo = MongoDB(mongourl, db)
    db = pd.DataFrame({'name': ['chenhao', 'chen'], 'age': [12, 34]})
    print(db)
    json_record = db.to_json(orient='records')
    print(json_record)
    # print()
    query = {'name': 'chenhao'}
    for x in json.loads(json_record):
        print(x)
    # mongo.insertOrUpdate('bili',query,json.loads(json_record)[1])
