import sys
import pickle
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


with open(sys.argv[1], 'rb') as f:
    data = pickle.load(f)

print("CONNECTING")
host = "HOST"
awsauth = AWS4Auth("KEY", "SECRETKEY", 'us-west-2', 'es')
es = Elasticsearch([{'host': host, 'port': 443}], http_auth=awsauth, use_ssl=True, verify_certs=True, connection_class=RequestsHttpConnection)
print(es.info())

print("DONE")

entries_indexed = 0

es.indices.delete(index='actions', ignore=[400,404])

for user_id, actions in data['user_actions'].items():
    name = data['user_labels'][user_id]
    print("Loading data for user", name)
    for a in actions:
        a['username'] = name
        es.index(index='actions', doc_type='action', body=a)
        entries_indexed += 1
    print("Done!")

print("Pickle file has been indexed with {} entries".format(entries_indexed))
