"""
This script loads random data into an elasticsearch cluster.  As of this documentation, it creates the following:

* 100,000 boxes
* Boxes are split between 5 scans
* All scans belong to 1 sample
* All samples belong to 1 accession
* Each box has 1-3 random generated cell type sources
"""
import pprint
import random
import requests
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers

cell_type_ids = [100, 1001, 1002, 200, 2001, 2001]
scan_size = (1000, 1000)


def compute_degrees_per_slide(grid_size):
    """ @arg grid_size: The number of columns to divide the map into at the most zoomed-out level
    """
    lng_error = {
        1: 23,
        2: 5.6,
        3: 0.7,
        4: 0.18,
        5: 0.022,
        6: 0.0055,
        7: 0.00068,
        8: 0.00017,
    }
    return lng_error[2] * grid_size

degrees_per_slide = compute_degrees_per_slide(1)




def build_random_cell_type_sources():
    return [
        {
            "user_id": "user{}".format(id_ctr % 5),
            "cell_type_id": random.choice(cell_type_ids),
            "date": datetime.now() - timedelta(days=random.random()*100),
            "score": 1 if random.random() > 0.5 else -1,
        }
    for (id_ctr, i) in enumerate(range(int(random.random() * 2) + 1))]

def build_random_boxes(cnt):
    for id_ctr in range(cnt):

        x = int(random.random() * scan_size[0])
        y = int(random.random() * scan_size[1])

        yield {
            "id": id_ctr,
            "cell_type_id": random.choice(cell_type_ids),
            "scan_id": id_ctr % 5,
            "x": x,
            "y": y,
            "width": int(random.random() * 100) + 10,
            "height": int(random.random() * 100) + 10,
            "confidence": random.random(),

            # Modified extras that are useful for queries
            "sample_id": 1,
            "accession_id": 1,
            "cell_type_sources": build_random_cell_type_sources(),
            "location": {
                "lat": float(x) / scan_size[0] * degrees_per_slide,
                "lon": float(y) / scan_size[1] * degrees_per_slide,
            }
        }

print("Making search elasticsearch is running")
res = requests.get('http://localhost:9200')
print(res.content)
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

print("Loading random data into the index")
es.indices.delete(index='images', ignore=[400,404])

print("Create an index with the appropriate mappings")
es.indices.create(index='images', ignore=400, body={
    "mappings": {
        "image": {
            "properties": {
                "location": { "type": "geo_point" },
            },
        },
    },
})

print("Adding random boxes")
#for idx, b in enumerate(build_random_boxes(100)):
BATCH_SIZE = 5000

batch = []

def _index_batch():
    helpers.bulk(es, [
        {
            "_index": "images",
            "_type": "image",
            "_id": box['id'],
            "body": box,

        }
    for box in batch])

for idx, b in enumerate(build_random_boxes(500000)):
    #batch.append(b)
    #if (len(batch) % BATCH_SIZE == 0):
    #    _index_batch()
    #    batch = []
    #    print(idx)

    es.index(index='images', doc_type='image', id=b['id'], body=b)
    print(idx)

_index_batch()

print("Done!")

print("Show a random image")
pprint.pprint(es.get(index='images', doc_type='image', id=5))

