import pprint
import random
import requests
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def time_func(fn):
    """ A helper decorator to time each function and give us a notion of query times
    """
    def _inner(*args, **kwargs):
        print(">>>>>>>>>>>>>>>> Timing {}".format(fn.__name__), args)
        start = datetime.now()
        ret = fn(*args, **kwargs)
        elapsed = datetime.now() - start
        pprint.pprint(ret)
        print(">>>>>>>>>>>>>>>> Query took {} ms".format(elapsed.total_seconds()*1000))
        return ret
    return _inner

def wait():
    _ = raw_input("Press any continue to continue...")
    return

@time_func
def get_all_images_in_scan():
    return es.search(index="images",
        _source=False,
        sort="id:asc",
        body={
            "query": {
                "match": {
                    'scan_id': 1
                }
            }
        }
    )

@time_func
def get_all_verified_by_john():
    return es.search(index="images",
        _source=False,
        sort="id:asc",
        body={
            "query": {
                "nested": {
                    "path": "cell_type_sources",
                    "query": {
                        "match": {
                            "cell_type_sources.user_id": "user.1",
                        }
                    }
                }
            }
        }
    )

@time_func
def count_images_by_cell_type():
    return es.search(index="images",
        _source=False,
        size=0,

        body={
            # This will get define aggregate searches.  In this case we want to return the total number
            # of objects by cell type id
            "aggs": {
                "cell_type_count": {
                    "terms": {
                        "field": "cell_type_id"
                    },
                },
            },
            "query": {
                "match": {
                    'accession_id': 1
                }
            }
        }
    )

@time_func
def get_avg_image_metrics():
    return es.search(index="images",
        _source=False,
        size=0,

        body={
            # This will get define aggregate searches.  In this case we want to return the total number
            # of objects by cell type id
            "aggs": {
                "avg_width": {
                    "avg": {
                        "field": "width"
                    },
                },
                "avg_height": {
                    "avg": {
                        "field": "height"
                    },
                },
                "avg_area": {
                    "avg": {
                        "script": "doc.width.value * doc.height.value"
                    },
                },
            },
            "query": {
                "match": {
                    'scan_id': 1
                }
            }
        }
    )

# Get cluter points in a scan
@time_func
def get_cluster_points(precision):
    return es.search(index="images",
        _source=False,

        # This doesn't return any payload, just the aggregates
        size=0,

        body={
            # This will get define aggregate searches.  In this case we want to return the total number
            # of objects by cell type id
            "aggs": {
                "map_clusters": {
                    "geohash_grid": {
                        "field":"location",
                        "precision": precision,
                    },
                },
            }
        }
    )



#get_all_images_in_scan()
#wait()

get_all_verified_by_john()
wait()

#count_images_by_cell_type()
#wait()

#get_avg_image_metrics()
#wait()

#for p in range(1, 4):
#    get_cluster_points(p)
#    wait()
