import json
from indexs import ExampleIndex


if __name__ == '__main__':
    es = ExampleIndex("127.0.0.1", "9200")
    example_data = {
        "id": "1",
        "example_field": "example"
    }
    # index document
    es.index(example_data)

    # search
    example_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "id": 1
                        }
                    }
                ]
            }
        }
    }
    data = es.search(example_query)
    print(json.dumps(data, indent=4))
    # or scroll if document is over 10000 (default max search of es)
    data = es.advanced_search(example_query)
    print(json.dumps(data, indent=4))

    # and some other method in es.py
