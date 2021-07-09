from es import BaseElastic

prefix_index = 'ana_'
default_settings = {
    'number_of_shards': 5,
    'number_of_replicas': 3,
    # 'analysis': {
    #     "analyzer": {
    #         "vietnamese_standard": {
    #             "tokenizer": "icu_tokenizer",
    #             "filter": [
    #                 "icu_folding",
    #                 "icu_normalizer",
    #                 "icu_collation"
    #             ]
    #         }
    #     }
    # }
}


class ExampleIndex(BaseElastic):

    def __init__(self, host=None, port=None, user=None, password=None):
        super().__init__(
            host=host if host is not None else "example_elasticsearch_host",
            port=port if port is not None else "example_elasticsearch_port"
        )
        self._id = 'id'

    def index_conf(self) -> str:
        return prefix_index + 'example_index'

    def settings_conf(self) -> dict:
        return default_settings

    def mapping_conf(self) -> dict:
        return {
            "id": {
                "type": "integer"
            },
            "example_field": {
                "type": "keyword"
            }
        }


class ExampleIndex2(BaseElastic):

    def __init__(self):
        super(ExampleIndex2, self).__init__(
            host="example_elasticsearch_host_2",
            port="example_elasticsearch_port_2"
        )
        self._id = 'id'

    def index_conf(self) -> str:
        return prefix_index + 'example_index_2'

    def settings_conf(self) -> dict:
        return default_settings

    def mapping_conf(self) -> dict:
        return {
            "id": {
                "type": "integer"
            },
            "example_field": {
                "type": "keyword"
            },
            "example_field_2": {
                "type": "keyword"
            }
        }
