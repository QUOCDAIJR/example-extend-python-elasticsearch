from abc import ABC, abstractmethod
import logging
from elasticsearch import Elasticsearch, ElasticsearchException


class BaseElastic(ABC):
    MAX_COUNT_SEARCH = 10000
    DEFAULT_SCROLL = "1m"

    def __init__(self, host, port, user=None, password=None, timeout='1s'):
        self._id = '_id'
        self._timeout = timeout
        self.current_connection = None
        try:
            if not self.index_conf():
                raise ElasticsearchException('Index is invalid')

            if not host or not port:
                raise ElasticsearchException('Please provide elastic server config')
            elif user and password:
                self.current_connection = Elasticsearch(host=host, port=port, http_auth=(user, password), )
            else:
                self.current_connection = Elasticsearch(host=host, port=port)

            if self.current_connection and self.is_connected() and not self.exists() and self.mapping_conf():
                self.init_mapping()

        except ElasticsearchException as e:
            raise ElasticsearchException(e)

    @abstractmethod
    def index_conf(self) -> str:
        pass

    @abstractmethod
    def mapping_conf(self) -> dict:
        return {}

    @abstractmethod
    def settings_conf(self) -> dict:
        return {}

    def get_connection(self):
        """
        :rtype: Elasticsearch
        """
        return self.current_connection

    def is_connected(self):
        if self.get_connection().ping():
            return True
        else:
            return False

    def exists(self):
        if self.is_connected():
            return self.get_connection().indices.exists(self.index_conf())
        else:
            return False

    def init_mapping(self):
        if self.is_connected():
            body = {
                'mappings': {
                    '_source': {
                        'enabled': True
                    },
                    'properties': self.mapping_conf()
                }
            }

            if self.settings_conf():
                body['settings'] = self.settings_conf()

            self.get_connection().indices.create(index=self.index_conf(), body=body)

    def index(self, data, refresh=True):
        if self.is_connected():
            if self._id in data:
                id = data[self._id]
            else:
                id = None

            result = self.get_connection().index(index=self.index_conf(), body=data, id=id, refresh=bool(refresh),
                                                 timeout=self._timeout)
            if result and '_shards' in result and 'successful' in result['_shards'] \
                    and result['_shards']['successful'] == 1:
                return True
        return False

    def update(self, index_id, data, refresh=True):
        if self.is_connected() and self.get_connection().exists(index=self.index_conf(), id=index_id) and data:
            body = {
                'doc': data
            }
            result = self.get_connection().update(index=self.index_conf(), id=index_id, body=body,
                                                  refresh=bool(refresh), timeout=self._timeout)
            if result and 'result' in result and (
                    (result['result'] == 'updated' and '_shards' in result and 'successful' in result['_shards'] and
                     result['_shards']['successful'] == 1)
                    or result['result'] == 'noop'
            ):
                return True

        return False

    def delete(self, index_id, refresh=True):
        result = False
        if self.is_connected() and index_id:
            if self.get_connection().exists(index=self.index_conf(), id=index_id):
                result = self.get_connection().delete(index=self.index_conf(), id=index_id, refresh=bool(refresh),
                                                      timeout=self._timeout)
            else:
                logging.error('No data to delete.')
        return result

    def delete_by_params(self, data, refresh=True):
        result = False
        if self.is_connected() and data:
            result = self.get_connection().delete_by_query(index=self.index_conf(), body=data, conflicts='proceed',
                                                           refresh=bool(refresh), timeout=self._timeout)
        else:
            logging.error('The parameter passed is not matched.')
        return result

    def bulk(self, data):
        result = False
        if self.is_connected() and data:
            result = self.get_connection().bulk(body=data, index=self.index_conf())
        else:
            logging.error('The parameter passed is not matched.')
        return result

    def search(self, params):
        result = []
        total = 0
        if self.is_connected() and self.exists():
            if 'body' in params:
                body = params['body']
            else:
                body = params

            data = self.get_connection().search(index=self.index_conf(), body=body)

            if 'hits' in data:
                if 'total' in data['hits'] and 'value' in data['hits']['total']:
                    total = data['hits']['total']['value']

                if 'hits' in data['hits']:
                    hits = data['hits']['hits']
                    step = 0
                    while step < len(hits):
                        if hits[step]:
                            result.append(hits[step])
                        step += 1

        return {
            'data': result,
            'total': total
        }

    def scroll(self, params, offset, limit):
        result = []
        total = 0
        if self.is_connected() and self.exists():
            if 'body' in params:
                body = params['body']
            else:
                body = params

            search_data = self.get_connection().search(index=self.index_conf(), body=body,
                                                       scroll=self.DEFAULT_SCROLL, size=limit)
            total = search_data['hits']['total']['value']

            i = len(search_data['hits']['hits'])
            if offset == 0:
                data = search_data['hits']['hits']
            else:
                scroll_id = search_data['_scroll_id']
                scroll_data = {}
                while i <= offset:
                    scroll_data = self.get_connection().scroll(scroll_id=scroll_id, scroll=self.DEFAULT_SCROLL)
                    count = len(scroll_data['hits']['hits'])
                    if count > 0:
                        scroll_id = scroll_data['_scroll_id']
                    else:
                        break
                    i += count
                data = scroll_data['hits']['hits'] if 'hits' in scroll_data and \
                                                      'hits' in scroll_data['hits'] else []

            if data:
                step = 0
                while step < len(data):
                    if data[step]:
                        result.append(data[step])
                    step += 1

        return {
            'data': result,
            'total': total
        }

    def scroll_all(self, params):
        data = []
        total = 0
        if self.is_connected() and self.exists():
            if 'body' in params:
                body = params['body']
            else:
                body = params
            search_data = self.get_connection().search(index=self.index_conf(), body=body,
                                                       scroll=self.DEFAULT_SCROLL, size=self.MAX_COUNT_SEARCH)
            total = search_data['hits']['total']['value']
            data += search_data['hits']['hits']

            scroll_id = search_data['_scroll_id']
            while True:
                scroll_data = self.get_connection().scroll(scroll_id=scroll_id, scroll=self.DEFAULT_SCROLL)
                count = len(scroll_data['hits']['hits'])
                if count > 0:
                    data += scroll_data['hits']['hits']
                    scroll_id = scroll_data['_scroll_id']
                else:
                    break

        return {
            'data': data,
            'total': total
        }

    def advanced_search(self, params, offset=0, limit=MAX_COUNT_SEARCH):
        if (offset + limit) <= self.MAX_COUNT_SEARCH:
            params = {
                **params,
                "from": offset,
                "size": limit
            }
            return self.search(params)
        else:
            return self.scroll(params, offset, limit)

    def advanced_search_all(self, params, total=None):
        if not total:
            total = self.count(params)
        if total <= self.MAX_COUNT_SEARCH:
            params = {
                **params,
                "from": 0,
                "size": total
            }
            return self.search(params)
        else:
            return self.scroll_all(params)

    def count(self, params):
        count = 0
        if self.is_connected() and self.exists():
            if 'body' in params:
                body = params['body']
            else:
                body = params

            if 'size' in body:
                del body['size']
            if 'from' in body:
                del body['from']
            if 'sort' in body:
                del body['sort']

            data = self.get_connection().count(index=self.index_conf(), body=body)
            if 'count' in data:
                count = data['count']
        return count
