from intake.source import base
import json
from . import __version__
try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


class ElasticSearchSeqSource(base.DataSource):
    """
    Data source which executes arbitrary queries on ElasticSearch

    This is the tabular reader: will return dataframes. Nested return items
    will become dict-like objects in the output.

    Parameters
    ----------
    query: str
       Query to execute. Can either be in Lucene single-line format, or a
       JSON structured query (presented as text)
    qargs: dict
        Further parameters to pass to the query, such as set of indexes to
        consider, filtering, ordering. See
        http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
    es_kwargs: dict
        Settings for the ES connection, e.g., a simple local connection may be
        ``{'host': 'localhost', 'port': 9200}``.
        Other keywords to the Plugin that end up here and are material:

        scroll: str
            how long the query is live for, default ``'100m'``
        size: int
            the paging size when downloading, default 1000.
    metadata: dict
        Extra information for this source.
    """
    name = 'elasticsearch_seq'
    container = 'python'
    version = __version__
    partition_access = False

    def __init__(self, query, qargs={}, metadata={}, **es_kwargs):
        from elasticsearch import Elasticsearch
        self._query = query
        self._qargs = qargs
        self._scroll = es_kwargs.pop('scroll', '100m')
        self._size = es_kwargs.pop('size', 1000)  # default page size
        self._es_kwargs = es_kwargs
        self._dataframe = None
        self.es = Elasticsearch([es_kwargs])  # maybe should be (more) global?

        super(ElasticSearchSeqSource, self).__init__(metadata=metadata)

    def _run_query(self, size=None, end=None):
        if size is None:
            size = self._size
        if end is not None:
            size = min(end, size)
        try:
            q = json.loads(self._query)
            if 'query' not in q:
                q = {'query': q}
            s = self.es.search(body=q, size=size, scroll=self._scroll,
                               **self._qargs)
        except (JSONDecodeError, TypeError):
            s = self.es.search(q=self._query, size=size, scroll=self._scroll,
                               **self._qargs)
        sid = s['_scroll_id']
        scroll_size = s['hits']['total']
        while scroll_size > len(s['hits']['hits']):
            page = self.es.scroll(scroll_id=sid, scroll=self._scroll)
            sid = page['_scroll_id']
            s['hits']['hits'].extend(page['hits']['hits'])
            if end is not None and len(s['hits']['hits']) > end:
                break
        self.es.clear_scroll(scroll_id=sid)
        return s

    def _get_schema(self, retry=2):
        """Get schema from first 10 hits or cached dataframe"""
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        """Downloads all data

        ES has a hard maximum of 10000 items to fetch. Otherwise need to
        implement paging, known to ES as "scroll"
        https://stackoverflow.com/questions/41655913/elk-how-do-i-retrieve-more-than-10000-results-events-in-elastic-search
        """
        results = self._run_query()
        return [r['_source'] for r in results['hits']['hits']]
