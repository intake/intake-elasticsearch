from intake.source import base
import json
from elasticsearch import Elasticsearch
import pandas as pd
import time

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

__version__ = '0.0.1'


class ElasticSearchSource(base.DataSource):
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

    def __init__(self, query, qargs, es_kwargs, metadata):
        self._query = query
        self._qargs = qargs
        self._scroll = es_kwargs.pop('scroll', '100m')
        self._size = es_kwargs.pop('size', 1000)  # default page size
        self._es_kwargs = es_kwargs
        self._dataframe = None
        self.es = Elasticsearch([es_kwargs])  # maybe should be (more) global?

        super(ElasticSearchSource, self).__init__(container='dataframe',
                                                  metadata=metadata)

    def _run_query(self, size=None):
        if size is None:
            size = self._size
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
        self.es.clear_scroll(scroll_id=sid)
        return s

    def _get_schema(self, retry=2):
        """Get schema from first 10 hits or cached dataframe"""
        if self._dataframe is not None:
            return base.Schema(datashape=None,
                               dtype=self._dataframe[:0],
                               shape=self._dataframe.shape,
                               npartitions=1,
                               extra_metadata=self._extra_metadata)
        else:
            while True:
                results = self._run_query(10)
                if 'hits' in results and results['hits']['hits']:
                    # ES likes to return empty result-sets while indexing
                    break
                retry -= 0.2
                time.sleep(0.2)
                if retry < 0:
                    raise IOError('No results arrived')
            df = pd.DataFrame([r['_source'] for r in results['hits']['hits']])
            results.pop('hits')
            self._extra_metadata = results
            return base.Schema(datashape=None,
                               dtype=df[:0],
                               shape=(None, df.shape[1]),
                               npartitions=1,
                               extra_metadata=self._extra_metadata)

    def to_dask(self):
        """Make single-partition lazy dask data-frame"""
        import dask.dataframe as dd
        from dask import delayed
        self.discover()
        part = delayed(self._get_partition(0))
        return dd.from_delayed([part], meta=self.dtype)

    def _get_partition(self, _):
        """Downloads all data

        ES has a hard maximum of 10000 items to fetch. Otherwise need to
        implement paging, known to ES as "scroll"
        https://stackoverflow.com/questions/41655913/elk-how-do-i-retrieve-more-than-10000-results-events-in-elastic-search
        """
        if self._dataframe is None:
            results = self._run_query()
            df = pd.DataFrame([r['_source'] for r in results['hits']['hits']])
            self._dataframe = df
            self._schema = None
            self.discover()
        return self._dataframe

    def _close(self):
        self._dataframe = None
