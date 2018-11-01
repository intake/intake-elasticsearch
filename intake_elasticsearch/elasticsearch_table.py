from intake.source import base

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError
from .elasticsearch_seq import ElasticSearchSeqSource


class ElasticSearchTableSource(ElasticSearchSeqSource):
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
    _dataframe = None
    container = 'dataframe'

    def __init__(self, *args, **kwargs):
        ElasticSearchSeqSource.__init__(self, *args, **kwargs)
        self.part = True

    def _get_schema(self):
        import pandas as pd
        """Get schema from first 10 hits or cached dataframe"""
        if self._dataframe is None:
            results = self._run_query(end=100)
            df = pd.DataFrame([r['_source'] for r in results['hits']['hits']])
            self._dataframe = df
            self.part = True
        dtype = {k: str(v) for k, v
                 in self._dataframe.dtypes.to_dict().items()}
        shape = (None if self.part else len(self._dataframe), len(dtype))
        return base.Schema(datashape=None,
                           dtype=dtype,
                           shape=shape,
                           npartitions=self.npartitions,
                           extra_metadata=self.metadata)

    def to_dask(self):
        """Make single-partition lazy dask data-frame"""
        import dask.dataframe as dd
        from dask import delayed
        self.discover()
        parts = []
        if self.npartitions == 1:
            part = delayed(self._get_partition)()
            return dd.from_delayed([part], meta=self.dtype)

        for slice_id in range(self.npartitions):
            part = delayed(self._get_partition)(slice_id)
            parts.append(part)
        return dd.from_delayed(parts, meta=self.dtype)

    def _get_partition(self, partition=None):
        """
        Downloads all data or get the partiont-th slice of the scroll query

        ES has a hard maximum of 10000 items to fetch. Otherwise need to
        implement paging, known to ES as "scroll"
        https://stackoverflow.com/questions/41655913/elk-how-do-i-retrieve-more-than-10000-results-events-in-elastic-search

        Parameters
        ----------

        partition: int|None
            Slice id for the slice query or None.
        """
        import pandas as pd
        results = super(ElasticSearchTableSource, self)._get_partition(
            partition)
        df = pd.DataFrame(results)
        if df.empty:
            df = self._dataframe[:0]
        self._schema = None
        self.part = False

        self.discover()

        return df

    def _close(self):
        self._dataframe = None
