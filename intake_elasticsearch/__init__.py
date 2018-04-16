from intake.source import base

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class ESSeqPlugin(base.Plugin):
    """Plugin for ElasticSearch to sequence reader"""

    container = 'python'
    name = 'elasticsearch_seq'
    source = 'seq'

    def __init__(self):
        base.Plugin.__init__(self, name=self.name,
                             version=__version__,
                             container=self.container,
                             partition_access=False)

    def open(self, query, **kwargs):
        """
        Create ElasticSearchSource instance

        Parameters:
            query : str
                Query string (lucene syntax or JSON text)
            qargs: dict
                Set of modifiers to apply to the query
                (https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search)
            kwargs (dict):
                Additional parameters to pass to ElasticSearch init.
        """
        from .elasticsearch_table import ElasticSearchTableSource
        from .elasticsearch_seq import ElasticSearchSeqSource
        sources = {'seq': ElasticSearchSeqSource,
                   'table': ElasticSearchTableSource}
        source = sources[self.source]
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return source(query=query, qargs=qargs,
                      es_kwargs=source_kwargs,
                      metadata=base_kwargs['metadata'])


class ESTablePlugin(ESSeqPlugin):
    """Plugin for ElasticSearch to pandas reader"""

    container = 'dataframe'
    name = 'elasticsearch_table'
    source = 'table'
