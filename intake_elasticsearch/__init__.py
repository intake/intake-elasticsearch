from .elasticsearch_table import __version__, ElasticSearchSource
from intake.source import base


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='elasticsearch_table',
                                     version=__version__,
                                     container='dataframe',
                                     partition_access=False)

    def open(self, query, **kwargs):
        """
        Parameters:
            query : str
                Query string (lucene syntax or JSON text)
            qargs: dict
                Set of modifiers to apply to the query
                (http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch)
            kwargs (dict):
                Additional parameters to pass to ElasticSearch init.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return ElasticSearchSource(query=query, qargs=qargs,
                                   es_kwargs=source_kwargs,
                                   metadata=base_kwargs['metadata'])


