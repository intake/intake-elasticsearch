
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .elasticsearch_seq import ElasticSearchSeqSource
from .elasticsearch_table import ElasticSearchTableSource
