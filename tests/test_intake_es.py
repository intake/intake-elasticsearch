import os
import pickle

from elasticsearch import Elasticsearch, RequestError
import pytest
import pandas as pd

from intake_elasticsearch import Plugin, ElasticSearchSource
from .util import verify_plugin_interface, verify_datasource_interface


CONNECT = {'host': 'localhost', 'port': 9200}
TEST_DATA_DIR = 'tests'
TEST_DATA = 'sample1.csv'
df = pd.read_csv(os.path.join(TEST_DATA_DIR, TEST_DATA))


# TODO: this is the ElasticSearch test suite


@pytest.fixture(scope='module')
def engine():
    """Start docker container for ES and cleanup connection afterward."""
    from .util import start_es, stop_docker
    stop_docker('intake-es', let_fail=True)
    start_es()

    es = Elasticsearch([CONNECT])
    try:
        es.indices.create(index='intake_test')
    except RequestError:
        # index already existed - ignore
        pass
    for i, item in df.iterrows():
        es.index(index='intake_test', doc_type='entry', id=i,
                 body=item.to_dict())
    try:
        yield
    finally:
        stop_docker('intake-es')


def test_es_plugin(engine):
    p = Plugin()
    assert isinstance(p.version, str)
    assert p.container == 'dataframe'
    verify_plugin_interface(p)


def test_open(engine):
    p = Plugin()
    d = p.open('score:[30 TO 150]', **CONNECT)
    assert d.container == 'dataframe'
    assert d.description is None
    verify_datasource_interface(d)


def test_discover(engine):
    p = Plugin()
    source = p.open('score:[30 TO 150]', **CONNECT)
    info = source.discover()
    # NB: ES results come as dicts, so column order can vary
    assert info['dtype'].dtypes.to_dict() == df[:0].dtypes.to_dict()
    assert info['shape'] == (None, 3)
    assert info['npartitions'] == 1


def test_read(engine):
    p = Plugin()
    source = p.open('score:[0 TO 150]', **CONNECT)
    out = source.read()
    # this would be easier with a full query with sorting
    assert all([d in out.to_dict(orient='records')
               for d in df.to_dict(orient='records')])


def test_read_small_scroll(engine):
    p = Plugin()
    source = p.open('score:[0 TO 150]', scroll='5m', size=1,
                    **CONNECT)
    out = source.read()
    # this would be easier with a full query with sorting
    assert all([d in out.to_dict(orient='records')
               for d in df.to_dict(orient='records')])


def test_discover_after_read(engine):
    p = Plugin()
    source = p.open('score:[0 TO 150]', **CONNECT)
    info = source.discover()
    assert info['dtype'].dtypes.to_dict() == df[:0].dtypes.to_dict()
    assert info['shape'] == (None, 3)
    assert info['npartitions'] == 1

    out = source.read()
    assert all([d in out.to_dict(orient='records')
               for d in df.to_dict(orient='records')])

    info = source.discover()
    assert info['dtype'].dtypes.to_dict() == df[:0].dtypes.to_dict()
    assert info['shape'] == (4, 3)
    assert info['npartitions'] == 1


def test_read_chunked(engine):
    p = Plugin()
    # drop in a test of sort - only works on numerical field without work
    source = p.open('score:[0 TO 150]', qargs={
        "sort": 'rank'},
        **CONNECT)

    parts = list(source.read_chunked())
    out = pd.concat(parts)

    # with sort, comparison is simpler
    assert out[df.columns].equals(df)


def test_to_dask(engine):
    p = Plugin()
    source = p.open('score:[0 TO 150]', qargs={
        "sort": 'rank'},
        **CONNECT)

    dd = source.to_dask()
    assert dd.npartitions == 1
    assert set(dd.columns) == set(df.columns)
    out = dd.compute()

    assert out[df.columns].equals(df)


def test_close(engine):
    p = Plugin()
    source = p.open('score:[0 TO 150]', qargs={
        "sort": 'rank'},
        **CONNECT)

    source.close()
    # Can reopen after close
    out = source.read()

    assert out[df.columns].equals(df)


def test_pickle(engine):
    p = Plugin()
    source = p.open('score:[0 TO 150]', qargs={
        "sort": 'rank'},
        **CONNECT)

    pickled_source = pickle.dumps(source)
    source_clone = pickle.loads(pickled_source)

    out = source_clone.read()

    assert out[df.columns].equals(df)
