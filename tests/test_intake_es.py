import os
import pickle
import time

from elasticsearch import Elasticsearch, RequestError
import pytest
import pandas as pd

from intake_elasticsearch import (ElasticSearchTableSource,
                                  ElasticSearchSeqSource)
from .util import verify_datasource_interface


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
    cid = start_es()

    try:
        es = Elasticsearch([CONNECT])
        try:
            es.indices.create(index='intake_test')
        except RequestError:
            # index already existed - ignore
            pass
        for i, item in df.iterrows():
            es.index(index='intake_test', doc_type='entry', id=i,
                     body=item.to_dict())
        time.sleep(1)
        yield
    finally:
        stop_docker('intake-es', cid=cid)


def test_open(engine):
    d = ElasticSearchTableSource('score:[30 TO 150]', **CONNECT)
    assert d.container == 'dataframe'
    assert d.description is None
    verify_datasource_interface(d)


def test_discover(engine):
    source = ElasticSearchTableSource('score:[30 TO 150]', **CONNECT)
    info = source.discover()
    # NB: ES results come as dicts, so column order can vary
    assert info['dtype'] == {k: str(v) for k, v
                             in df[:0].dtypes.to_dict().items()}
    assert info['shape'] == (None, 3)
    assert info['npartitions'] == 1


def test_read(engine):
    source = ElasticSearchTableSource('score:[0 TO 150]', **CONNECT)
    out = source.read()
    # this would be easier with a full query with sorting
    assert all([d in out.to_dict(orient='records')
               for d in df.to_dict(orient='records')])


def test_read_sequence(engine):
    source = ElasticSearchSeqSource('score:[0 TO 150]', **CONNECT)
    out = source.read()
    assert all([d in out for d in df.to_dict(orient='records')])


def test_read_small_scroll(engine):
    source = ElasticSearchSeqSource('score:[0 TO 150]', scroll='5m', size=1,
                                    **CONNECT)
    out = source.read()
    # this would be easier with a full query with sorting
    assert all([d in out
               for d in df.to_dict(orient='records')])


def test_discover_after_read(engine):
    source = ElasticSearchTableSource('score:[0 TO 150]', **CONNECT)
    info = source.discover()
    dt = {k: str(v) for k, v in df.dtypes.to_dict().items()}
    assert info['dtype'] == dt
    assert info['shape'] == (None, 3)
    assert info['npartitions'] == 1

    out = source.read()
    assert all([d in out.to_dict(orient='records')
               for d in df.to_dict(orient='records')])

    info = source.discover()
    assert info['dtype'] == dt
    assert info['shape'] == (4, 3)
    assert info['npartitions'] == 1


def test_to_dask(engine):
    source = ElasticSearchTableSource('score:[0 TO 150]', qargs={
                                      "sort": 'rank'},
                                      **CONNECT)

    dd = source.to_dask()
    assert dd.npartitions == 1
    assert set(dd.columns) == set(df.columns)
    out = dd.compute()

    assert out[df.columns].equals(df)


def test_to_dask_with_partitions(engine):
    source = ElasticSearchTableSource('score:[0 TO 150]', qargs={
                                      "sort": 'rank'},
                                      **CONNECT)
    dd = source.to_dask(npartitions=2)
    assert dd.npartitions == 2
    assert set(dd.columns) == set(df.columns)

    out = dd.compute()

    assert len(out) == len(df)
    assert all([d in out.to_dict(orient='records')
               for d in df.to_dict(orient='records')])


def test_close(engine):
    source = ElasticSearchTableSource('score:[0 TO 150]', qargs={
        "sort": 'rank'},
        **CONNECT)

    source.close()
    # Can reopen after close
    out = source.read()

    assert out[df.columns].equals(df)


def test_pickle(engine):
    source = ElasticSearchTableSource('score:[0 TO 150]', qargs={
        "sort": 'rank'},
        **CONNECT)

    pickled_source = pickle.dumps(source)
    source_clone = pickle.loads(pickled_source)

    out = source_clone.read()

    assert out[df.columns].equals(df)
