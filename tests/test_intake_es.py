import os
import pickle

from elasticsearch import Elasticsearch, RequestError
import pytest
import pandas as pd
import time

from intake_elasticsearch.elasticsearch_table import ElasticSearchSource, Plugin
from .util import verify_plugin_interface, verify_datasource_interface


CONNECT = {'host': 'localhost', 'port': 9200}
TEST_DATA_DIR = 'tests'
TEST_DATA = 'sample1.csv'
df = pd.read_csv(os.path.join(TEST_DATA_DIR, TEST_DATA))


# TODO: this is the ElasticSearch test suite


@pytest.fixture(scope='module')
def engine():
    """Start docker container for ES and cleanup connection afterward."""
    from .util import start_es, stop_es
    stop_es(let_fail=True)
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
        stop_es()


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


# @pytest.mark.skip('Not implemented yet')
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_read_chunked(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = postgres.Plugin()
#     source = p.open(DB_URI, 'select * from '+table_name)
#
#     parts = list(source.read_chunked())
#     df = pd.concat(parts)
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.skip('Partition support not planned')
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_read_partition(engine, table_name, csv_fpath):
#     expected_df1 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#     expected_df2 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = postgres.Plugin()
#     source = p.open(DB_URI, 'select * from '+table_name)
#
#     source.discover()
#     assert source.npartitions == 2
#
#     # Read partitions is opposite order
#     df2 = source.read_partition(1)
#     df1 = source.read_partition(0)
#
#     assert expected_df1.equals(df1)
#     assert expected_df2.equals(df2)
#
#
# @pytest.mark.skip('Not implemented yet')
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_to_dask(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = postgres.Plugin()
#     source = p.open(DB_URI, 'select * from '+table_name)
#
#     dd = source.to_dask()
#     df = dd.compute()
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_close(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = postgres.Plugin()
#     source = p.open(DB_URI, 'select * from '+table_name)
#
#     source.close()
#     # Can reopen after close
#     df = source.read()
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_pickle(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = postgres.Plugin()
#     source = p.open(DB_URI, 'select * from '+table_name)
#
#     pickled_source = pickle.dumps(source)
#     source_clone = pickle.loads(pickled_source)
#
#     expected_df = source.read()
#     df = source_clone.read()
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.parametrize('table_name,_1', TEST_DATA)
# def test_catalog(engine, table_name, _1):
#     catalog_fpath = os.path.join(TEST_DATA_DIR, 'catalog1.yml')
#
#     catalog = Catalog(catalog_fpath)
#     ds_name = table_name.rsplit('_idx', 1)[0]
#     src = catalog[ds_name]
#     pgsrc = src.get()
#
#     assert src.describe()['container'] == 'dataframe'
#     assert src.describe_open()['plugin'] == 'postgres'
#     assert src.describe_open()['args']['sql_expr'][:6] in ('select', 'SELECT')
#
#     metadata = pgsrc.discover()
#     assert metadata['npartitions'] == 1
#
#     expected_df = pd.read_sql_query(pgsrc._sql_expr, engine)
#     df = pgsrc.read()
#     assert expected_df.equals(df)
#
#     pgsrc.close()
#
#
# def test_catalog_join(engine):
#     catalog_fpath = os.path.join(TEST_DATA_DIR, 'catalog1.yml')
#
#     catalog = Catalog(catalog_fpath)
#     ds_name = 'sample2'
#     src = catalog[ds_name]
#     pgsrc = src.get()
#
#     assert src.describe()['container'] == 'dataframe'
#     assert src.describe_open()['plugin'] == 'postgres'
#     assert src.describe_open()['args']['sql_expr'][:6] in ('select', 'SELECT')
#
#     metadata = pgsrc.discover()
#     assert metadata['npartitions'] == 1
#
#     expected_df = pd.read_sql_query(pgsrc._sql_expr, engine)
#     df = pgsrc.read()
#     assert expected_df.equals(df)
#
#     pgsrc.close()
