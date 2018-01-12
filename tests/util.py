import shlex
import subprocess
import requests


def verify_plugin_interface(plugin):
    """Assert types of plugin attributes."""
    assert isinstance(plugin.version, str)
    assert isinstance(plugin.container, str)
    assert isinstance(plugin.partition_access, bool)


def verify_datasource_interface(source):
    """Assert presence of datasource attributes."""
    for attr in ['container', 'description', 'datashape', 'dtype', 'shape',
                 'npartitions', 'metadata']:
        assert hasattr(source, attr)

    for method in ['discover', 'read', 'read_chunked', 'read_partition',
                   'to_dask', 'close']:
        assert hasattr(source, method)


def start_es():
    """Bring up a container running ES.

    Waits until REST API is live and responsive.
    """
    print('Starting ES server...')

    # More options here: https://github.com/appropriate/docker-postgis
    cmd = shlex.split('docker run -d -p 9200:9200 -p 9300:9300 -e '
                      '"discovery.type=single-node" --name intake-es '
                      'docker.elastic.co/elasticsearch/elasticsearch:6.1.1')
    subprocess.check_call(cmd)

    while True:
        try:
            r = requests.get('http://localhost:9200')
            r.json()
            break
        except:
            pass


def stop_es(let_fail=False):
    """Attempt to shut down the container started by ``start_es()``

    Raise an exception if this operation fails, unless ``let_fail``
    evaluates to True.
    """
    try:
        print('Stopping ES server...')
        subprocess.check_call('docker ps -q --filter "name=intake-es" | '
                              'xargs docker kill -vf', shell=True)
    except subprocess.CalledProcessError:
        if not let_fail:
            raise
