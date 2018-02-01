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

    cmd = shlex.split('docker run -d -p 9200:9200 -p 9300:9300 -e '
                      '"discovery.type=single-node" --name intake-es '
                      '-e "http.host=0.0.0.0"  -e "transport.host=127.0.0.1" '
                      '-e "xpack.security.enabled=false" '
                      'docker.elastic.co/elasticsearch/elasticsearch:6.1.1')
    subprocess.check_call(cmd)   # the return value is actually the container ID

    while True:
        try:
            r = requests.get('http://localhost:9200')
            r.json()
            break
        except:
            pass


def stop_docker(name, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        print('Stopping %s ...' % name)
        cmd = shlex.split('docker ps -q --filter "name=%s"' % name)
        cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            subprocess.call(['docker', 'kill', cid])
            subprocess.call(['docker', 'rm', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise
