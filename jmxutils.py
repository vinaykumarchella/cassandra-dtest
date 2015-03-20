from urllib2 import urlopen
import json
import os
import subprocess

JOLOKIA_JAR = os.path.join('lib', 'jolokia-jvm-1.2.3-agent.jar')


def make_mbean(package, mbean):
    return 'org.apache.cassandra.%s:type=%s' % (package, mbean)


class JolokiaAgent(object):

    node = None

    def __init__(self, node):
        self.node = node

    def start(self):
        args = ('java',
                '-jar', JOLOKIA_JAR,
                '--host', self.node.network_interfaces['binary'][0],
                'start', str(self.node.pid))
        try:
            subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, exc:
            print "Failed to start jolokia agent (command was: %s): %s" % (' '.join(args), exc)
            print "Exit status was: %d" % (exc.returncode,)
            print "Output was: %s" % (exc.output,)
            raise

    def stop(self):
        args = ('java',
                '-jar', JOLOKIA_JAR,
                'stop', str(self.node.pid))
        try:
            subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, exc:
            print "Failed to stop jolokia agent (command was: %s): %s" % (' '.join(args), exc)
            print "Exit status was: %d" % (exc.returncode,)
            print "Output was: %s" % (exc.output,)
            raise

    def _query(self, body):
        request_data = json.dumps(body)
        url = 'http://%s:8778/jolokia/' % (self.node.network_interfaces['binary'][0],)
        response = urlopen(url, data=request_data, timeout=10.0)
        if response.code != 200:
            raise Exception("Failed to query Jolokia agent; HTTP response code: %d; response: %s" % (response.code, response.readlines()))

        raw_response = response.readline()
        response = json.loads(raw_response)
        if response['status'] != 200:
            raise Exception("Jolokia agent returned non-200 status: %s" % (response,))
        return response

    def read_attribute(self, mbean, attribute, path=None):
        body = {'type': 'read',
                'mbean': mbean,
                'attribute': attribute}
        if path:
            body['path'] = path
        response = self._query(body)
        return response['value']

    def write_attribute(self, mbean, attribute, value, path=None):
        body = {'type': 'write',
                'mbean': mbean,
                'attribute': attribute,
                'value': value}
        if path:
            body['path'] = path
        self._query(body)

    def execute_method(self, mbean, operation, arguments=None):
        if arguments is None:
            arguments = []

        body = {'type': 'exec',
                'mbean': mbean,
                'operation': operation,
                'arguments': arguments}

        response = self._query(body)
        return response['value']

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        self.stop()
        return exc_type is None
