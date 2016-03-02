# Copyright (c) 2015 Scality
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for Sproxyd Client"""

import base64
import itertools
import pickle
import socket
import unittest
import urlparse
import weakref

import eventlet
import mock
import urllib3
import urllib3.exceptions

from scality_sproxyd_client.sproxyd_client import SproxydClient
from scality_sproxyd_client.exceptions import SproxydConfException, \
    SproxydException, SproxydHTTPException
from . import utils


class TestSproxydClient(unittest.TestCase):
    """Tests for scality_sproxyd_client.sproxyd_client.SproxydClient"""

    def test_init_with_no_endpoint(self):
        self.assertRaises(ValueError, SproxydClient, [])

    def test_init_with_invalid_endpoint(self):
        self.assertRaises(ValueError, SproxydClient, ['invalid://'])

    def test_init_with_empty_endpoint(self):
        self.assertRaises(ValueError, SproxydClient, [''])

    def test_init_with_some_empty_endpoints(self):
        sproxyd_client = SproxydClient(['', 'http://host:81/path/', ''])
        self.assertEqual(
            frozenset([urlparse.urlparse('http://host:81/path/')]),
            sproxyd_client._endpoints)

    def test_init_with_endpoint_with_params(self):
        utils.assertRaisesRegexp(ValueError, '.* params .*',
                                 SproxydClient, ['http://host:81/path;param'])

    def test_init_with_endpoint_with_query(self):
        utils.assertRaisesRegexp(ValueError, '.* query .*',
                                 SproxydClient, ['http://host:81/path?q='])

    def test_init_with_endpoint_with_fragment(self):
        utils.assertRaisesRegexp(ValueError, '.* fragment .*',
                                 SproxydClient, ['http://host:81/path#frag'])

    @mock.patch('eventlet.spawn')
    def test_init_with_default_timeout_values(self, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        self.assertEqual(10, sproxyd_client.conn_timeout)
        self.assertEqual(3, sproxyd_client.read_timeout)

    @mock.patch('eventlet.spawn')
    def test_init_with_custom_timeout_values(self, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'],
                                       conn_timeout=42.1, read_timeout=4242.1)
        self.assertEqual(42.1, sproxyd_client.conn_timeout)
        self.assertEqual(4242.1, sproxyd_client.read_timeout)

    @mock.patch('eventlet.spawn')
    def test_init_sproxyd_hosts(self, _):
        hosts = set(['http://host1:81/path1/', 'http://host2:82/path2/'])
        sproxyd_client = SproxydClient(hosts)
        self.assertEqual(
            frozenset(urlparse.urlparse(host) for host in hosts),
            sproxyd_client._endpoints)

    @mock.patch('eventlet.spawn')
    def test_init_sproxyd_hosts_with_iterator(self, _):
        hosts = ['http://h1:81/path1/', 'http://h2:82/path2/']
        sproxyd_client = SproxydClient(iter(hosts))
        self.assertEqual(
            frozenset(urlparse.urlparse(host) for host in hosts),
            sproxyd_client._endpoints)

    # In this test, `eventlet.spawn` must be mocked with a specific side-effect,
    # because in the default case its result value would always be the same
    # `MagicMock`, and as a result the cardinality of the `_healthcheck_threads`
    # set would be 1 instead of the expected 2.
    @mock.patch('eventlet.spawn', side_effect=lambda *_: mock.MagicMock())
    def test_init_monitoring_threads(self, _):
        hosts = ['http://host1:81/path/', 'http://host2:82/path/']
        sproxyd_client = SproxydClient(hosts)
        self.assertEqual(2, len(sproxyd_client._healthcheck_threads))

    @mock.patch('eventlet.spawn', mock.Mock())
    @mock.patch('functools.partial')
    def test_init_calls_ping_with_correct_url(self, mock_partial):
        # No slash, 1 slash, 2 slashes
        hosts = ['http://h1/p1', 'http://h2/p2/', 'http://h3/p3//']
        SproxydClient(hosts)
        self.assertTrue(mock.call(mock.ANY, 'http://h1/p1/.conf') in
                        mock_partial.call_args_list)
        self.assertTrue(mock.call(mock.ANY, 'http://h2/p2/.conf') in
                        mock_partial.call_args_list)
        self.assertTrue(mock.call(mock.ANY, 'http://h3/p3/.conf') in
                        mock_partial.call_args_list)

    @mock.patch('eventlet.spawn', mock.Mock())
    @mock.patch('urllib3.PoolManager.request',
                return_value=mock.Mock(data="by_path_enabled=True"))
    def test_ping_with_valid_sproxyd_conf(self, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        self.assertTrue(sproxyd_client._ping('http://ignored'))

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request',
                side_effect=SproxydConfException(""))
    def test_ping_with_bad_sproxyd_conf(self, request_mock, _):
        mock_logger = mock.Mock()
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock_logger)
        ping_result = sproxyd_client._ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.warning.called)
        (msg, _, exc), _ = mock_logger.warning.call_args
        self.assertTrue(type(exc) is SproxydConfException)
        self.assertTrue("is invalid:" in msg)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request', side_effect=Exception)
    def test_ping_with_unexpected_exc(self, urlopen_mock, _):
        mock_logger = mock.Mock()
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=mock_logger)
        ping_result = sproxyd_client._ping('http://ignored')

        self.assertFalse(ping_result)
        self.assertTrue(mock_logger.exception.called)
        (msg, _), _ = mock_logger.exception.call_args
        self.assertTrue("Unexpected" in msg)

    @mock.patch('eventlet.spawn')
    def test_on_sproxyd_up(self, _):
        sproxyd_client = SproxydClient(['http://host1:81/path/'])
        sproxyd_url_2 = urlparse.urlparse('http://host2:81/path/')
        sproxyd_client._on_sproxyd_up(sproxyd_url_2)
        self.assertTrue(sproxyd_url_2 in sproxyd_client._alive)
        self.assertTrue(sproxyd_url_2 in itertools.islice(sproxyd_client._cycle, 2))
        self.assertTrue(sproxyd_client.has_alive_endpoints)

    @mock.patch('eventlet.spawn')
    def test_on_sproxyd_down(self, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        sproxyd_client._on_sproxyd_down(urlparse.urlparse('http://host:81/path/'))
        self.assertFalse('http://host:81/path/' in sproxyd_client._alive)
        self.assertEqual([], list(sproxyd_client._cycle))
        self.assertFalse(sproxyd_client.has_alive_endpoints)

    @mock.patch('eventlet.spawn', mock.Mock())
    def test_get_url_for_object(self):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        self.assertEqual('http://host:81/path/ob%20j',
                         sproxyd_client.get_url_for_object("ob%20j"))

    @mock.patch('eventlet.spawn', mock.Mock())
    def test_get_url_for_object_wrt_slashes(self):
        # No slash, 1 slash, 2 slashes
        hosts = ['http://h1/p', 'http://h2/p/', 'http://h3/p//']
        sproxyd_client = SproxydClient(hosts)
        for _ in range(len(hosts)):
            # The slash in the object name must not be swallowed nor quoted
            url = sproxyd_client.get_url_for_object("/ob%20j")
            self.assertTrue(url.endswith('/p//ob%20j'))

    @mock.patch('eventlet.spawn', mock.Mock())
    def test_get_url_for_object_no_endpoint_alive(self):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        sproxyd_client._on_sproxyd_down(urlparse.urlparse('http://host:81/path/'))
        self.assertRaises(SproxydException,
                          sproxyd_client.get_url_for_object, "")

    @mock.patch('eventlet.spawn', mock.Mock())
    @mock.patch('socket.socket.connect', side_effect=socket.timeout)
    def test_do_http_connection_timeout(self, mock_http_connect):
        timeout = 0.01
        # This need to be a valid host name otherwise the test fails very early
        # with gaierror(-5, 'No address associated with hostname')
        sproxyd_client = SproxydClient(['http://localhost:81/p/'], conn_timeout=timeout)

        regex = r'^.*connect timeout=%s.*$' % timeout
        utils.assertRaisesRegexp(urllib3.exceptions.ConnectTimeoutError, regex,
                                 sproxyd_client._do_http, 'me', {},
                                 'HTTP_METH', '/')

    def test_do_http_timeout(self):
        server1 = eventlet.listen(('127.0.0.1', 0))
        (ip, port) = server1.getsockname()

        def run_server1(sock):
            (client, addr) = sock.accept()
            eventlet.sleep(0.1)

        t = eventlet.spawn(run_server1, server1)
        timeout = 0.01
        with mock.patch('eventlet.spawn'):
            sproxyd_client = SproxydClient(['http://%s:%d/path' % (ip, port)],
                                           read_timeout=timeout)

        regex = r'^.*read timeout=%s.*$' % timeout
        utils.assertRaisesRegexp(urllib3.exceptions.ReadTimeoutError, regex,
                                 sproxyd_client._do_http, 'me', {},
                                 'HTTP_METH', '/')
        t.kill()

    @mock.patch('eventlet.spawn')
    def test_do_http_unexpected_http_status(self, _):
        mock_response = mock.Mock()
        mock_response.status = 500
        mock_response.read.return_value = 'error'

        sproxyd_client = SproxydClient(['http://host:81/path/'])
        msg = r'^caller1: %s .*' % mock_response.read.return_value
        with mock.patch('urllib3.PoolManager.request', return_value=mock_response):
            utils.assertRaisesRegexp(SproxydHTTPException, msg, sproxyd_client._do_http,
                                     'caller1', {}, 'HTTP_METH', '/')

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_do_http(self, mock_http, _):
        mock_handler = mock.Mock(return_value=(None, True))

        sproxyd_client = SproxydClient(['http://host:81/'])
        method = 'HTTP_METH'
        # Note the white space, to test the client doesn't tamper with the URL
        path = 'pa th'
        headers = {'k': 'v'}
        sproxyd_client._do_http('caller1', {200: mock_handler},
                                method, path, headers)

        mock_http.assert_called_once_with(method,
                                          'http://host:81/' + path,
                                          headers=headers, body=None,
                                          preload_content=False)
        mock_handler.assert_called_once_with(mock_http.return_value)

    @mock.patch('eventlet.spawn')
    def test_do_http_drains_connection(self, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        mock_response = mock.Mock()
        mock_response.status = 200
        mock_response.read.side_effect = ['blah', 'blah', '']

        handlers = {200: lambda response: (None, True)}
        with mock.patch('urllib3.PoolManager.request', return_value=mock_response):
            sproxyd_client._do_http('caller1', handlers, 'METHOD', '/')

        self.assertEqual(3, mock_response.read.call_count)
        mock_response.release_conn.assert_called_once_with()

    @mock.patch('eventlet.spawn', mock.Mock())
    @mock.patch('scality_sproxyd_client.sproxyd_client.drain_connection')
    def test_do_http_dont_drain_connection(self, mock_drain):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        mock_response = mock.Mock(status=200)

        handlers = {200: lambda response: (None, False)}
        with mock.patch('urllib3.PoolManager.request', return_value=mock_response):
            sproxyd_client._do_http('caller1', handlers, 'METHOD', '/')

        self.assertFalse(mock_drain.called)
        self.assertFalse(mock_response.release_conn.called)

    @mock.patch('eventlet.spawn', mock.Mock())
    def test_head_with_headers(self):
        sproxyd_client = SproxydClient(['http://host:81/path/'])

        headers = {'k': 'v'}
        mock_response = urllib3.response.HTTPResponse(status=200,
                                                      headers=headers)
        with mock.patch('urllib3.PoolManager.request',
                        return_value=mock_response) as mock_http:
            returned_headers = sproxyd_client.head('_', headers)

        mock_http.assert_called_once_with('HEAD', 'http://host:81/path/_',
                                          headers=headers, body=None,
                                          preload_content=False)
        self.assertEqual(headers, returned_headers)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request')
    def test_get_meta_on_200(self, mock_http, _):
        headers = {'x-scal-usermd': base64.b64encode(pickle.dumps('fake'))}
        mock_http.return_value = urllib3.response.HTTPResponse(status=200,
                                                               headers=headers)

        sproxyd_client = SproxydClient(['http://host:81/path/'])
        metadata = sproxyd_client.get_meta('obj_1')

        mock_http.assert_called_once_with('HEAD',
                                          'http://host:81/path/obj_1',
                                          headers=None, body=None,
                                          preload_content=False)
        self.assertEqual('fake', metadata)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request',
                return_value=urllib3.response.HTTPResponse(status=404))
    def test_get_meta_on_404(self, mock_http, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'])

        self.assertTrue(sproxyd_client.get_meta('object_name_1') is None)

    @mock.patch('eventlet.spawn', mock.Mock())
    @mock.patch('urllib3.PoolManager.request',
                return_value=urllib3.response.HTTPResponse(status=413))
    def test_get_meta_on_413(self, mock_http):
        sproxyd_client = SproxydClient(['http://host:81/path/'])

        self.assertRaises(SproxydHTTPException, sproxyd_client.get_meta, '_')

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_put_meta(self, mock_http, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'])
        sproxyd_client.put_meta('object_name_1', 'fake')

        self.assertEqual(1, mock_http.call_count)
        (method, path), kwargs = mock_http.call_args
        self.assertEqual('PUT', method)
        self.assertTrue('object_name_1' in path)
        headers = kwargs['headers']
        self.assertTrue('x-scal-cmd' in headers)
        self.assertEqual('update-usermd', headers['x-scal-cmd'])
        self.assertTrue('x-scal-usermd' in headers)
        self.assertTrue(len(headers['x-scal-usermd']) > 0)

    @mock.patch('eventlet.spawn')
    def test_put_meta_with_no_metadata(self, _):
        sproxyd_client = SproxydClient(['http://host:81/path/'])

        utils.assertRaisesRegexp(SproxydHTTPException, 'no usermd',
                                 sproxyd_client.put_meta, 'obj_1', None)

    @mock.patch('eventlet.spawn')
    @mock.patch('urllib3.PoolManager.request',
                return_value=urllib3.response.HTTPResponse(status=200))
    def test_del_object_with_headers(self, mock_http, _):
        headers = {'k': 'v'}

        sproxyd_client = SproxydClient(['http://host:81/path/'])
        sproxyd_client.del_object('obj_1', headers)

        mock_http.assert_called_once_with('DELETE',
                                          'http://host:81/path/obj_1',
                                          headers=headers, body=None,
                                          preload_content=False)

    def test_get_object(self):
        content = 'Hello, World!'

        def my_app(env, start_response):
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [content]

        with utils.WSGIServer(my_app) as server:
            with mock.patch('eventlet.spawn'):
                sproxyd_client = SproxydClient(['http://%s:%d/path'
                                                % (server.ip, server.port)])
            headers, obj = sproxyd_client.get_object('ignored')

            self.assertTrue('content-length' in headers)
            self.assertEqual(len(content), int(headers['content-length']))
            self.assertEqual(content, obj.next())
            # Assert that `obj` is an Iterable
            self.assertRaises(StopIteration, obj.next)

    def test_put_object_with_headers(self):
        obj_name = utils.get_random_unicode(10).encode('utf-8')
        obj_metadata = utils.get_random_unicode(128).encode('utf-8')
        obj_body = utils.get_random_unicode(1024).encode('utf-8')

        def my_app(env, start_response):
            self.assertEqual('PUT', env['REQUEST_METHOD'])
            self.assertEqual('/path/' + obj_name, env['PATH_INFO'])

            data = env['wsgi.input'].read()
            self.assertEqual(obj_body, data)

            self.assertEqual(obj_metadata, env['HTTP_X_SCAL_USERMD'])

            start_response('200 OK', [('Content-Type', 'text/plain')])
            return []

        with utils.WSGIServer(my_app) as server:
            with mock.patch('eventlet.spawn'):
                sproxyd_client = SproxydClient(['http://%s:%d/path'
                                                % (server.ip, server.port)])

            sproxyd_client.put_object(obj_name, obj_body,
                                      {'x-scal-usermd': obj_metadata})

    @mock.patch('urllib3.connectionpool.HTTPConnectionPool._put_conn')
    def test_get_http_conn_for_put(self, mock_put_conn):
        obj_name = utils.get_random_unicode(10).encode('utf-8')
        obj_metadata = utils.get_random_unicode(128).encode('utf-8')
        obj_body = utils.get_random_unicode(1024).encode('utf-8')

        def my_app(env, start_response):
            self.assertEqual('PUT', env['REQUEST_METHOD'])
            self.assertEqual('/path/' + obj_name, env['PATH_INFO'])

            data = env['wsgi.input'].read()
            self.assertEqual(obj_body, data)

            actual_metadata = pickle.loads(base64.b64decode(env['HTTP_X_SCAL_USERMD']))
            self.assertEqual(obj_metadata, actual_metadata)

            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [obj_body]

        with utils.WSGIServer(my_app) as server:
            with mock.patch('eventlet.spawn'):
                sproxyd_client = SproxydClient(['http://%s:%d/path'
                                                % (server.ip, server.port)])

            headers = {
                'x-scal-usermd': base64.b64encode(pickle.dumps(obj_metadata)),
                'Content-Length': str(len(obj_body))
            }

            conn, release_conn = sproxyd_client.get_http_conn_for_put(obj_name,
                                                                      headers)
            conn.send(obj_body)
            resp = conn.getresponse()
            self.assertEqual(obj_body, resp.read())

            release_conn()

        mock_put_conn.assert_called_once_with(conn)

    @mock.patch('eventlet.spawn', mock.Mock())
    @mock.patch('httplib.HTTPConnection.close')
    @mock.patch('socket.socket')
    def test_get_http_conn_for_put_connection_timeout(self, mock_socket, mock_close):
        timeout = 0.1
        mock_socket().connect.side_effect = socket.timeout

        # If we set 'localhost' here, the test fails on CentOS
        # I think it's because, on CentOS, localhost could resolve to
        # [::1, 127.0.0.1]
        sproxyd_client = SproxydClient(['http://127.0.0.1:81/path'],
                                       conn_timeout=timeout)

        regex = r'^.*connect timeout=%s.*$' % timeout
        utils.assertRaisesRegexp(SproxydException, regex,
                                 sproxyd_client.get_http_conn_for_put, '_')
        mock_socket().settimeout.assert_called_once_with(timeout)
        mock_socket().connect.assert_called_once_with(('127.0.0.1', 81))
        mock_close.assert_called_once_with()

    @mock.patch('eventlet.spawn')
    def test_del_instance(self, mock_spawn):
        sproxyd_client = SproxydClient(['http://host:81/path/'])

        # Reset mock to clear some references to bound methods
        # Otherwise reference count can never go to 0
        mock_spawn.reset_mock()

        ref = weakref.ref(sproxyd_client)
        del sproxyd_client
        if ref() is not None:
            self.skipTest("GC didn't collect our object yet")

        mock_spawn().kill.assert_called_once_with()


def test_ping_when_network_exception_is_raised():

    @mock.patch('eventlet.spawn')
    def assert_ping_failed(expected_exc, _):
        logger = mock.Mock()
        sproxyd_client = SproxydClient(['http://host:81/path/'], logger=logger)

        with mock.patch('urllib3.PoolManager.request', side_effect=expected_exc):
            ping_result = sproxyd_client._ping('http://ignored/')

            assert ping_result is False, ('Ping returned %r, '
                                          'not False' % ping_result)
            assert logger.info.called
            (msg, _, exc), _ = logger.info.call_args
            assert type(exc) is expected_exc
            assert "network error" in msg

    for exc in [IOError, urllib3.exceptions.HTTPError]:
        yield assert_ping_failed, exc
