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

import base64
import eventlet
import functools
import httplib
import itertools
import logging
import pickle
import socket
import urlparse

from scality_sproxyd_client import exceptions
from scality_sproxyd_client import utils

urllib3 = utils.get_urllib3()


def drain_connection(response):
    '''Read remaining data of the `Response` to 'clean' underlying socket.'''
    while response.read(64 * 1024):
        pass


class SproxydClient(object):
    """A Sproxyd client with connection pooling and dead nodes detection."""

    DEFAULT_LOGGER = logging.getLogger(__name__)

    def __init__(self, endpoints,
                 url_cert_bundle=None, url_client_cert=None, url_client_key=None,
                 url_username=None, url_password=None,
                 conn_timeout=10.0, read_timeout=3.0, logger=None):
        '''Construct an `sproxyd` client

        This client connects in a round-robin fashion to online Sproxyd
        connectors provided in the `endpoints` list.

        If `conn_timeout`, `read_timeout` or `logger` are not provided, a
        default value will be used.

        :param endpoints: URLs where Sproxyd connectors accept 'by path' queries
        :type endpoints: iterable of `str` or `urlparse.ParseResult`
        :param conn_timeout: Connection timeout (to establish connection)
        :type conn_timeout: `float`
        :param read_timeout: Read timeout (for the `recv()` on the socket)
        :type read_timeout: `float`
        :param logger: Logger used by methods on the instance
        :type logger: `logging.Logger`
        '''

        self._url_username = url_username
        self._url_password = url_password

        self._conn_timeout = conn_timeout
        self._read_timeout = read_timeout

        if logger:
            self._logger = logger
        else:
            self._logger = self.DEFAULT_LOGGER

        # Must be here, `__del__` relies on it
        self._healthcheck_threads = set()

        self._endpoints = frozenset(
            endpoint if not isinstance(endpoint, basestring)
            else urlparse.urlparse(endpoint)
            for endpoint in endpoints if endpoint)

        if not self._endpoints:
            raise ValueError("At least one Sproxyd endpoint "
                             "must be provided")

        for endpoint in self._endpoints:
            if endpoint.scheme not in ['http', 'https']:
                raise ValueError(
                    'Unknown endpoint scheme: %r in %r' %
                    (endpoint.scheme, endpoint.geturl()))

            for attr in ['params', 'query', 'fragment']:
                if getattr(endpoint, attr):
                    raise ValueError(
                        'Endpoint with %s not supported: %r' %
                        (attr, endpoint.geturl()))

        cert_reqs = None
        if url_cert_bundle:
            cert_reqs = 'CERT_REQUIRED'

        self._pool_manager = urllib3.PoolManager(
            len(self._endpoints),
            cert_reqs=cert_reqs, ca_certs=url_cert_bundle,
            cert_file=url_client_cert, key_file=url_client_key,
            retries=False, maxsize=32, timeout=urllib3.Timeout(
                connect=conn_timeout, read=self._read_timeout))

        self._alive = frozenset(self._endpoints)
        self._cycle = itertools.cycle(self._alive)

        for endpoint in self._endpoints:
            url = '%s/.conf' % endpoint.geturl().rstrip('/')

            ping = functools.partial(self._ping, url)
            on_up = functools.partial(self._on_sproxyd_up, endpoint)
            on_down = functools.partial(self._on_sproxyd_down, endpoint)

            thread = eventlet.spawn(utils.monitoring_loop, ping, on_up, on_down)
            self._healthcheck_threads.add(thread)

    def __repr__(self):
        return 'SproxydClient(%s)' % ', '.join('%s=%r' % attr for attr in [
            ('endpoints', self._endpoints),
            ('conn_timeout', self._conn_timeout),
            ('read_timeout', self._read_timeout),
            ('logger', self._logger
                if self._logger is not self.DEFAULT_LOGGER else None),
        ])

    def get_next_endpoint(self):
        return self._cycle.next()

    def _ping(self, url):
        """Retrieves the Sproxyd active configuration for health checking."""
        try:
            headers = None
            timeout = urllib3.Timeout(1)
            if self._url_username and self._url_password:
                creds_str = ('%s:%s' % (self._url_username, self._url_password))
                headers = urllib3.util.make_headers(basic_auth=creds_str)
            conf = self._pool_manager.request('GET', url, headers=headers, timeout=timeout)
            return utils.is_sproxyd_conf_valid(conf.data)
        except (IOError, urllib3.exceptions.HTTPError) as exc:
            self._logger.info("Could not read Sproxyd configuration at %s "
                              "due to a network error: %r", url, exc)
        except exceptions.SproxydConfException as exc:
            self._logger.warning("Sproxyd configuration at %s is invalid: "
                                 "%s", url, exc)
        except:
            self._logger.exception("Unexpected exception during Sproxyd "
                                   "health check of %s", url)

        return False

    def _alter_alive(self, fn):
        self._alive = fn(self._alive)
        self._cycle = itertools.cycle(self._alive)

    def _on_sproxyd_up(self, endpoint):
        self._logger.info("Sproxyd connector at %s is up", endpoint)
        self._alter_alive(lambda s: s.union([endpoint]))
        self._logger.debug('Alive set is now: %r', self._alive)

    def _on_sproxyd_down(self, endpoint):
        self._logger.warning("Sproxyd connector at %s is down " +
                             "or misconfigured", endpoint)
        self._alter_alive(lambda s: s.difference([endpoint]))
        self._logger.debug('Alive set is now: %r', self._alive)

    def __del__(self):
        for thread in self._healthcheck_threads:
            try:
                thread.kill()
            except:
                msg = "Exception while killing healthcheck thread"
                self._logger.exception(msg)

    @property
    def has_alive_endpoints(self):
        '''Determine whether any client endpoints are alive

        :return: Client has alive endpoints
        :rtype: `bool`
        '''

        return len(self._alive) > 0

    @property
    def conn_timeout(self):
        '''Get the connection timeout (to establish connection).'''
        return self._conn_timeout

    @property
    def read_timeout(self):
        '''Get the read timeout (recv() on the socket).'''
        return self._read_timeout

    def get_url_for_object(self, name):
        '''Get an absolute URL from which the object `name` can be accessed.

        Only healthy Sproxyd endpoints are concidered to form the base object
        path. The object `name` is expected to have been properly encoded by
        the caller and must contain only URL-safe characters.
        '''

        try:
            endpoint = self.get_next_endpoint()
        except StopIteration:
            raise exceptions.SproxydException("No Sproxyd endpoint alive")

        return '%s/%s' % (endpoint.geturl().rstrip('/'), name)

    def _do_http(self, caller_name, handlers, method, path, headers=None,
                 body=None):
        '''Common code for handling a single HTTP request

        Handler functions passed through `handlers` will be called with the HTTP
        response object.

        :param caller_name: Name of the caller function, used in exceptions
        :type caller_name: `str`
        :param handlers: Dictionary mapping HTTP response codes to handlers
        :type handlers: `dict` of `int` to `callable`
        :param method: HTTP request method
        :type method: `str`
        :param path: HTTP request path
        :type path: `str`
        :param headers: HTTP request headers
        :type headers: `dict` of `str` to `str`
        :param body: HTTP request body
        :type body: `str` or `object` with a `read(blocksize)` method

        :raises SproxydHTTPException: Received an unhandled HTTP response
        '''

        full_url = self.get_url_for_object(path)

        def unexpected_http_status(response):
            message = response.read()

            raise exceptions.SproxydHTTPException(
                '%s: %s' % (caller_name, message),
                url=full_url,
                http_status=response.status,
                http_reason=response.reason)

        if self._url_username and self._url_password:
            creds_str = ('%s:%s' % (self._url_username, self._url_password))
            if headers is None:
                headers = {}
            headers.update(urllib3.util.make_headers(basic_auth=creds_str))
        response = self._pool_manager.request(
            method, full_url, headers=headers, body=body, preload_content=False)
        handler = handlers.get(response.status, unexpected_http_status)
        result, should_release_conn = handler(response)

        # If the handler says it's safe to release the connection now
        if should_release_conn:
            try:
                drain_connection(response)
                response.release_conn()
            except Exception as exc:
                self._logger.error("Unexpected exception while releasing an "
                                   "HTTP connection after request to %s: %r",
                                   full_url, exc)

        return result

    def head(self, name, headers=None):
        """Performs a HEAD request and returns the HTTP headers."""

        handlers = {
            200: lambda response: (response.headers, True)
        }

        return self._do_http('head', handlers, 'HEAD', name, headers)

    def get_meta(self, name):
        """Get the user metadata for object `name`."""

        try:
            usermd = self.head(name)['x-scal-usermd']
        except exceptions.SproxydHTTPException as exc:
            if exc.http_status == 404:
                return None
            raise

        pickled = base64.b64decode(usermd)
        return pickle.loads(pickled)

    def put_meta(self, name, metadata):
        """Connect to sproxyd and put usermd."""
        if metadata is None:
            raise exceptions.SproxydHTTPException("no usermd")

        headers = {
            'x-scal-cmd': 'update-usermd',
            'x-scal-usermd': base64.b64encode(pickle.dumps(metadata)),
        }

        handlers = {
            200: lambda _: (None, True),
        }

        result = self._do_http('put_meta', handlers, 'PUT', name, headers)

        self._logger.debug("Metadata stored for %s : %s", name, metadata)

        return result

    def del_object(self, name, headers=None):
        """Connect to sproxyd and delete object."""

        handlers = {
            200: lambda _: (None, True),
        }

        return self._do_http('del_object', handlers, 'DELETE', name, headers)

    def get_object(self, name, headers=None):
        """Connect to sproxyd and returns an object and its properties."""

        def handle_200_or_206(response):
            def gen():
                for chunk in response.stream(amt=1024 * 64):
                    yield chunk
                response.release_conn()
            return (response.headers, gen()), False

        handlers = {
            200: handle_200_or_206,
            206: handle_200_or_206
        }

        return self._do_http('get_object', handlers, 'GET', name, headers)

    def put_object(self, name, body, headers=None):
        """Connect to sproxyd and put an object."""

        # The Content-Length is automatically added to the headers and
        # set to the correct value. See `httplib.HTTPConnection.request`

        handlers = {
            200: lambda _: (None, True),
        }

        return self._do_http('put_object', handlers, 'PUT', name,
                             headers=headers, body=body)

    def get_http_conn_for_put(self, name, headers=None):
        full_url = self.get_url_for_object(name)
        url_pieces = urlparse.urlparse(full_url)
        conn_pool = self._pool_manager.connection_from_host(url_pieces.hostname,
                                                            url_pieces.port,
                                                            url_pieces.scheme)
        timeout_obj = conn_pool.timeout.clone()

        conn = None
        try:
            # Request a connection from the queue.
            conn = conn_pool._get_conn()
            conn_pool.num_requests += 1

            conn.timeout = timeout_obj.connect_timeout

            conn.putrequest('PUT', url_pieces.path,
                            skip_host=(headers and 'Host' in headers))
            if headers:
                for header, value in headers.iteritems():
                    conn.putheader(header, str(value))

            if self._url_username and self._url_password:
                creds_str = ('%s:%s' % (self._url_username, self._url_password))
                basic_auth_header = urllib3.util.make_headers(basic_auth=creds_str)
                conn.putheader(basic_auth_header.keys()[0],
                               basic_auth_header.values()[0])
            conn.endheaders()

        except (httplib.HTTPException, socket.error,
                urllib3.exceptions.TimeoutError) as ex:
            if conn:
                # Discard the connection for these exceptions. It will be
                # be replaced during the next _get_conn() call.
                conn.close()
                conn = None
            msg = "Error when trying to PUT %s : %r" % (full_url, ex)
            self._logger.info(msg)
            raise exceptions.SproxydException(msg)

        def release_conn():
            conn_pool._put_conn(conn)

        return (conn, release_conn)
