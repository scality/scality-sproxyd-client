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
import itertools
import logging
import pickle
import types
import urllib

from scality_sproxyd_client import exceptions
from scality_sproxyd_client import utils

urllib3 = utils.get_urllib3()


def drain_connection(response):
    '''Read remaining data of the `Response` to 'clean' underlying socket.'''
    while response.read(64 * 1024):
        pass


class SproxydClient(object):
    """A sproxyd file system scheme."""

    logger = logging.getLogger(__name__)
    conn_timeout = 10.0
    proxy_timeout = 3.0

    def __init__(self, sproxyd_urls, conn_timeout=None, proxy_timeout=None,
                 logger=None):
        '''Construct an `sproxyd` client

        This client connects in a round-robin fashion to online Sproxyd
        connectors provided in the `sproxyd_urls` list.

        If `conn_timeout`, `proxy_timeout` or `logger` are not provided, a
        default value will be used.

        :param sproxyd_urls: URLs where Sproxyd connectors accept 'by path' queries
        :type sproxyd_urls: iterable of `str`
        :param conn_timeout: Connection timeout
        :type conn_timeout: `float`
        :param proxy_timeout: Proxy timeout
        :type proxy_timeout: `float`
        :param logger: Logger used by methods on the instance
        :type logger: `logging.Logger`
        '''

        if logger:
            self.logger = logger

        if conn_timeout is not None:
            self.conn_timeout = conn_timeout

        if proxy_timeout is not None:
            self.proxy_timeout = proxy_timeout

        self.healthcheck_threads = []
        self.sproxyd_urls_set = set()

        for sproxyd_url in sproxyd_urls:
            sproxyd_url = sproxyd_url.rstrip('/') + '/'
            self.sproxyd_urls_set.add(sproxyd_url)

            sproxyd_ping_url = sproxyd_url + '.conf'
            ping = functools.partial(self.ping, sproxyd_ping_url)
            on_up = functools.partial(self.on_sproxyd_up, sproxyd_url)
            on_down = functools.partial(self.on_sproxyd_down, sproxyd_url)
            thread = eventlet.spawn(utils.monitoring_loop, ping, on_up, on_down)
            self.healthcheck_threads.append(thread)

        sproxyd_urls_list = list(self.sproxyd_urls_set)
        self.sproxyd_urls = itertools.cycle(sproxyd_urls_list)

        timeout = urllib3.Timeout(connect=self.conn_timeout,
                                  read=self.proxy_timeout)
        # One HTTP Connection pool per sproxyd host
        self.http_pools = urllib3.PoolManager(len(self.sproxyd_urls_set),
                                              timeout=timeout, retries=False,
                                              maxsize=32)

    def ping(self, url):
        """Retrieves the Sproxyd active configuration for health checking."""
        try:
            timeout = urllib3.Timeout(1)
            conf = self.http_pools.request('GET', url, timeout=timeout)
            return utils.is_sproxyd_conf_valid(conf.data)
        except (IOError, urllib3.exceptions.HTTPError) as exc:
            self.logger.info("Could not read Sproxyd configuration at %s "
                             "due to a network error: %r", url, exc)
        except exceptions.SproxydConfException as exc:
            self.logger.warning("Sproxyd configuration at %s is invalid: "
                                "%s", url, exc)
        except:
            self.logger.exception("Unexpected exception during Sproxyd "
                                  "health check of %s", url)

        return False

    def on_sproxyd_up(self, sproxyd_url):
        self.logger.info("Sproxyd connector at %s is up", sproxyd_url)
        self.sproxyd_urls_set.add(sproxyd_url)
        self.sproxyd_urls = itertools.cycle(list(self.sproxyd_urls_set))
        self.logger.debug('sproxyd_urls_set is now: %r', self.sproxyd_urls_set)

    def on_sproxyd_down(self, sproxyd_url):
        self.logger.warning("Sproxyd connector at %s is down " +
                            "or misconfigured", sproxyd_url)
        self.sproxyd_urls_set.remove(sproxyd_url)
        self.sproxyd_urls = itertools.cycle(list(self.sproxyd_urls_set))
        self.logger.debug('sproxyd_urls_set is now: %r', self.sproxyd_urls_set)

    def __del__(self):
        for thread in self.healthcheck_threads:
            try:
                thread.kill()
            except:
                msg = "Exception while killing healthcheck thread"
                self.logger.exception(msg)

    def __repr__(self):
        ret = 'SproxydClient(sproxyd_urls=%r, conn_timeout=%r, ' + \
              'proxy_timeout=%r)'
        return ret % (self.sproxyd_urls_set, self.conn_timeout,
                      self.proxy_timeout)

    def _do_http(self, caller_name, handlers, method, path, headers=None):
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

        :raises SproxydHTTPException: Received an unhandled HTTP response
        '''

        sproxyd_url = self.sproxyd_urls.next()
        full_url = sproxyd_url + urllib.quote(path)

        def unexpected_http_status(response):
            message = response.read()

            raise exceptions.SproxydHTTPException(
                '%s: %s' % (caller_name, message),
                url=full_url,
                http_status=response.status,
                http_reason=response.reason)

        response = self.http_pools.request(method, full_url, headers=headers,
                                           preload_content=False)
        handler = handlers.get(response.status, unexpected_http_status)
        result = handler(response)

        # If the handler returns a generator, it must handle the connection
        # cleanup.
        if not isinstance(result, types.GeneratorType):
            try:
                drain_connection(response)
                response.release_conn()
            except Exception as exc:
                self.logger.error("Unexpected exception while releasing an "
                                  "HTTP connection after request to %s: %r",
                                  full_url, exc)

        return result

    def get_meta(self, name):
        """Open a connection and get usermd."""

        def handle_200(response):
            header = response.getheader('x-scal-usermd')
            pickled = base64.b64decode(header)
            return pickle.loads(pickled)

        def handle_404(response):
            return None

        handlers = {
            200: handle_200,
            404: handle_404,
        }

        return self._do_http('get_meta', handlers, 'HEAD', name)

    def put_meta(self, name, metadata):
        """Connect to sproxyd and put usermd."""
        if metadata is None:
            raise exceptions.SproxydHTTPException("no usermd")

        headers = {
            'x-scal-cmd': 'update-usermd',
            'x-scal-usermd': base64.b64encode(pickle.dumps(metadata)),
        }

        handlers = {
            200: lambda _: None,
        }

        result = self._do_http('put_meta', handlers, 'PUT', name, headers)

        self.logger.debug("Metadata stored for %s : %s", name, metadata)

        return result

    def del_object(self, name):
        """Connect to sproxyd and delete object."""

        def handle_200_or_404(response):
            return None

        handlers = {
            200: handle_200_or_404,
            404: handle_200_or_404,
        }

        return self._do_http('del_object', handlers, 'DELETE', name)

    def get_object(self, name, headers=None):
        """Connect to sproxyd and get an object."""

        def handle_200_or_206(response):
            for chunk in response.stream(amt=1024 * 64):
                yield chunk
            response.release_conn()

        handlers = {
            200: handle_200_or_206,
            206: handle_200_or_206
        }

        return self._do_http('get_object', handlers, 'GET', name, headers)
