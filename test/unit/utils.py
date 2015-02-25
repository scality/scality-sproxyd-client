# Copyright (c) 2014, 2015 Scality
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

"""A collection of functions that helps running unit tests"""

import eventlet.wsgi
import functools
import itertools
import random
import re
import unittest

import nose.plugins.skip

UNICODE_CHARS = [unichr(i) for i in itertools.chain(xrange(0x0370, 0x377), xrange(0x0384, 0x038a))]


class WSGIServer(object):
    """Start a WSGI Web server."""
    def __init__(self, application):
        self.application = application
        self._thread = None

        self.server = eventlet.listen(('127.0.0.1', 0))
        (self.ip, self.port) = self.server.getsockname()

    def __enter__(self):
        self._thread = eventlet.spawn(eventlet.wsgi.server, self.server,
                                      self.application)
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        self._thread.kill()
        self.server.close()


def skipIf(condition, reason):
    """
    A `skipIf` decorator.

    Similar to `unittest.skipIf`, for Python 2.6 compatibility.
    """
    def decorator(test_item):
        @functools.wraps(test_item)
        def wrapped(*args, **kwargs):
            if condition:
                raise nose.plugins.skip.SkipTest(reason)
            else:
                return test_item(*args, **kwargs)
        return wrapped
    return decorator


def assertRaisesRegexp(expected_exception, expected_regexp,
                       callable_obj, *args, **kwargs):
    """Asserts that the message in a raised exception matches a regexp."""
    try:
        callable_obj(*args, **kwargs)
    except expected_exception as exc_value:
        if not re.search(expected_regexp, str(exc_value)):
            # We accept both `string` and compiled regex object as 2nd
            # argument to assertRaisesRegexp
            pattern = getattr(expected_regexp, 'pattern', expected_regexp)
            raise unittest.TestCase.failureException(
                '"%s" does not match "%s"' %
                (pattern, str(exc_value)))
    else:
        if hasattr(expected_exception, '__name__'):
            excName = expected_exception.__name__
        else:
            excName = str(expected_exception)
        raise unittest.TestCase.failureException("%s not raised" % excName)


def get_random_unicode(length):
    """Get a random Unicode string."""
    return u''.join(random.choice(UNICODE_CHARS) for _ in xrange(length))
