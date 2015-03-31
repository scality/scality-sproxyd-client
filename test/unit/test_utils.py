# Copyright (c) 2014 Scality
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

'''Tests for `scality_sproxyd_client.utils`'''

import functools
import itertools
import types
import unittest

import eventlet
import mock

from scality_sproxyd_client import exceptions
from scality_sproxyd_client import utils


class TestIsSproxydConfValid(unittest.TestCase):

    def test_ini_conf_with_no_by_path_flag(self):
        conf = '''
               [chord_path]
               ring_driver="chord"
               '''
        self.assertRaises(exceptions.SproxydConfException,
                          utils.is_sproxyd_conf_valid, conf)

    def test_json_conf_with_no_by_path_flag(self):
        conf = '''
               {
               "type": "chord",
               }
               '''
        self.assertRaises(exceptions.SproxydConfException,
                          utils.is_sproxyd_conf_valid, conf)

    def test_ini_conf_with_by_path_disabled(self):
        conf = '''
               [chord_path]
               by_path_enabled="0"
               '''
        self.assertRaises(exceptions.SproxydConfException,
                          utils.is_sproxyd_conf_valid, conf)

    def test_json_conf_with_by_path_disabled(self):
        conf = '''
               {
               "by_path_enabled": false,
               }
               '''
        self.assertRaises(exceptions.SproxydConfException,
                          utils.is_sproxyd_conf_valid, conf)

    def test_ini_conf_with_by_path_enabled(self):
        conf = '''
               [chord_path]
               by_path_enabled="1",
               '''
        self.assertTrue(utils.is_sproxyd_conf_valid(conf))

    def test_json_conf_with_by_path_enabled(self):
        # is_sproxyd_conf_valid should be case-insensitive
        conf = '''
               {
               "by_path_enabled": TrUe
               }
               '''
        self.assertTrue(utils.is_sproxyd_conf_valid(conf))


class TestMonitoringLoop(unittest.TestCase):

    def setUp(self):
        self.on_up = mock.Mock()
        self.on_down = mock.Mock()

    def loop(self, ping):
        sleep = eventlet.sleep
        with mock.patch('eventlet.sleep', side_effect=lambda _: sleep(0.001)):
            utils.monitoring_loop(ping, self.on_up, self.on_down)

    def test_monitoring_loop_with_ping_always_false(self):
        ping = mock.Mock(return_value=False)

        thread = eventlet.spawn(functools.partial(self.loop, ping))
        eventlet.sleep(0.05)
        try:
            # The loop ran for ~0.05 sec, each iteration of the loop should
            # run in ~0.001 sec so there should be ~50 calls to `ping`.
            # We choose 5, just to be on the safe side
            self.assertTrue(ping.call_count >= 5)
            self.assertEqual(1, self.on_down.call_count)
            self.assertFalse(self.on_up.called)
        finally:
            thread.kill()

    def test_monitoring_loop_with_ping_always_true(self):
        ping = mock.Mock(return_value=True)

        thread = eventlet.spawn(functools.partial(self.loop, ping))
        eventlet.sleep(0.02)
        try:
            self.assertTrue(ping.call_count >= 5)
            self.assertEqual(1, self.on_up.call_count)
            self.assertFalse(self.on_down.called)
        finally:
            thread.kill()

    def test_monitoring_loop_with_ping_flapping(self):
        # The number of True and False value should be approximatively
        # the same, otherwise the AFD would discard some of them as outliers
        cycle = itertools.cycle([False] * 10 + [True] * 10)
        ping = mock.Mock(side_effect=cycle)

        thread = eventlet.spawn(functools.partial(self.loop, ping))
        eventlet.sleep(0.2)
        try:
            self.assertTrue(ping.call_count >= 5)
            # We've slept for a time long enough to see several (>=3 even
            # on a slow machine) UP & DOWN cycles
            self.assertTrue(self.on_down.call_count >= 3)
            self.assertTrue(self.on_up.call_count >= 3)

            # Call to `on_up` and `on_down` must alternate strictly
            diff = abs(self.on_down.call_count - self.on_up.call_count)
            self.assertTrue(abs(diff) <= 1)
        finally:
            thread.kill()


@mock.patch("pkg_resources.get_distribution")
def test_import_specific(mock_get_distrib):
    with utils.import_specific('foobar1', 'foobar2'):
        pass
    calls = [mock.call('foobar1'), mock.call('foobar2')]
    assert mock_get_distrib.call_args_list == calls


def test_get_urllib3():
    urllib3 = utils.get_urllib3()
    assert isinstance(urllib3, types.ModuleType)
    assert urllib3.__version__ in utils.REQUIRES['urllib3']


def test_get_urllib3_with_different_versions():

    @mock.patch('scality_sproxyd_client.utils.import_specific')
    def assert_import_specific_called(version, mock_import_specific):
        with mock.patch('urllib3.__version__', version):
            urllib3 = utils.get_urllib3()
        mock_import_specific.assert_called_once_with(utils.REQUIRES['urllib3'])
        assert isinstance(urllib3, types.ModuleType)

    for version in ('0.1', '9.9'):
        yield assert_import_specific_called, version


class TestSplitList(unittest.TestCase):
    def test_empty_string(self):
        self.assertEqual(
            [],
            list(utils.split_list('')))

    def test_basic(self):
        self.assertEqual(
            ['1', '2', '3'],
            list(utils.split_list('1, 2, 3')))

    def test_space_prefix(self):
        self.assertEqual(
            ['1', '2'],
            list(utils.split_list('   1, 2')))

    def test_space_suffix(self):
        self.assertEqual(
            ['1', '2'],
            list(utils.split_list('1, 2   ')))

    def test_words(self):
        self.assertEqual(
            ['one', 'two', 'three'],
            list(utils.split_list(' one, two, three ')))
