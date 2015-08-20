import json
import unittest
import urllib

import mock

from dataserv_client import api
from dataserv_client import exceptions


fixtures = json.load(open("tests/fixtures.json"))
_MOCK_ADDRESS = fixtures["addresses"]["alpha"]
_MOCK_SERVER_URL = "http://1.2.3.4:5678"


class MockHTTPError(urllib.error.HTTPError):

    def __init__(self, code):
        self.code = code


class MockURLError(urllib.error.URLError):

    def __init__(self):
        pass


class TestApiClientBase(unittest.TestCase):

    def setUp(self):
        urlopen_patch = mock.patch.object(urllib.request, "urlopen",
                                          autospec=True)
        self.addCleanup(urlopen_patch.stop)
        urlopen_patch.start()

        self.client = api.ApiClient(_MOCK_SERVER_URL, _MOCK_ADDRESS,
                                    connection_retry_limit=0,
                                    connection_retry_delay=0)


class TestApiClientRegister(TestApiClientBase):

    def test_register(self):
        urllib.request.urlopen.return_value.code = 200
        self.assertTrue(self.client.register())
        urllib.request.urlopen.assert_called_once_with(
            _MOCK_SERVER_URL + "/api/register/" + _MOCK_ADDRESS)

    def test_already_registered(self):
        urllib.request.urlopen.side_effect = MockHTTPError(409)
        with self.assertRaises(exceptions.AddressAlreadyRegistered):
            self.client.register()
        urllib.request.urlopen.assert_called_once_with(
            _MOCK_SERVER_URL + "/api/register/" + _MOCK_ADDRESS)

    def test_invalid_address(self):
        urllib.request.urlopen.side_effect = MockHTTPError(400)
        with self.assertRaises(exceptions.InvalidAddress):
            self.client.register()
        urllib.request.urlopen.assert_called_once_with(
            _MOCK_SERVER_URL + "/api/register/" + _MOCK_ADDRESS)

    def test_address_required(self):
        with self.assertRaises(exceptions.AddressRequired):
            api.Client().register()
        self.assertFalse(urllib.request.urlopen.called)


class TestApiClientPing(TestApiClientBase):

    def test_ping(self):
        urllib.request.urlopen.return_value.code = 200
        self.assertTrue(self.client.ping())
        urllib.request.urlopen.assert_called_once_with(
            _MOCK_SERVER_URL + "/api/ping/" + _MOCK_ADDRESS)

    def test_invalid_address(self):
        urllib.request.urlopen.side_effect = MockHTTPError(400)
        with self.assertRaises(exceptions.InvalidAddress):
            self.client.ping()
        urllib.request.urlopen.assert_called_once_with(
            _MOCK_SERVER_URL + "/api/ping/" + _MOCK_ADDRESS)

    def test_invalid_farmer(self):
        urllib.request.urlopen.side_effect = MockHTTPError(404)
        with self.assertRaises(exceptions.FarmerNotFound):
            self.client.ping()
        urllib.request.urlopen.assert_called_once_with(
            _MOCK_SERVER_URL + "/api/ping/" + _MOCK_ADDRESS)

    def test_address_required(self):
        with self.assertRaises(exceptions.AddressRequired):
            api.Client().ping()
        self.assertFalse(urllib.request.urlopen.called)


class TestApiClientHeight(TestApiClientBase):

    def test_height(self):
        urllib.request.urlopen.return_value.code = 200
        height = 42
        self.assertTrue(self.client.height(42))
        urllib.request.urlopen.assert_called_once_with(
            '%s/api/height/%s/%s' % (_MOCK_SERVER_URL, _MOCK_ADDRESS, height))

    def test_invalid_address(self):
        urllib.request.urlopen.side_effect = MockHTTPError(400)
        height = 42
        with self.assertRaises(exceptions.InvalidAddress):
            self.client.height(height)
        urllib.request.urlopen.assert_called_once_with(
            '%s/api/height/%s/%s' % (_MOCK_SERVER_URL, _MOCK_ADDRESS, height))

    def test_invalid_farmer(self):
        urllib.request.urlopen.side_effect = MockHTTPError(404)
        height = 42
        with self.assertRaises(exceptions.FarmerNotFound):
            self.client.height(height)
        urllib.request.urlopen.assert_called_once_with(
            '%s/api/height/%s/%s' % (_MOCK_SERVER_URL, _MOCK_ADDRESS, height))


class TestApiClientConnectionRetry(TestApiClientBase):

    def test_invalid_retry_limit(self):
        with self.assertRaises(exceptions.InvalidArgument):
            api.ApiClient(_MOCK_SERVER_URL, _MOCK_ADDRESS,
                          connection_retry_limit=-1)

    def test_invalid_retry_delay(self):
        with self.assertRaises(exceptions.InvalidArgument):
            api.ApiClient(_MOCK_SERVER_URL, _MOCK_ADDRESS,
                          connection_retry_delay=-1)

    @mock.patch("time.sleep")
    def test_no_retry(self, mock_sleep):
        urllib.request.urlopen.side_effect = MockURLError()
        client = api.Client(_MOCK_ADDRESS, url=_MOCK_SERVER_URL,
                            connection_retry_limit=0,
                            connection_retry_delay=0)
        with self.assertRaises(exceptions.ConnectionError):
            client.register()
        self.assertTrue(urllib.request.urlopen.called)
        self.assertFalse(mock_sleep.called)

    @mock.patch("time.sleep")
    def test_default_retry(self, mock_sleep):
        urllib.request.urlopen.side_effect = MockURLError()
        client = api.Client(_MOCK_ADDRESS, url=_MOCK_SERVER_URL,
                            connection_retry_limit=5, connection_retry_delay=5)
        with self.assertRaises(exceptions.ConnectionError):
            client.register()
        self.assertEqual(6, urllib.request.urlopen.call_count,
                         "Expect 1 call + 5 retries = 6 calls")
        mock_sleep.assert_has_calls([mock.call(5), mock.call(5), mock.call(5),
                                     mock.call(5), mock.call(5)])


if __name__ == "__main__":
    unittest.main()
