import datetime
import json
import time
import unittest

from dataserv_client import cli
from dataserv_client import api
from dataserv_client import exceptions


fixtures = json.load(open("tests/fixtures.json"))
addresses = fixtures["addresses"]
url = "http://127.0.0.1:5000"


class AbstractTestSetup(object):

    def setUp(self):
        time.sleep(2)  # avoid collision


class TestClientRegister(AbstractTestSetup, unittest.TestCase):

    def test_register(self):
        client = api.Client(addresses["alpha"], url=url)
        self.assertTrue(client.register())

    def test_already_registered(self):
        with self.assertRaises(exceptions.AddressAlreadyRegistered):
            client = api.Client(addresses["beta"], url=url)
            client.register()
            client.register()

    def test_invalid_address(self):
        with self.assertRaises(exceptions.InvalidAddress):
            client = api.Client("xyz", url=url)
            client.register()

    def test_invalid_farmer(self):
        with self.assertRaises(exceptions.FarmerNotFound):
            client = api.Client(addresses["nu"], url=url + "/xyz")
            client.register()

    def test_address_required(self):
        with self.assertRaises(exceptions.AddressRequired):
            api.Client().register()


class TestClientPing(AbstractTestSetup, unittest.TestCase):

    def test_ping(self):
        client = api.Client(addresses["gamma"], url=url)
        self.assertTrue(client.register())
        self.assertTrue(client.ping())

    def test_invalid_address(self):
        with self.assertRaises(exceptions.InvalidAddress):
            client = api.Client("xyz", url=url)
            client.ping()

    def test_invalid_farmer(self):
        with self.assertRaises(exceptions.FarmerNotFound):
            client = api.Client(addresses["delta"], url=url + "/xyz")
            client.ping()

    def test_address_required(self):
        with self.assertRaises(exceptions.AddressRequired):
            api.Client().ping()


class TestClientPoll(AbstractTestSetup, unittest.TestCase):

    def test_poll(self):
        client = api.Client(addresses["zeta"], url=url)
        self.assertTrue(client.poll(register_address=True, limit=60))

    def test_address_required(self):
        with self.assertRaises(exceptions.AddressRequired):
            api.Client().poll()


class TestClientVersion(AbstractTestSetup, unittest.TestCase):

    def test_version(self):
        client = api.Client(url=url)
        self.assertEqual(client.version(), api.__version__)


class TestInvalidArgument(AbstractTestSetup, unittest.TestCase):

    def test_invalid_retry_limit(self):
        with self.assertRaises(exceptions.InvalidArgument):
            api.Client(connection_retry_limit=-1)

    def test_invalid_retry_delay(self):
        with self.assertRaises(exceptions.InvalidArgument):
            api.Client(connection_retry_delay=-1)


class TestConnectionRetry(AbstractTestSetup, unittest.TestCase):

    def test_no_retry(self):
        def callback():
            client = api.Client(address=addresses["kappa"],
                                url="http://invalid.url",
                                connection_retry_limit=0,
                                connection_retry_delay=0)
            client.register()
        before = datetime.datetime.now()
        self.assertRaises(exceptions.ConnectionError, callback)
        after = datetime.datetime.now()
        self.assertTrue(datetime.timedelta(seconds=15) > (after - before))

    def test_default_retry(self):
        def callback():
            client = api.Client(address=addresses["kappa"],
                                url="http://invalid.url",
                                connection_retry_limit=5,
                                connection_retry_delay=5)
            client.register()
        before = datetime.datetime.now()
        self.assertRaises(exceptions.ConnectionError, callback)
        after = datetime.datetime.now()
        self.assertTrue(datetime.timedelta(seconds=25) < (after - before))


class TestClientBuild(AbstractTestSetup, unittest.TestCase):

    def test_build(self):
        client = api.Client(addresses["pi"], url=url, debug=True,
                            max_size=1024*1024*256)  # 256MB
        client.register()
        generated = client.build(cleanup=True)
        self.assertTrue(len(generated))

        client = api.Client(addresses["omicron"], url=url, debug=True,
                            max_size=1024*1024*512)  # 512MB
        client.register()
        generated = client.build(cleanup=True)
        self.assertTrue(len(generated) == 4)

    def test_address_required(self):
        with self.assertRaises(exceptions.AddressRequired):
            api.Client().build()


class TestClientCliArgs(AbstractTestSetup, unittest.TestCase):

    def test_poll(self):
        args = [
            "--address=" + addresses["eta"],
            "--url=" + url,
            "poll",
            "--register_address",
            "--delay=5",
            "--limit=60"
        ]
        self.assertTrue(cli.main(args))

    def test_register(self):
        args = ["--address=" + addresses["theta"], "--url=" + url, "register"]
        self.assertTrue(cli.main(args))

    def test_ping(self):
        args = ["--address=" + addresses["iota"], "--url=" + url, "register"]
        self.assertTrue(cli.main(args))

        args = ["--address=" + addresses["iota"], "--url=" + url, "ping"]
        self.assertTrue(cli.main(args))

    def test_no_command_error(self):
        with self.assertRaises(SystemExit):
            cli.main(["--address=" + addresses["lambda"]])

    def test_input_error(self):
        with self.assertRaises(ValueError):
            cli.main([
                "--address=" + addresses["mu"],
                "--url=" + url,
                "poll",
                "--register_address",
                "--delay=5",
                "--limit=xyz"
            ])

    def test_api_error(self):
        with self.assertRaises(exceptions.InvalidAddress):
            cli.main(["--address=xyz", "--url=" + url, "register"])


if __name__ == '__main__':
    unittest.main()
