#!/usr/bin/env python3

from future.standard_library import install_aliases
install_aliases()

import datetime
from http.client import HTTPException
import os
import socket
import time
import urllib
import urllib.error
import urllib.request

from dataserv_client import __version__
from dataserv_client import builder
from dataserv_client import common
from dataserv_client import deserialize
from dataserv_client import exceptions

_timedelta = datetime.timedelta
_now = datetime.datetime.now


class ApiClient(object):

    def __init__(self, server_url, client_address, connection_retry_limit,
                 connection_retry_delay):
        self._server_url = server_url

        if not client_address:
            raise exceptions.AddressRequired()
        self._client_address = client_address

        if connection_retry_limit < 0:
            raise exceptions.InvalidArgument()
        self._connection_retry_limit = connection_retry_limit

        if connection_retry_delay < 0:
            raise exceptions.InvalidArgument()
        self._connection_retry_delay = connection_retry_delay

    def _url_query(self, api_path, retries=0):
        try:
            response = urllib.request.urlopen(self._server_url + api_path)
            if response.code == 200:
                return True
            return False  # pragma: no cover

        except urllib.error.HTTPError as e:
            if e.code == 409:
                raise exceptions.AddressAlreadyRegistered(self._client_address,
                                                          self._server_url)
            elif e.code == 404:
                raise exceptions.FarmerNotFound(self._server_url)
            elif e.code == 400:
                raise exceptions.InvalidAddress(self._client_address)
            elif e.code == 500:  # pragma: no cover
                raise exceptions.FarmerError(self._server_url)
            else:
                raise e  # pragma: no cover
        except HTTPException:
            self._handle_connection_error(api_path, retries)
        except urllib.error.URLError:
            self._handle_connection_error(api_path, retries)
        except socket.error:
            self._handle_connection_error(api_path, retries)

    def _handle_connection_error(self, api_path, retries):
        if retries >= self._connection_retry_limit:
            raise exceptions.ConnectionError(self._server_url)
        time.sleep(self._connection_retry_delay)
        return self._url_query(api_path, retries + 1)

    def server_url(self):
        return self._server_url

    def client_address(self):
        return self._client_address

    def register(self):
        """Attempt to register this client address."""
        return self._url_query("/api/register/%s" % self._client_address)

    def ping(self):
        """Send a heartbeat message for this client address."""
        return self._url_query("/api/ping/%s" % self._client_address)

    def height(self, height):
        """Set the height claim for this client address."""
        return self._url_query('/api/height/%s/%s' % (self._client_address,
                                                      height))


class Client(object):

    def __init__(self, client_address=None, url=common.DEFAULT_URL, debug=False,
                 max_size=common.DEFAULT_MAX_SIZE,
                 store_path=common.DEFAULT_STORE_PATH,
                 connection_retry_limit=common.DEFAULT_CONNECTION_RETRY_LIMIT,
                 connection_retry_delay=common.DEFAULT_CONNECTION_RETRY_DELAY):

        self._validate_client_address(client_address)
        # FIXME add deserialize.positive_integer for retries
        self._api_client = ApiClient(url, client_address,
                                     connection_retry_limit,
                                     connection_retry_delay)
        self.debug = debug
        self.max_size = deserialize.byte_count(max_size)
        self.store_path = os.path.realpath(store_path)

        # ensure storage dir exists
        if not os.path.exists(self.store_path):
            os.makedirs(self.store_path)

    def _validate_client_address(self, client_address):
        if not client_address:  # TODO ensure address is valid
            raise exceptions.AddressRequired()

    def version(self):
        print(__version__)
        return __version__

    def register(self):
        """Attempt to register the config address."""
        if self._api_client.register():
            print("Address %s now registered on %s." % (
                self._api_client.client_address(),
                self._api_client.server_url()))
            return True
        else:
            return False

    def ping(self):
        """Attempt keep-alive with the server."""
        print("Pinging %s with address %s." % (
            self._api_client.server_url(), self._api_client.client_address()))
        return self._api_client.ping()

    def poll(self, register_address=False, delay=common.DEFAULT_DELAY,
             limit=None):
        """TODO doc string"""
        stop_time = _now() + _timedelta(seconds=int(limit)) if limit else None

        if register_address:
            self.register()

        while True:
            self.ping()

            if stop_time and _now() >= stop_time:
                return True
            time.sleep(int(delay))

    def build(self, cleanup=False, rebuild=False):
        """TODO doc string"""

        def on_generate_shard(height, unused_seed, unused_file_hash):
            self._api_client.height(height)
        bldr = builder.Builder(self.address, common.SHARD_SIZE, self.max_size,
                               on_generate_shard=on_generate_shard)
        generated = bldr.build(self.store_path, debug=self.debug,
                               cleanup=cleanup, rebuild=rebuild)
        height = len(generated)
        self._api_client.height(height)
        return generated
