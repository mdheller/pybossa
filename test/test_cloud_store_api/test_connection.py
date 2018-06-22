# -*- coding: utf8 -*-
# This file is part of PYBOSSA.
#
# Copyright (C) 2018 Scifabric LTD.
#
# PYBOSSA is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PYBOSSA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with PYBOSSA.  If not, see <http://www.gnu.org/licenses/>.

from mock import patch, Mock
from default import Test, with_context
from pybossa.cloud_store_api.connection import create_connection, CustomAuthHandler, CustomProvider
from nose.tools import assert_raises
from boto.auth_handler import NotReadyToAuthenticate


class TestS3Connection(Test):

    auth_headers = [('test', 'name')]

    @with_context
    def test_path(self):
        conn = create_connection(host='s3.store.com', host_suffix='/test',
                                 auth_headers=self.auth_headers)
        path = conn.get_path(path='/')
        assert path == '/test/', path

    @with_context
    def test_path_key(self):
        conn = create_connection(host='s3.store.com', host_suffix='/test',
                                 auth_headers=self.auth_headers)
        path = conn.get_path(path='/bucket/key')
        assert path == '/test/bucket/key', path

    @with_context
    def test_no_verify_context(self):
        conn = create_connection(host='s3.store.com', s3_ssl_no_verify=True,
                                 auth_headers=self.auth_headers)
        assert 'context' in conn.http_connection_kwargs

        conn = create_connection(host='s3.store.com', auth_headers=self.auth_headers)
        assert 'context' not in conn.http_connection_kwargs

    @with_context
    def test_auth_handler_error(self):
        provider = CustomProvider('aws')
        assert_raises(NotReadyToAuthenticate, CustomAuthHandler,
                      's3.store.com', None, provider)

    @with_context
    def test_custom_headers(self):
        header = 'x-custom-access-key'
        host = 's3.store.com'
        access_key = 'test-access-key'

        conn = create_connection(host=host, aws_access_key_id=access_key,
                                 auth_headers=[(header, 'access_key')])
        http = conn.build_base_http_request('GET', '/', None)
        http.authorize(conn)
        assert header in http.headers
        assert http.headers[header] == access_key
