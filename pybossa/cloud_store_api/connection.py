from copy import deepcopy
import ssl
import sys
import time
from urllib.parse import urlparse

from boto.auth_handler import AuthHandler
import boto.auth

from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.provider import Provider
import jwt


def create_connection(**kwargs):
    return CustomConnection(**kwargs)


class CustomProvider(Provider):
    """Extend Provider to carry information about the end service provider, in
       case the service is being proxied.
    """

    def __init__(self, name, access_key=None, secret_key=None,
                 security_token=None, profile_name=None, object_service=None,
                 auth_headers=None):
        self.object_service = object_service or name
        self.auth_headers = auth_headers
        super(CustomProvider, self).__init__(name, access_key, secret_key,
            security_token, profile_name)


class CustomConnection(S3Connection):

    def __init__(self, *args, **kwargs):
        if not kwargs.get('calling_format'):
            kwargs['calling_format'] = OrdinaryCallingFormat()

        kwargs['provider'] = CustomProvider('aws',
            kwargs.get('aws_access_key_id'),
            kwargs.get('aws_secret_access_key'),
            kwargs.get('security_token'),
            kwargs.get('profile_name'),
            kwargs.pop('object_service', None),
            kwargs.pop('auth_headers', None))

        kwargs['bucket_class'] = CustomBucket

        ssl_no_verify = kwargs.pop('s3_ssl_no_verify', False)
        self.host_suffix = kwargs.pop('host_suffix', '')

        super(CustomConnection, self).__init__(*args, **kwargs)

        if kwargs.get('is_secure', True) and ssl_no_verify:
            self.https_validate_certificates = False
            context = ssl._create_unverified_context()
            self.http_connection_kwargs['context'] = context

    def get_path(self, path='/', *args, **kwargs):
        ret = super(CustomConnection, self).get_path(path, *args, **kwargs)
        return self.host_suffix + ret


class CustomKey(Key):

    def generate_url(self, *args, **kwargs):
        rv = super().generate_url(*args, **kwargs)
        return rv.replace(':443', '')


class CustomBucket(Bucket):
    """Handle both 200 and 204 as response code"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_key_class(CustomKey)

    def delete_key(self, *args, **kwargs):
        try:
            super(CustomBucket, self).delete_key(*args, **kwargs)
        except S3ResponseError as e:
            if e.status != 200:
                raise


class CustomAuthHandler(AuthHandler):
    """Implements sending of custom auth headers"""

    capability = ['s3']

    def __init__(self, host, config, provider):
        if not provider.auth_headers:
            raise boto.auth_handler.NotReadyToAuthenticate()
        self._provider = provider
        super(CustomAuthHandler, self).__init__(host, config, provider)

    def add_auth(self, http_request, **kwargs):
        headers = http_request.headers
        for header, attr in self._provider.auth_headers:
            headers[header] = getattr(self._provider, attr)

    def sign_string(self, *args, **kwargs):
        return ''
