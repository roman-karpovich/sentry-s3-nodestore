"""
sentry_s3_nodestore.backend
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2015 by Ernest W. Durbin III.
:license: BSD, see LICENSE for more details.
"""

from __future__ import absolute_import

import simplejson
from base64 import urlsafe_b64encode
from time import sleep
from uuid import uuid4

import boto3

from sentry.nodestore.base import NodeStorage


def retry(attempts, func, *args, **kwargs):
    for _ in range(attempts):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            sleep(0.1)
            raise
    raise


class S3NodeStorage(NodeStorage):

    def __init__(self, bucket_name=None, region='eu-west-1', max_retries=3):
        self.max_retries = max_retries
        self.bucket_name = bucket_name
        self.client = boto3.client('s3', region)

    def delete(self, id):
        """
        >>> nodestore.delete('key1')
        """
        self.client.delete_object(Bucket=self.bucket_name, Key=id)

    def delete_multi(self, id_list):
        """
        Delete multiple nodes.

        Note: This is not guaranteed to be atomic and may result in a partial
        delete.

        >>> delete_multi(['key1', 'key2'])
        """
        self.client.delete_objects(Bucket=self.bucket_name, Delete={
            'Objects': [{'Key': id} for id in id_list]
        })

    def get(self, id):
        """
        >>> data = nodestore.get('key1')
        >>> print data
        """
        result = retry(self.max_retries, self.client.get_object, Bucket=self.bucket_name, Key=id)
        return simplejson.loads(result['Body'].read())

    def set(self, id, data):
        """
        >>> nodestore.set('key1', {'foo': 'bar'})
        """
        data = simplejson.dumps(data)
        retry(self.max_retries, self.client.put_object, Body=data, Bucket=self.bucket_name, Key=id)

    def generate_id(self):
        return urlsafe_b64encode(uuid4().bytes)
