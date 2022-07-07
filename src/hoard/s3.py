import io
import re
from functools import cached_property
import boto3
import botocore

from .hoard import Hoard


class S3Hoard(Hoard):

    S3_LIST_MAX_KEYS = 1000

    def __init__(self, bucket_name, partition='root'):
        self.bucket_name = bucket_name
        self.partition = partition

    @cached_property
    def s3client(self):
        return boto3.client('s3')

    @cached_property
    def s3resource(self):
        return boto3.resource('s3')

    def s3object(self, k):
        return self.s3resource.Object(self.bucket_name, self.key(k))

    def key(self, key):
        return f'{self.partition}/{key}'

    def __delitem__(self, k):
        self.s3object(k).delete()

    def __contains__(self, k):
        try:
            self.s3object(k).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise
        else:
            return True

    def keys(self):

        strip_prefix = lambda x: re.match(f'{self.partition}/(.*)', x).groups()[0]

        kwargs = {
            'Bucket': self.bucket_name,
            'MaxKeys': self.S3_LIST_MAX_KEYS,
            'Prefix': f'{self.partition}/',
        }

        while True:

            response = self.s3client.list_objects_v2(**kwargs)
            for o in response['Contents']:
                yield strip_prefix(o['Key'])

            if response['IsTruncated']:
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            else:
                return

    def load_raw(self, k):
        stream = io.BytesIO()
        self.s3client.download_fileobj(self.bucket_name, self.key(k), stream)
        stream.seek(0)
        return stream

    def store_raw(self, k, stream):
        self.s3client.upload_fileobj(stream, self.bucket_name, self.key(k))


if __name__ == '__main__':

    from hoard.utils.argparse import ArgumentParser, MainProgram


    class TestS3Hoard(MainProgram):

        @classmethod
        def configure_parser(cls, p):
            p.add_argument('bucket')

        @classmethod
        def main(cls, pargs):

            h = S3Hoard(pargs.bucket, partition='test')

            keys = [f'foo{i}' for i in range(10)]

            for k in keys:
                h[k] = k

            assert set(h.keys()) == set(keys)
            del h['foo1']
            assert not 'foo1' in h

    parser = ArgumentParser()
    parser.add_main_program('test', TestS3Hoard)
    parser.parse_and_run()
