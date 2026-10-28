"""Private object storage. Only verified immutable objects may be attached to tasks."""
import hashlib
import io
import json
from urllib.parse import urlparse
import boto3
from botocore.config import Config
from PIL import Image, UnidentifiedImageError
import warnings

MAX_MEDIA=12*1024*1024
MAX_PIXELS=30_000_000


class Objects:
    def __init__(self,settings):
        self.bucket=settings['bucket']
        self.prefix=settings.get('prefix','rpa-gateway').strip('/')+'/'
        self.endpoint=settings.get('endpoint')
        self.public_endpoint=settings.get('public_endpoint',self.endpoint)
        self.allowed_hosts={urlparse(u).netloc for u in [self.public_endpoint] if u}
        self.client=self.make_client(settings,self.endpoint)
        self.signer=self.make_client(settings,self.public_endpoint)

    @staticmethod
    def make_client(settings,endpoint):
        return boto3.client('s3',endpoint_url=endpoint,region_name=settings.get('region','us-east-1'),
            aws_access_key_id=settings['access_key'],aws_secret_access_key=settings['secret_key'],
            config=Config(signature_version='s3v4',s3={'addressing_style':settings.get('addressing_style','path')},
                          connect_timeout=2,read_timeout=10,retries={'max_attempts':2},max_pool_connections=8))

    def key(self,tenant,sha):
        return self.prefix+'original/'+hashlib.sha256(tenant.encode()).hexdigest()+'/'+sha

    def staging_key(self,tenant,upload_id):
        return self.prefix+'staging/'+hashlib.sha256(tenant.encode()).hexdigest()+'/'+upload_id

    def put(self,key,data,mime):
        self.client.put_object(Bucket=self.bucket,Key=key,Body=data,ContentType=mime,
                               Metadata={'sha256':hashlib.sha256(data).hexdigest()})

    def get(self,key,limit=MAX_MEDIA):
        response=self.client.get_object(Bucket=self.bucket,Key=key)
        body=response['Body']
        try:
            if response['ContentLength']>limit:raise ValueError('Object exceeds media limit')
            data=body.read(limit+1)
            if len(data)>limit:raise ValueError('Object exceeds media limit')
            return data
        finally:body.close()

    def delete(self,key):
        if not key.startswith(self.prefix):raise ValueError('Object outside gateway prefix')
        self.client.delete_object(Bucket=self.bucket,Key=key)

    def download(self,key,seconds=120):
        return self.signer.generate_presigned_url('get_object',Params={'Bucket':self.bucket,'Key':key},ExpiresIn=seconds)

    def upload(self,key,size):
        return self.signer.generate_presigned_post(self.bucket,key,Conditions=[['content-length-range',1,size]],ExpiresIn=300)

    def ready(self):
        self.client.head_bucket(Bucket=self.bucket)
        return True


def verify_image(data):
    if not data or len(data)>MAX_MEDIA:raise ValueError('Image exceeds byte limit')
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error',Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as image:
                kind=image.format
                if kind not in ('PNG','JPEG') or image.width*image.height>MAX_PIXELS:
                    raise ValueError('Unsupported or excessive image')
                image.verify()
            with Image.open(io.BytesIO(data)) as image:image.load()
    except (UnidentifiedImageError,OSError,SyntaxError,Image.DecompressionBombError,Image.DecompressionBombWarning) as exc:
        raise ValueError('Invalid original image') from exc
    return {'sha256':hashlib.sha256(data).hexdigest(),'md5':hashlib.md5(data).hexdigest(),
            'size':len(data),'mime':{'PNG':'image/png','JPEG':'image/jpeg'}[kind]}
