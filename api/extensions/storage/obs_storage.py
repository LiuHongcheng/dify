import logging
from contextlib import closing
from typing import Generator

from flask import Flask
from obs import GetObjectRequest
from obs import ObsClient

from extensions.storage.base_storage import BaseStorage

logger = logging.getLogger(__name__)


class ObsStorage(BaseStorage):
    """Implementation for huawei obs storage.
    """

    def __init__(self, app: Flask):
        super().__init__(app)
        app_config = self.app.config
        self.bucket_name = app_config.get('OBS_BUCKET_NAME')
        self.client = ObsClient(
            access_key_id=app_config.get('OBS_ACCESS_KEY'),
            secret_access_key=app_config.get('OBS_SECRET_KEY'),
            server=app_config.get('OBS_SERVER')
        )

    def save(self, filename, data):
        self.client.putContent(
            bucketName=self.bucket_name,
            objectKey=filename,
            content=data)

    def load_once(self, filename: str) -> bytes:
        resp = self.client.getObject(self.bucket_name, filename, loadStreamInMemory=True)
        if resp.status < 300:
            logger.info('Get Object Succeeded')
            logger.info('requestId: {}'.format(resp.requestId))
            # 获取对象内容
            logger.info('buffer: {}'.format(resp.body.buffer))
            return resp.body.buffer
        else:
            logger.info('Get Object Failed')
            logger.info('requestId: {}'.format(resp.requestId))
            logger.info('errorCode: {}'.format(resp.errorCode))
            logger.info('errorMessage: {}', resp.errorMessage)

    def load_stream(self, filename: str) -> Generator:
        def generate(filename: str = filename) -> Generator:
            # 下载对象的附加请求参数
            getObjectRequest = GetObjectRequest()
            # 获取对象时重写响应中的Content-Type头。
            getObjectRequest.content_type = 'text/plain'
            resp = self.client.getObject(bucketName=self.bucket_name, objectKey=filename,
                                         getObjectRequest=getObjectRequest, loadStreamInMemory=False)
            if resp.status < 300:
                logger.info('Get Object Succeeded')
                logger.info('requestId: {}'.format(resp.requestId))

                with closing(resp.body.response) as obj:
                    while chunk := obj.read(4096):
                        yield chunk
            else:
                logger.info('Get Object Failed')
                logger.info('requestId: {}'.format(resp.requestId))
                logger.info('errorCode: {}'.format(resp.errorCode))
                logger.info('errorMessage: {}'.format(resp.errorMessage))

            # with closing(self.client.getObject(bucketName=self.bucket_name, objectKey=filename)) as obj:
            #     while chunk := obj.read(4096):
            #         yield chunk

        return generate()

    def download(self, filename, target_filepath):
        self.client.getObject(self.bucket_name, filename, downloadPath=target_filepath)

    def exists(self, filename):
        try:
            resp = self.client.getObjectMetadata(bucketName=self.bucket_name, objectKey=filename)
            if resp.status < 300:
                return True
            else:
                return False
        except:
            return False

    def delete(self, filename):
        self.client.deleteObject(bucketName=self.bucket_name, objectKey=filename)
