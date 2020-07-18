# _*_ coding: utf-8 _*_
# @Time         : 2020/7/18 23:20
# @Author       : steven
# @File         : hdfs_file_api.py
# @Software     : zzDataScience
# @Version      : V0.1

"""
Description：

"""

from hdfs.client import InsecureClient
import os


class HDFSFileAPI:
    def __init__(self, url, user):
        self.client = InsecureClient(url, user)

    def upload_file_to_hdfs(self, local_path, hdfs_path):
        """
        # 上传本地文件到 HDFS
        :param local_path:
        :param hdfs_path:
        :return:
        """
        client = self.client
        print("===== file upload start =====")
        print(client.list(hdfs_path, status=False))
        client.upload(hdfs_path, local_path, cleanup=True)
        print("==== file upload finish =====")

    def download_file_to_local(self, local_path, hdfs_path):
        """
        # 下载 HDFS file or folder到本地
        :param local_path:
        :param hdfs_path:
        :return:
        """
        client = self.client
        print("===== file download start =====")
        client.download(hdfs_path, local_path, overwrite=False)
        print("==== file download finish =====")

    def append_file_to_hdfs(self, hdfs_path, data):
        """
        # 追加数据到hdfs文件
        :param hdfs_path:
        :param data:
        :return:
        """
        client = self.client

        print("===== file append start =====")
        client.write(hdfs_path, data, overwrite=False, append=True)
        print("==== file append finish =====")

    def overwrite_to_hdfs(self, hdfs_path, data):
        """
        # 覆盖数据写到hdfs文件
        :param hdfs_path:
        :param data:
        :return:
        """
        client = self.client

        client.write(hdfs_path, data, overwrite=True, append=False)

    def move_or_rename(self, hdfs_src_path, hdfs_dst_path):
        """
        # 移动或者修改文件
        :param hdfs_src_path:
        :param hdfs_dst_path:
        :return:
        """
        client = self.client
        client.rename(hdfs_src_path, hdfs_dst_path)

    def delete_hdfs_file(self, hdfs_path):
        """
        # 删除hdfs文件
        :param hdfs_path:
        :return:
        """
        client = self.client
        client.delete(hdfs_path)

    def mkdirs(self, hdfs_path):
        """
        # 创建目录
        :param hdfs_path:
        :return:
        """
        client = self.client
        client.makedirs(hdfs_path)

    def read_hdfs_file(self, hdfs_path):
        """
        # 读取hdfs 文件
        :param hdfs_path:
        :return:
        """
        client = self.client
        with client.read(hdfs_path) as reader:
            print(reader.read())


if __name__ == '__main__':
    host = 'hadoop02'
    port = '50070'
    url = f'http://{host}:{port}'
    user = 'hadoop'
    os_file_prefix = 'D:/'
    os_file_name = 'config.ini'

    hdfs_path = '/tmp/'
    hdfs_file_name = os_file_name

    local_file_path = os.path.join(os_file_prefix, os_file_name)
    hdfs_file_path = os.path.join(hdfs_path, hdfs_file_name)

    hdfs = HDFSFileAPI(url, user)
    # upload
    # hdfs.upload_file_to_hdfs(local_file_path, hdfs_path)

    # delete
    # hdfs.delete_hdfs_file(hdfs_file_path)

    # download
    # hdfs.download_file_to_local('D:/temp', hdfs_file_path)

    # append
    # hdfs.append_file_to_hdfs(hdfs_file_path, 'dasdas\nfsfds\n')

    # overwrite
    # hdfs.overwrite_to_hdfs(hdfs_file_path, '1111111111111')

    # 移动或者修改文件
    # hdfs.move_or_rename('/tmp/ccc', '/tmp/ccc_new')

    # mkdirs
    # hdfs.mkdirs("/tmp/zt")

    # read hdfs
    # hdfs.read_hdfs_file('/tmp/config.ini')
