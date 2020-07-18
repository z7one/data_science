#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time         : 2020/7/18 17:35
# @Author       : steven
# @File         : pyhdfs_file_manager.py
# @Software     : zzDataScience
# @Version      : V0.1

"""
Description：
use pyhdfs test HDFS  API
DOC:https://pyhdfs.readthedocs.io/en/latest/pyhdfs.html#pyhdfs.HdfsClient
"""

import pyhdfs
import pathlib
import os


class PYHDFSFileManager:
    def __init__(self, hosts, user_name):
        self.hosts = hosts
        self.user_name = user_name
        self.fs = pyhdfs.HdfsClient(hosts=self.hosts, user_name=self.user_name)

    def file_upload(self, local_path, hdfs_path):
        """
        # 上传本地文件到 HDFS
        :param local_path:
        :param hdfs_path:
        :return:
        """
        fs = self.fs
        print("===== file upload start =====")
        print(fs.get_active_namenode())
        print(fs.listdir('/'))
        fs.copy_from_local(local_path, hdfs_path)
        print("==== file upload finish =====")

    def file_download(self, local_path, hdfs_path):
        """
        # 下载 HDFS文件到本地
        :param local_path:
        :param hdfs_path:
        :return:
        """
        fs = self.fs
        print("===== file download start =====")
        print(fs.listdir('/'))
        fs.copy_to_local(hdfs_path, local_path)
        print("==== file download finish =====")

    def os_file_exists(self, os_file_path):
        """
        # 判断系统目录是否存在
        :param os_file_path:
        :return:
        """
        path = pathlib.Path(os_file_path)
        # 判断路径是否存在
        path.exists()

        # 判断是否为文件
        path.is_file()

        # 判断是否为目录
        path.is_dir()

    def hdfs_file_exists(self, hdfs_file_path):
        """
        # 判断 HDFS 目录是否存在
        :param hdfs_file_path:
        :return:
        """
        fs = self.fs
        return fs.exists(hdfs_file_path)

    def hdfs_file_read(self, hdfs_file_path):
        fs = self.fs
        # 打开一个远程节点上的文件，返回一个HTTPResponse对象
        response = fs.open(hdfs_file_path)
        print(hdfs_file_path)
        print(type(response))
        # 查看文件内容
        response.read()
        print(response.status)
        # 返回的数据
        print(response.data.decode('unicode_escape'))
        print(response.status, response.headers, len(response.data))

    def hdfs_file_append(self, hdfs_file_path, content):
        fs = self.fs
        fs.append(hdfs_file_path, content)

    def get_hdfs_content_summary(self, hdfs_file_path):
        fs = self.fs
        print(hdfs_file_path)
        hdfs_content = fs.get_content_summary(hdfs_file_path)
        print(hdfs_content)

    def hdfs_file_delete(self, hdfs_file_path):
        """
        # 删除文件
        :param hdfs_file_path:
        :return:
        """
        fs = self.fs
        fs.delete(hdfs_file_path)  # 删除文件

    def hdfs_path_delete(self, hdfs_path):
        """
        # 删除目录
        :param hdfs_path:
        :return:
        """
        fs = self.fs
        fs.delete(hdfs_path, recursive=True)  # 删除目录  recursive=True

    def hdfs_makdir(self, hdfs_path):
        """
        # 新建目录
        :param hdfs_path:
        :return:
        """
        fs = self.fs
        if not fs.exists(hdfs_path):
            # os.system('hadoop fs -mkdir '+filePath)
            fs.mkdirs(hdfs_path)
            return 'mkdir'
        return 'exits'

    def rename(self, src_path, dst_path):
        """
        # 重命名
        :param src_path:
        :param dst_path:
        :return:
        """
        fs = self.fs
        if not fs.exists(src_path):
            return
        fs.rename(src_path, dst_path)


if __name__ == '__main__':
    host = 'hadoop02'
    user_name = 'hadoop'
    os_file_prefix = 'D:/'
    os_file_name = 'sdss.csv'

    hdfs_file_prefix = '/tmp/'
    hdfs_file_name = os_file_name
    local_path = os.path.join(os_file_prefix, os_file_name)
    hdfs_path = os.path.join(hdfs_file_prefix, hdfs_file_name)

    hdfs = PYHDFSFileManager(host, user_name)
    # hdfs.file_upload(local_path, hdfs_path)
    hdfs.hdfs_file_read(hdfs_path)
    # hdfs.hdfs_file_append(hdfs_path, "dadsaas")
    # hdfs_path_result = hdfs.hdfs_file_exists(hdfs_path)
    # hdfs.get_hdfs_content_summary(hdfs_path)
