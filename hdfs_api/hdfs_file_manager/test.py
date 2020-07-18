#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time         : 2020/7/18 18:26
# @Author       : steven
# @File         : test.py
# @Software     : zzDataScience
# @Version      : V0.1

"""
Description：

"""
import pyhdfs


def file_upload(self, host, user_name, local_path, hdfs_path):
    print("===== file upload start =====")
    fs = pyhdfs.HdfsClient(hosts=host, user_name=user_name)
    print(fs.get_active_namenode())
    print(fs.listdir('/'))
    fs.copy_from_local(local_path, hdfs_path)
    print("==== file upload finish =====")


def file_download(self, host, user_name, local_path, hdfs_path):
    print("===== file download start =====")
    fs = pyhdfs.HdfsClient(hosts=host, user_name=user_name)
    print(fs.listdir('/'))
    fs.copy_to_local(hdfs_path, local_path)
    print("==== file download finish =====")


if __name__ == '__main__':
    host = 'hadoop02'
    user_name = 'hadoop'

    # "D:\PYTHNON/hadoop/3.mp4"
    local_path = 'D:/config.ini'
    hdfs_path = '/tmp'
    file_upload(host, user_name, local_path)
