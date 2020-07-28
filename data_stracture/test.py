#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time         : 2020/7/28 12:47
# @Author       : steven
# @File         : test.py.py
# @Software     : python36
# @Version      : V0.1

"""
Description：
问题：有300道不同的题，40个队列，要求每道题至少在20个队列里出现过，
同时40个队列中的题目数量都相等，且一个队列中不允许出现相同的题。

Solution:
每道题出现20次，40个队列，每个队列里面有150个不同的题目

分配结果用字典存储，字典key 为队列编号，value 为题目编号
"""
import random


class Main:
    def __init__(self):
        # 创建题目编号，300个0~299的数值
        self.task_num_list = [index for index in range(300)]
        random.shuffle(self.task_num_list)

        # 创建队列编号 0~39
        self.queue_num_list = [index for index in range(40)]

        # 创建结果字典，key 为队列编号，value 为list ，题目编号
        self.all_queue = {}
        for i in range(40):
            self.all_queue[str(i)] = []

    def fill_queue(self, task_id):
        """
        填充字典
        :param task_id:
        :return:
        """
        target_queue_list = self.get_queue_list()
        ii = 0
        task_queue_list = []
        while ii < 20:
            queue_num = random.choice(target_queue_list)
            self.all_queue[str(queue_num)].append(task_id)
            index1 = target_queue_list.index(queue_num)
            target_queue_list.pop(index1)
            task_queue_list.append(queue_num)
            ii += 1
        # print(str(task_id) + ":" + str(task_queue_list) + str(len(task_queue_list)))

    def get_queue_current_length(self):
        """
        获取字典里面，每个队列当前分配的题目
        :return:
        """
        len_dic = {}
        for key_index in self.all_queue:
            len_dic[key_index] = str(len(self.all_queue[key_index]))
        return len_dic

    def get_queue_average(self):
        """
        获取字典里面，队列分配题目的平均数
        :return:
        """
        queue_usage_dic = self.get_queue_current_length()
        queue_uasge_sum = 0
        for key_index in queue_usage_dic:
            queue_uasge_sum = int(queue_usage_dic[key_index]) + queue_uasge_sum
        queue_usage_average = int(queue_uasge_sum / len(queue_usage_dic))
        # print(queue_usage_average)
        return queue_usage_average

    def get_queue_list(self):
        """
        计算当前需要分配的作业要分配到哪些队列里面
        :return:
        """
        average = self.get_queue_average()
        len_dic = self.get_queue_current_length()
        queue_list = []
        for key_index in len_dic:
            len = int(len_dic[key_index])
            if len <= average:
                queue_list.append(key_index)
        return queue_list


if __name__ == '__main__':
    m = Main()
    iii = 0
    allocate = []
    for task in m.task_num_list:
        allocate.append(task)
        # print(task)
        # iii = iii + 1
        # print(iii)
        # print(m.all_queue)
        # print(allocate)
        m.fill_queue(task)

    count = 0
    for i in m.all_queue:
        dic_len = len(m.all_queue[i])
        count = count + dic_len
    print(count)

    for i in m.all_queue:
        print(i + ":" + str(len(m.all_queue[i])))
    print("===== result =====")
    print(m.all_queue)
    print("===== result =====")
