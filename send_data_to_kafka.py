# -*- coding: utf-8 -*-

from kafka import KafkaProducer

kafka_project = KafkaProducer(bootstrap_servers='172.16.23.180:9092')


def send_4g_data_to_kafka():
    topic_4g = 'm_pkx_ue_mr_01'
    file_4g = r"E:\work\mr_fatigue_test\test_data\4g.txt"
    with open(file_4g, "rb") as file:
        for f_line in file:
            print(f_line)
            print('--' * 50)
            kafka_project.send(topic_4g, f_line.strip())


def send_5g_data_to_kafka():
    topic_5g = 'm_pkx_ue_mr_02'
    file_5g = r"E:\work\mr_fatigue_test\test_data\5g.txt"
    with open(file_5g, "rb") as file:
        for f_line in file:
            print(f_line)
            print('--' * 50)
            kafka_project.send(topic_5g, f_line.strip())


send_5g_data_to_kafka()
