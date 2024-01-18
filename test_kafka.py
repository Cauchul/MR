# -*- coding: utf-8 -*-
import datetime
import logging
import schedule
import time

from kafka import KafkaProducer

log_file = r'E:\work\mr_fatigue_test\test_log\out_log.log'
# 配置日志
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s: %(message)s')
kafka_project = KafkaProducer(bootstrap_servers='172.16.23.180:9092')
cnt = 0


def send_4g_data_to_kafka():
    topic_4g = 'm_pkx_ue_mr_01'
    file_4g = r"E:\work\mr_fatigue_test\test_data\4g.txt"
    with open(file_4g, "rb") as file:
        for f_line in file:
            print('4G_data: ', f_line)
            print('--' * 50)
            kafka_project.send(topic_4g, f_line.strip())


def send_5g_data_to_kafka():
    topic_5g = 'm_pkx_ue_mr_02'
    file_5g = r"E:\work\mr_fatigue_test\test_data\5g.txt"
    with open(file_5g, "rb") as file:
        for f_line in file:
            print('5G_data: ', f_line)
            print('--' * 50)
            kafka_project.send(topic_5g, f_line.strip())


def run():
    global cnt
    cnt += 1
    logging.info('发送4G数据到kafka' + '--' * 30)
    send_4g_data_to_kafka()
    logging.info('发送5G数据到kafka' + '--' * 30)
    send_5g_data_to_kafka()
    # logging.info("--" * 30 + '数据发送完成' + '--' * 30)
    logging.info("==" * 30 + f'第{cnt}次，数据发送完成' + '==' * 30)


# 将 hourly_job 函数添加到定时任务
schedule.every().hour.at(":00").do(run)
# schedule.every(5).seconds.do(run)

if __name__ == "__main__":
    start_time = time.time()
    while True:
        if time.time() - start_time > 7 * 24 * 3600:
            print('疲劳测试时长达到 7 * 24 小时，结束测试')
            break
        schedule.run_pending()
        # time.sleep(1)
