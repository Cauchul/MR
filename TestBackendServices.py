import threading
from kafka import KafkaProducer
import concurrent.futures


class TestBackendServices:
    def setup_class(self):
        self.bootstrap_servers = '172.16.23.180:9092'
        self.topic_4g = 'm_pkx_ue_mr_01'
        self.topic_5g = 'm_pkx_ue_mr_02'
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        self.counter_5g = 0
        self.counter_4g = 0
        self.lock_5g = threading.Lock()
        self.lock_4g = threading.Lock()

    def send_data_to_kafka(self, line, topic, lock):
        if topic == self.topic_5g:
            with lock:
                self.counter_5g += 1
                counter = self.counter_5g
                print("发送第{0}条 5G UEMR数据:{1}到主题:{2}\n".format(counter, line.strip(), topic))
        elif topic == self.topic_4g:
            with lock:
                self.counter_4g += 1
                counter = self.counter_4g
                print("发送第{0}条 4G UEMR数据:{1}到主题:{2}\n".format(counter, line.strip(), topic))
        self.producer.send(topic, line.strip())

    def test_send_uemrdata_to_4g_topic(self):
        fiveg_uemr_file_5g = r"D:\桌面文件\2023-04-09\kafka_test\2023-12-26\5guemr23.txt"
        fiveg_uemr_file_4g = r"D:\桌面文件\2023-04-09\kafka_test\2023-12-26\4guemr23.txt"

        max_threads = 20  # 设置线程池的最大线程数
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            with open(fiveg_uemr_file_5g, "rb") as f:
                for line in f:
                    executor.submit(self.send_data_to_kafka, line, self.topic_5g, self.lock_5g)

            with open(fiveg_uemr_file_4g, "rb") as f:
                for line in f:
                    executor.submit(self.send_data_to_kafka, line, self.topic_4g, self.lock_4g)

    def teardown_class(self):
        self.producer.flush()
        self.producer.close()