from confluent_kafka import Consumer, KafkaException
import json

class KafkaClient:
    def __init__(self, brokers, group_id, topic):
        self.conf = {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        self.topic = topic
        self.consumer = Consumer(self.conf)