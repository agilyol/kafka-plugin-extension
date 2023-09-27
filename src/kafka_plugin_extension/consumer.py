from abc import ABC, abstractmethod
from confluent_kafka import Consumer, KafkaError
from exceptions import KafkaConsumerError


class ConsumerC(ABC):

    def __init__(self, error_handler: callable, conf: dict, topic: str, value_deserializer: callable):
        """here is a sample of the conf dictionary
        conf = {
            'bootstrap.servers': 'your_bootstrap_servers',
            'group.id': 'your_consumer_group',
            'auto.offset.reset': 'earliest',
        }
        """

        self.consumer = Consumer(conf, value_deserializer)
        self.error_handler = error_handler
        self.topic = topic
        self.run = True

    def stop(self):
        self.run = False

    @abstractmethod
    def handler(self, msg):
        raise NotImplemented('Please implement handler first')

    def __run(self) -> None:
        try:

            self.consumer.subscribe([self.topic])
            while self.run:
                msg = self.consumer.poll(1.0)
                if not msg.error():
                    self.handler(msg)

                elif msg.error().code() != KafkaError._PARTITION_EOF:
                    self.error_handler.log_error(msg.error(), KafkaError)
                    raise KafkaConsumerError('Cannot consume message now')

        except Exception as e:
            self.error_handler.handle_error(e)

    def execute(self) -> None:
        self.__run()
