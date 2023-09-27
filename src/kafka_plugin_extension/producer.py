from confluent_kafka.avro import AvroProducer
from confluent_kafka import Producer, avro
from enum import Enum


class EncodingEnum(Enum):
    JSON = 'json'
    AVRO = 'avro'


class KafkaProducer(object):

    def __init__(self, conf, topic, error_handler, encoding_type, schema=None):

        if encoding_type == EncodingEnum.AVRO.value:
            if not schema:
                raise ValueError('schema required for avro events')
            self._producer = AvroProducer(conf, default_value_schema=avro.load(schema))

        elif encoding_type == EncodingEnum.JSON.value:
            self._producer = Producer(conf)

        else:
            raise ValueError('encoding_type must be json or avro')

        self._topic = topic
        self.error_handler = error_handler

    def publish(self, data: dict):
        try:
            # produce an event
            self._producer.produce(topic=self._topic, value = data)

        except Exception as e:
            # implement error handling class from error_handler.py
            self.error_handler.handle_error(e)

        finally:
            # Close the producer
            self._producer.flush()
