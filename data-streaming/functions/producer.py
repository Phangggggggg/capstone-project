import json
from confluent_kafka import Producer
import functions.ccloud_lib as ccloud_lib  
class KafkaProducer:
    def __init__(self,config_file:str,topic:str) -> None:
        self.config_file = config_file
        self.conf =  ccloud_lib.read_ccloud_config(self.config_file)
        self.topic = topic
        self.producer = self._create_producer()
        self._create_topic()
        self.delivered_records = 0

        
    def _create_producer(self):
        """
        Create and return a Kafka producer instance.
        
        Returns:
        - producer: A Confluent Kafka Producer instance.
        """
        producer_conf = ccloud_lib.pop_schema_registry_params_from_config(self.conf)
        return Producer(producer_conf)

    def _create_topic(self):
        """
        Create the Kafka topic if it does not exist.
        """
        ccloud_lib.create_topic(self.conf, self.topic)
        
    def acked(self, err, msg):
        """
        Delivery report handler called on successful or failed delivery of a message.
        """
        if err is not None:
            print(f"Failed to deliver message: {err}")
        else:
            self.delivered_records += 1
            print(f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")
            
    def produce_message(self, message, key=None):
        """
        Produce a message to the Kafka topic.
        
        Parameters:
        - message: The message (dict) to be sent.
        - key: (Optional) The key for the message. Default is None.
        """
        self.producer.produce(self.topic, key=key, value=json.dumps(message), on_delivery=self.acked)
        self.poll_producer()

    def poll_producer(self):
        """
        Poll the producer to trigger delivery report callbacks.
        """
        self.producer.poll(0)
    
    def flush(self):
        """
        Flush all messages in the producer's buffer.
        """
        self.producer.flush()
        
    def get_delivered_count(self):
        """
        Get the number of successfully delivered records.
        """
        return self.delivered_records
    
if __name__ == '__main__':
    # Configuration and topic setup
    config_file = "application/config/client.properties"
    topic = "test-topic"

    # Initialize the Kafka producer
    kafka_producer = KafkaProducer(config_file, topic)

    # Sample data to produce
    sample_data = {"id": 1, "name": "example"}

    # Produce a message
    kafka_producer.produce_message(sample_data)

    # Ensure all messages are sent
    kafka_producer.flush()

    # Get the count of delivered messages
    print(f"{kafka_producer.get_delivered_count()} messages were produced to topic {topic}!")