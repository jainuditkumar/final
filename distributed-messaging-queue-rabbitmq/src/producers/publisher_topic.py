import json
import uuid
import time
import pika
from pika.exceptions import AMQPError
from src.config import settings
from src.utils.connection import connection_manager
from src.utils.logger import producer_logger as logger

class TopicPublisher:
    """
    Publisher for topic exchange messages
    """
    
    def __init__(self):
        self.exchange = settings.EXCHANGE_TOPIC
        self.connection = connection_manager
    
    def setup(self):
        """
        Set up exchange
        """
        channel = self.connection.get_channel()
        
        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='topic',
            durable=True
        )
        
        logger.info(f"Topic publisher set up complete. Exchange: {self.exchange}")
    
    def publish_message(self, message, routing_key, persistent=True):
        """
        Publish a message to the topic exchange
        
        Args:
            message (dict): Message payload
            routing_key (str): Topic routing key (e.g., 'orders.new')
            persistent (bool): Whether to persist the message
            
        Returns:
            bool: True if publishing was successful, False otherwise
        """
        try:
            self.setup()
            channel = self.connection.get_channel()
            
            # Convert message to JSON
            message_json = json.dumps(message)
            
            # Set message properties
            properties = None
            if persistent:
                properties = pika.BasicProperties(
                    delivery_mode=settings.MESSAGE_PERSISTENCE,  # Make message persistent
                    message_id=str(uuid.uuid4()),
                    content_type='application/json',
                    timestamp=int(time.time())
                )
            
            # Publish message
            channel.basic_publish(
                exchange=self.exchange,
                routing_key=routing_key,
                body=message_json.encode('utf-8'),
                properties=properties
            )
            
            logger.info(f"Published message to exchange '{self.exchange}' with routing key '{routing_key}': {message}")
            return True
        
        except AMQPError as e:
            logger.error(f"AMQP error while publishing message: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error while publishing message: {e}")
            return False

if __name__ == "__main__":
    publisher = TopicPublisher()
    
    # Example: Publishing new order messages
    for i in range(3):
        message = {
            "id": i,
            "type": "new",
            "item": f"Product {i}",
            "timestamp": time.time()
        }
        publisher.publish_message(message, settings.ROUTING_KEY_TOPIC_1)
        time.sleep(1)  # Wait between messages
    
    # Example: Publishing order update messages
    for i in range(3):
        message = {
            "id": i,
            "type": "update",
            "status": "shipped",
            "timestamp": time.time()
        }
        publisher.publish_message(message, settings.ROUTING_KEY_TOPIC_2)
        time.sleep(1)  # Wait between messages
    
    publisher.connection.close()
