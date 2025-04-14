import json
import uuid
import pika
import time
from pika.exceptions import AMQPError
from src.config import settings
from src.utils.connection import connection_manager
from src.utils.logger import producer_logger as logger

class DirectPublisher:
    """
    Publisher for direct exchange messages
    """
    
    def __init__(self):
        self.exchange = settings.EXCHANGE_DIRECT
        self.routing_key = settings.ROUTING_KEY_DIRECT
        self.connection = connection_manager
    
    def setup(self):
        """
        Set up exchange
        """
        channel = self.connection.get_channel()
        
        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='direct',
            durable=True
        )
        
        logger.info(f"Direct publisher set up complete. Exchange: {self.exchange}")
    
    def publish_message(self, message, persistent=True):
        """
        Publish a message to the direct exchange
        
        Args:
            message (dict): Message payload
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
                routing_key=self.routing_key,
                body=message_json.encode('utf-8'),
                properties=properties
            )
            
            logger.info(f"Published message to exchange '{self.exchange}' with routing key '{self.routing_key}': {message}")
            return True
        
        except AMQPError as e:
            logger.error(f"AMQP error while publishing message: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error while publishing message: {e}")
            return False

if __name__ == "__main__":
    import time
    
    publisher = DirectPublisher()
    
    # Example: Publishing messages
    for i in range(5):
        message = {
            "id": i,
            "message": f"Direct message {i}",
            "timestamp": time.time()
        }
        publisher.publish_message(message)
        time.sleep(1)  # Wait between messages
    
    publisher.connection.close()
