import json
import uuid
import time
import pika
from pika.exceptions import AMQPError
from src.config import settings
from src.utils.connection import connection_manager
from src.utils.logger import producer_logger as logger

class FanoutPublisher:
    """
    Publisher for fanout exchange messages
    """
    
    def __init__(self):
        self.exchange = settings.EXCHANGE_FANOUT
        self.connection = connection_manager
    
    def setup(self):
        """
        Set up exchange
        """
        channel = self.connection.get_channel()
        
        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='fanout',
            durable=True
        )
        
        logger.info(f"Fanout publisher set up complete. Exchange: {self.exchange}")
    
    def publish_message(self, message, persistent=True):
        """
        Publish a broadcast message to the fanout exchange
        
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
            
            # Publish message (no routing key needed for fanout)
            channel.basic_publish(
                exchange=self.exchange,
                routing_key='',  # Empty routing key for fanout
                body=message_json.encode('utf-8'),
                properties=properties
            )
            
            logger.info(f"Published broadcast message to exchange '{self.exchange}': {message}")
            return True
        
        except AMQPError as e:
            logger.error(f"AMQP error while publishing message: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error while publishing message: {e}")
            return False

if __name__ == "__main__":
    publisher = FanoutPublisher()
    
    # Example: Broadcasting system messages
    broadcast_types = ["maintenance", "update", "alert"]
    
    for i, btype in enumerate(broadcast_types):
        message = {
            "id": i,
            "type": btype,
            "message": f"System {btype} notification",
            "timestamp": time.time()
        }
        publisher.publish_message(message)
        time.sleep(1)  # Wait between messages
    
    publisher.connection.close()
