import json
import time
import pika
from pika.exceptions import AMQPError
from src.config import settings
from src.utils.connection import connection_manager
from src.utils.logger import consumer_logger as logger

class FanoutConsumer:
    """
    Consumer for fanout exchange messages
    """
    
    def __init__(self, queue_name):
        """
        Initialize the fanout consumer
        
        Args:
            queue_name (str): Name of the queue to consume from
        """
        self.exchange = settings.EXCHANGE_FANOUT
        self.queue = queue_name
        self.connection = connection_manager
        self.retry_count = {}  # Track message retry counts
    
    def setup(self):
        """
        Set up exchange, queue, and binding
        """
        channel = self.connection.get_channel()
        
        # Declare fanout exchange
        channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='fanout',
            durable=True
        )
        
        # Declare queue
        channel.queue_declare(
            queue=self.queue,
            durable=True
        )
        
        # Bind queue to exchange
        channel.queue_bind(
            queue=self.queue,
            exchange=self.exchange
        )
        
        # Set QoS for fair dispatch
        channel.basic_qos(prefetch_count=settings.PREFETCH_COUNT)
        
        logger.info(f"Fanout consumer set up complete. Exchange: {self.exchange}, Queue: {self.queue}")
    
    def process_message(self, message_body):
        """
        Process the message content
        
        Args:
            message_body (str): Message payload
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            data = json.loads(message_body)
            logger.info(f"Processing fanout broadcast message: {data}")
            
            # Simulate processing time
            time.sleep(1)
            
            # Add your broadcast message processing logic here
            # For fanout exchanges, all bound queues receive the message
            
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def handle_message(self, channel, method, properties, body):
        """
        Message handler callback
        
        Args:
            channel (pika.channel.Channel): The channel object
            method (pika.spec.Basic.Deliver): The basic deliver object
            properties (pika.spec.BasicProperties): The properties of the message
            body (bytes): The message body
        """
        delivery_tag = method.delivery_tag
        message_id = properties.message_id if properties.message_id else delivery_tag
        
        try:
            message_body = body.decode('utf-8')
            logger.info(f"Received broadcast message {message_id} on queue {self.queue}")
            
            if self.process_message(message_body):
                # Acknowledge successful processing
                channel.basic_ack(delivery_tag=delivery_tag)
                logger.info(f"Message {message_id} processed successfully")
                
                # Reset retry count if exists
                if message_id in self.retry_count:
                    del self.retry_count[message_id]
            else:
                # Message processing failed, handle retry logic
                if message_id not in self.retry_count:
                    self.retry_count[message_id] = 1
                else:
                    self.retry_count[message_id] += 1
                
                if self.retry_count[message_id] <= settings.MAX_RETRIES:
                    # Reject message for requeue
                    logger.warning(f"Message {message_id} processing failed, retrying ({self.retry_count[message_id]}/{settings.MAX_RETRIES})")
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                else:
                    # Max retries exceeded, dead-letter or discard
                    logger.error(f"Message {message_id} processing failed after {settings.MAX_RETRIES} attempts, discarding")
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                    del self.retry_count[message_id]
        
        except Exception as e:
            logger.error(f"Error handling message {message_id}: {e}")
            # Reject and requeue the message if it's a temporary error
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
    
    def start_consuming(self):
        """
        Start consuming messages
        """
        try:
            self.setup()
            channel = self.connection.get_channel()
            
            logger.info(f"Starting to consume broadcast messages from {self.queue}")
            channel.basic_consume(
                queue=self.queue,
                on_message_callback=self.handle_message,
                auto_ack=False  # Manual acknowledgment
            )
            
            logger.info("Waiting for broadcast messages. To exit press CTRL+C")
            channel.start_consuming()
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user, shutting down consumer")
            if channel:
                channel.stop_consuming()
            self.connection.close()
        
        except AMQPError as e:
            logger.error(f"AMQP error: {e}")
            # Implement reconnection logic here if needed
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    # Example: creating consumers for different queues bound to the same fanout exchange
    consumer = FanoutConsumer(settings.QUEUE_FANOUT_1)
    consumer.start_consuming()
    
    # To run multiple consumers, you would need to run them in different processes or threads
    # consumer2 = FanoutConsumer(settings.QUEUE_FANOUT_2)
    # consumer2.start_consuming()
