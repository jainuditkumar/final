import json
import time
import pika
from pika.exceptions import AMQPError
from src.config import settings
from src.utils.connection import connection_manager
from src.utils.logger import consumer_logger as logger

class TopicConsumer:
    """
    Consumer for topic exchange messages
    """
    
    def __init__(self, queue_name, routing_pattern):
        """
        Initialize the topic consumer
        
        Args:
            queue_name (str): Name of the queue to consume from
            routing_pattern (str): Routing pattern to bind to (e.g., 'orders.*')
        """
        self.exchange = settings.EXCHANGE_TOPIC
        self.queue = queue_name
        self.routing_pattern = routing_pattern
        self.connection = connection_manager
        self.retry_count = {}  # Track message retry counts
    
    def setup(self):
        """
        Set up exchange, queue, and binding
        """
        channel = self.connection.get_channel()
        
        # Declare topic exchange
        channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='topic',
            durable=True
        )
        
        # Declare queue
        channel.queue_declare(
            queue=self.queue,
            durable=True
        )
        
        # Bind queue to exchange with pattern
        channel.queue_bind(
            queue=self.queue,
            exchange=self.exchange,
            routing_key=self.routing_pattern
        )
        
        # Set QoS for fair dispatch
        channel.basic_qos(prefetch_count=settings.PREFETCH_COUNT)
        
        logger.info(f"Topic consumer set up complete. Exchange: {self.exchange}, Queue: {self.queue}, Pattern: {self.routing_pattern}")
    
    def process_message(self, message_body, routing_key):
        """
        Process the message content
        
        Args:
            message_body (str): Message payload
            routing_key (str): The routing key of the message
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            data = json.loads(message_body)
            logger.info(f"Processing topic message with routing key '{routing_key}': {data}")
            
            # Simulate processing time
            time.sleep(1)
            
            # Add your specific processing logic based on routing key
            if 'new' in routing_key:
                logger.info("Processing new order")
                # Logic for new orders
            elif 'update' in routing_key:
                logger.info("Processing order update")
                # Logic for order updates
            
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
        routing_key = method.routing_key
        message_id = properties.message_id if properties.message_id else delivery_tag
        
        try:
            message_body = body.decode('utf-8')
            logger.info(f"Received message {message_id} with routing key '{routing_key}'")
            
            if self.process_message(message_body, routing_key):
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
            
            logger.info(f"Starting to consume messages from {self.queue} with pattern '{self.routing_pattern}'")
            channel.basic_consume(
                queue=self.queue,
                on_message_callback=self.handle_message,
                auto_ack=False  # Manual acknowledgment
            )
            
            logger.info("Waiting for messages. To exit press CTRL+C")
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
    # Example: creating consumers for different topic patterns
    consumer1 = TopicConsumer(settings.QUEUE_TOPIC_1, "orders.new")
    consumer1.start_consuming()
    
    # To run multiple consumers, you would need to run them in different processes or threads
    # consumer2 = TopicConsumer(settings.QUEUE_TOPIC_2, "orders.update")
    # consumer2.start_consuming()
