"""
Example: Topic Exchange pattern using the DistributedMQ API
This example demonstrates how to use topic exchanges for routing messages
based on routing key patterns.
"""
import json
import time
import sys
import os
import threading

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.api.distributed_mq import DistributedMQ
from src.utils.logger import app_logger as logger

def consumer_callback(message, properties, method):
    """Callback function for processing messages"""
    consumer_id = threading.current_thread().name
    routing_key = method.routing_key
    print(f"[{consumer_id}] Received message with routing key '{routing_key}': {json.dumps(message, indent=2)}")
    
    # Simulate processing time
    time.sleep(0.5)
    
    # Return True to acknowledge message
    return True

def start_consumer(consumer_id, exchange_name, queue_name, routing_patterns):
    """Start a consumer for the topic exchange"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Create a channel
    channel = dmq.connection_manager.get_channel()
    
    # Declare the topic exchange
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type='topic',
        durable=True
    )
    
    # Declare queue
    result = channel.queue_declare(queue=queue_name, durable=True)
    queue_name = result.method.queue
    
    # Bind queue to exchange with routing patterns
    for pattern in routing_patterns:
        channel.queue_bind(
            exchange=exchange_name,
            queue=queue_name,
            routing_key=pattern
        )
    
    # Create a consumer with custom setup
    consumer = dmq.create_consumer(
        exchange_type='topic',
        exchange_name=exchange_name,
        routing_key=routing_patterns[0],  # First pattern (just for initialization)
        queue_name=queue_name,
        prefetch_count=1,
        auto_ack=False
    )
    
    # Set the thread name for identification in the callback
    threading.current_thread().name = f"Consumer-{consumer_id}"
    
    # Start consuming with callback
    consumer.consume(consumer_callback)
    
    print(f"[Consumer-{consumer_id}] Started consuming from queue: {queue_name}")
    print(f"[Consumer-{consumer_id}] Subscribed to patterns: {routing_patterns}")
    
    return consumer, dmq

def publish_messages(exchange_name):
    """Publish messages to topic exchange with different routing keys"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Get a channel
    channel = dmq.connection_manager.get_channel()
    
    # Declare the topic exchange
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type='topic',
        durable=True
    )
    
    # Define some sample routing keys
    routing_keys = [
        "orders.new",
        "orders.update",
        "orders.cancel",
        "users.signup",
        "users.login",
        "users.profile.update",
        "payments.success",
        "payments.failure"
    ]
    
    try:
        # Publish messages with different routing keys
        for i, routing_key in enumerate(routing_keys):
            message = {
                "id": i,
                "routing_key": routing_key,
                "data": f"Message with routing key: {routing_key}",
                "timestamp": time.time()
            }
            
            # Create properties
            properties = pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json'
            )
            
            # Convert message to JSON
            message_body = json.dumps(message)
            
            # Publish message
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message_body,
                properties=properties
            )
            
            print(f"Published message with routing key: {routing_key}")
            time.sleep(0.5)
    finally:
        # Clean up
        dmq.close()
        print("Publisher closed")

def run_topic_exchange_demo():
    """Run the topic exchange demonstration"""
    # Use a unique exchange name
    exchange_name = f"topic_exchange_demo_{int(time.time())}"
    
    print(f"Starting topic exchange demo with exchange: {exchange_name}")
    
    # Define consumers with different routing patterns
    consumers_config = [
        {
            "id": "Orders",
            "queue": "orders_queue",
            "patterns": ["orders.*"]  # All order events
        },
        {
            "id": "Users",
            "queue": "users_queue",
            "patterns": ["users.#"]  # All user events (including nested)
        },
        {
            "id": "Payments",
            "queue": "payments_queue",
            "patterns": ["payments.*"]  # All payment events
        },
        {
            "id": "AllEvents",
            "queue": "all_events_queue",
            "patterns": ["#"]  # All events
        }
    ]
    
    # Start consumers
    consumers = []
    dmq_instances = []
    
    try:
        for config in consumers_config:
            consumer, dmq = start_consumer(
                config["id"],
                exchange_name,
                config["queue"],
                config["patterns"]
            )
            consumers.append(consumer)
            dmq_instances.append(dmq)
        
        # Wait a moment for consumers to initialize
        time.sleep(2)
        
        # Import here to avoid circular imports in the examples
        import pika
        
        # Publish messages with different routing keys
        publish_messages(exchange_name)
        
        # Keep consumers running for a while to process messages
        print("Waiting for consumers to process all messages...")
        time.sleep(5)
    finally:
        # Clean up
        print("Stopping consumers...")
        for consumer, dmq in zip(consumers, dmq_instances):
            consumer.stop()
            consumer.close()
            dmq.close()
        print("All consumers stopped")

if __name__ == "__main__":
    run_topic_exchange_demo()