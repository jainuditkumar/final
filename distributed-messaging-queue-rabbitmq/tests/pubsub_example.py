"""
Example: Publish-Subscribe pattern using the DistributedMQ API
This example demonstrates how to use fanout exchanges for broadcasting messages
to multiple consumers.
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
    print(f"[{consumer_id}] Received message: {json.dumps(message, indent=2)}")
    
    # Simulate processing time
    time.sleep(0.5)
    
    # Return True to acknowledge message
    return True

def start_subscriber(subscriber_id, exchange_name):
    """Start a subscriber for the pub-sub pattern"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Create a unique queue name for this subscriber
    queue_name = f"pubsub_queue_{subscriber_id}"
    
    # Create a subscriber (consumer for fanout exchange)
    consumer = dmq.create_pubsub_subscriber(
        exchange_name=exchange_name,
        queue_name=queue_name,
        auto_ack=False
    )
    
    # Set the thread name for identification in the callback
    threading.current_thread().name = f"Subscriber-{subscriber_id}"
    
    # Start consuming with callback
    consumer.consume(consumer_callback)
    
    print(f"[Subscriber-{subscriber_id}] Started consuming from queue: {consumer.queue_name}")
    
    return consumer, dmq

def start_publisher(exchange_name, message_count=5):
    """Start a publisher for the pub-sub pattern"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Create a publisher
    producer = dmq.create_pubsub(exchange_name=exchange_name)
    
    try:
        # Publish messages
        for i in range(message_count):
            message = {
                "id": i,
                "data": f"Broadcast message {i}",
                "timestamp": time.time()
            }
            
            success = producer.publish(message)
            if success:
                print(f"Published broadcast message {i}")
            else:
                print(f"Failed to publish broadcast message {i}")
            
            time.sleep(1)
    finally:
        producer.close()
        dmq.close()
        print("Publisher closed")

def run_pubsub_demo():
    """Run the publish-subscribe demonstration"""
    # Use a unique exchange name
    exchange_name = f"pubsub_demo_{int(time.time())}"
    
    print(f"Starting pub-sub demo with exchange: {exchange_name}")
    
    # Start multiple subscribers in separate threads
    subscribers = []
    dmq_instances = []
    
    try:
        for i in range(3):  # Start 3 subscribers
            consumer, dmq = start_subscriber(i, exchange_name)
            subscribers.append(consumer)
            dmq_instances.append(dmq)
        
        # Wait a moment for subscribers to initialize
        time.sleep(2)
        
        # Start publisher in the main thread
        start_publisher(exchange_name)
        
        # Keep subscribers running for a while to process messages
        print("Waiting for subscribers to process all messages...")
        time.sleep(3)
    finally:
        # Clean up
        print("Stopping subscribers...")
        for consumer, dmq in zip(subscribers, dmq_instances):
            consumer.stop()
            consumer.close()
            dmq.close()
        print("All subscribers stopped")

if __name__ == "__main__":
    run_pubsub_demo()