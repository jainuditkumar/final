"""
Example: Direct exchange pattern using the DistributedMQ API
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
    print(f"Received message: {json.dumps(message, indent=2)}")
    print(f"Processing message with ID: {message.get('id', 'unknown')}")
    
    # Simulate processing time
    time.sleep(1)
    
    # Return True to acknowledge message, False to reject/retry
    return True

def start_consumer():
    """Start a consumer for direct exchange"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Create a consumer
    consumer = dmq.create_consumer(
        exchange_type='direct',
        prefetch_count=1,
        auto_ack=False
    )
    
    # Start consuming with callback
    consumer.consume(consumer_callback)
    
    print(f"Started consuming from queue: {consumer.queue_name}")
    print("Press Ctrl+C to exit")
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping consumer...")
        consumer.stop()
        consumer.close()
        dmq.close()
        print("Consumer stopped")

def publish_messages():
    """Publish messages to direct exchange"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Create a producer
    producer = dmq.create_producer(exchange_type='direct')
    
    try:
        # Publish a few messages
        for i in range(5):
            message = {
                "id": i,
                "data": f"Test message {i}",
                "timestamp": time.time()
            }
            
            success = producer.publish(message)
            if success:
                print(f"Published message {i}")
            else:
                print(f"Failed to publish message {i}")
            
            time.sleep(0.5)
    finally:
        producer.close()
        dmq.close()
        print("Producer closed")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "consumer":
        start_consumer()
    else:
        publish_messages()