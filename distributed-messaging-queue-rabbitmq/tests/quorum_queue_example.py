"""
Example: Quorum Queues and Cluster Management using the DistributedMQ API

This example demonstrates how to use the DistributedMQ API's cluster management
features and work with quorum queues for stronger consistency guarantees.
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
import pika

def consumer_callback(message, properties, method):
    """Callback function for processing messages"""
    consumer_id = threading.current_thread().name
    print(f"[{consumer_id}] Received message: {json.dumps(message, indent=2)}")
    
    # Simulate processing time
    time.sleep(0.5)
    
    # Return True to acknowledge message
    return True

def demo_quorum_queues():
    """Demonstrate quorum queue usage"""
    # Set Docker mode environment variable
    os.environ['RABBITMQ_DOCKER_MODE'] = 'true'
    
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    try:
        # Check cluster status
        print("\n--- Checking cluster status ---")
        status = dmq.get_cluster_status()
        print(f"Cluster name: {status['cluster_name']}")
        print(f"Is clustered: {status['is_clustered']}")
        print(f"Connected to: {status['connected_to']}")
        print(f"Nodes: {status['nodes']}")
        print(f"Nodes status: {status['nodes_status']}")
        
        # List existing queues
        print("\n--- Listing existing queues ---")
        queues = dmq.list_queues()
        for queue in queues:
            print(f"Queue: {queue.get('name', 'unknown')}, Type: {queue.get('type', 'unknown')}")
        
        # Create a quorum queue
        quorum_queue_name = "q.critical_tasks"
        print(f"\n--- Creating quorum queue: {quorum_queue_name} ---")
        queue_info = dmq.create_quorum_queue(
            queue_name=quorum_queue_name,
            routing_key="critical",
            exchange_name="critical_tasks_exchange",
            exchange_type="direct"
        )
        print(f"Created queue: {json.dumps(queue_info, indent=2)}")
        
        # Create a classic queue for comparison
        classic_queue_name = "normal_tasks"
        print(f"\n--- Creating classic mirrored queue: {classic_queue_name} ---")
        
        # Use standard channel operations for classic queue
        channel = dmq.connection_manager.get_channel()
        channel.exchange_declare(
            exchange="normal_tasks_exchange",
            exchange_type="direct",
            durable=True
        )
        channel.queue_declare(
            queue=classic_queue_name,
            durable=True
        )
        channel.queue_bind(
            exchange="normal_tasks_exchange",
            queue=classic_queue_name,
            routing_key="normal"
        )
        print(f"Created classic queue: {classic_queue_name}")
        
        # List queues again to see the new queues
        print("\n--- Listing queues after creation ---")
        queues = dmq.list_queues()
        for queue in queues:
            print(f"Queue: {queue.get('name', 'unknown')}, Type: {queue.get('type', 'unknown')}")
        
        # Create a consumer for the quorum queue
        print(f"\n--- Setting up consumer for quorum queue: {quorum_queue_name} ---")
        consumer = dmq.create_consumer(
            exchange_type="direct",
            exchange_name="critical_tasks_exchange",
            routing_key="critical",
            queue_name=quorum_queue_name,
            queue_type="quorum"  # Explicitly specify quorum queue type
        )
        
        # Start consuming with callback
        consumer.consume(consumer_callback)
        
        # Create a producer for the quorum queue
        print(f"\n--- Publishing messages to quorum queue: {quorum_queue_name} ---")
        producer = dmq.create_producer(
            exchange_type="direct",
            exchange_name="critical_tasks_exchange",
            routing_key="critical",
            queue_name=quorum_queue_name
        )
        
        # Publish a few messages
        for i in range(3):
            message = {
                "id": i,
                "task": "critical_operation",
                "priority": "high",
                "data": f"Critical task data {i}",
                "timestamp": time.time()
            }
            
            producer.publish(message)
            print(f"Published critical message {i} to quorum queue")
            time.sleep(1)
        
        # Publish to classic queue for comparison
        print(f"\n--- Publishing messages to classic queue: {classic_queue_name} ---")
        classic_producer = dmq.create_producer(
            exchange_type="direct",
            exchange_name="normal_tasks_exchange",
            routing_key="normal",
            queue_name=classic_queue_name
        )
        
        for i in range(3):
            message = {
                "id": i,
                "task": "normal_operation",
                "priority": "medium",
                "data": f"Normal task data {i}",
                "timestamp": time.time()
            }
            
            classic_producer.publish(message)
            print(f"Published normal message {i} to classic queue")
            time.sleep(1)
        
        # Wait for messages to be processed
        print("\n--- Waiting for message processing ---")
        time.sleep(5)
        
        # Get detailed queue info
        print(f"\n--- Getting detailed info for quorum queue: {quorum_queue_name} ---")
        quorum_queue_info = dmq.get_queue_info(quorum_queue_name)
        print(f"Quorum queue info: {json.dumps(quorum_queue_info, indent=2)}")
        
        print(f"\n--- Getting detailed info for classic queue: {classic_queue_name} ---")
        classic_queue_info = dmq.get_queue_info(classic_queue_name)
        print(f"Classic queue info: {json.dumps(classic_queue_info, indent=2)}")
        
        # Demonstrate queue policy management
        print("\n--- Setting quorum queue policy ---")
        result = dmq.set_quorum_queue_policy("^critical\.", "critical-policy")
        print(f"Policy set result: {result}")
        
        # Perform cluster health check
        print("\n--- Performing cluster health check ---")
        health = dmq.health_check()
        print(f"Health check result: {json.dumps(health, indent=2)}")
        
        # Clean up
        print("\n--- Cleaning up ---")
        consumer.stop()
        producer.close()
        classic_producer.close()
        
    finally:
        # Close connections
        dmq.close()
        print("Connections closed")

if __name__ == "__main__":
    demo_quorum_queues()