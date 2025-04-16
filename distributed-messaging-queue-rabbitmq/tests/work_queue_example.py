"""
Example: Work Queue pattern using the DistributedMQ API
This example demonstrates how to use work queues for distributing tasks
among multiple workers.
"""
import json
import time
import sys
import os
import threading
import random

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.api.distributed_mq import DistributedMQ
from src.utils.logger import app_logger as logger

def worker_callback(message, properties, method):
    """Worker callback function for processing tasks"""
    worker_id = threading.current_thread().name
    print(f"[{worker_id}] Processing task: {json.dumps(message, indent=2)}")
    
    # Simulate variable processing time
    processing_time = random.uniform(0.5, 2.0)
    print(f"[{worker_id}] Task will take {processing_time:.2f} seconds")
    time.sleep(processing_time)
    
    print(f"[{worker_id}] Completed task: {message.get('id')}")
    
    # Return True to acknowledge message
    return True

def start_worker(worker_id, queue_name):
    """Start a worker for the work queue"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Create a worker (consumer for work queue)
    worker = dmq.create_work_queue_worker(
        queue_name=queue_name,
        prefetch_count=1,  # Process one task at a time
        auto_ack=False
    )
    
    # Set the thread name for identification in the callback
    threading.current_thread().name = f"Worker-{worker_id}"
    
    # Start consuming with callback
    worker.consume(worker_callback)
    
    print(f"[Worker-{worker_id}] Started processing tasks from queue: {worker.queue_name}")
    
    return worker, dmq

def publish_tasks(queue_name, task_count=10):
    """Publish tasks to the work queue"""
    # Create DistributedMQ instance
    dmq = DistributedMQ()
    
    # Create a task producer
    producer = dmq.create_work_queue(queue_name=queue_name)
    
    try:
        # Publish tasks
        for i in range(task_count):
            task = {
                "id": i,
                "task_type": "process_data",
                "payload": f"Data batch {i}",
                "created_at": time.time()
            }
            
            success = producer.publish(task)
            if success:
                print(f"Published task {i}")
            else:
                print(f"Failed to publish task {i}")
            
            # Don't wait between publishing tasks
    finally:
        producer.close()
        dmq.close()
        print("Task producer closed")

def run_work_queue_demo():
    """Run the work queue demonstration"""
    # Use a unique queue name
    hostname = input("Enter your hostname: ")
    queue_name = input("Enter a unique queue name: ")
    queue_name = f"{hostname}-{queue_name}"
    
    print(f"Starting work queue demo with queue: {queue_name}")
    
    # Start multiple workers in separate threads
    workers = []
    dmq_instances = []
    
    try:
        for i in range(3):  # Start 3 workers
            worker, dmq = start_worker(i, queue_name)
            workers.append(worker)
            dmq_instances.append(dmq)
        
        # Wait a moment for workers to initialize
        time.sleep(2)
        
        # Publish tasks in the main thread
        publish_tasks(queue_name, task_count=10)
        
        # Keep workers running for a while to process all tasks
        print("Waiting for workers to process all tasks...")
        time.sleep(15)
    finally:
        # Clean up
        print("Stopping workers...")
        for worker, dmq in zip(workers, dmq_instances):
            worker.stop()
            worker.close()
            dmq.close()
        print("All workers stopped")

if __name__ == "__main__":
    run_work_queue_demo()