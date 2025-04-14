import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# RabbitMQ connection settings
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VIRTUAL_HOST = os.getenv('RABBITMQ_VIRTUAL_HOST', '/')

# Exchange settings
EXCHANGE_DIRECT = 'direct_exchange'
EXCHANGE_TOPIC = 'topic_exchange'
EXCHANGE_FANOUT = 'fanout_exchange'

# Queue settings
QUEUE_DIRECT = 'direct_queue'
QUEUE_TOPIC_1 = 'topic_queue_1'
QUEUE_TOPIC_2 = 'topic_queue_2'
QUEUE_FANOUT_1 = 'fanout_queue_1'
QUEUE_FANOUT_2 = 'fanout_queue_2'

# Routing keys
ROUTING_KEY_DIRECT = 'direct_key'
ROUTING_KEY_TOPIC_1 = 'orders.new'
ROUTING_KEY_TOPIC_2 = 'orders.update'

# Message settings
MESSAGE_PERSISTENCE = 2  # Make messages persistent (saved to disk)

# Consumer settings
PREFETCH_COUNT = 1  # Number of messages to prefetch (for fair dispatch)

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
