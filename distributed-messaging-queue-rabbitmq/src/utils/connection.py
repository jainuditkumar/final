import pika
import time
import socket
from src.config import settings
from src.utils.logger import app_logger as logger

class RabbitMQConnection:
    """
    Handles RabbitMQ connection with automatic reconnection
    """
    
    def __init__(self, host=settings.RABBITMQ_HOST, 
                 port=settings.RABBITMQ_PORT,
                 username=settings.RABBITMQ_USERNAME,
                 password=settings.RABBITMQ_PASSWORD,
                 virtual_host=settings.RABBITMQ_VIRTUAL_HOST,
                 connection_attempts=5,
                 retry_delay=5):
        """
        Initialize RabbitMQ connection parameters
        
        Args:
            host (str): RabbitMQ host
            port (int): RabbitMQ port
            username (str): RabbitMQ username
            password (str): RabbitMQ password
            virtual_host (str): RabbitMQ virtual host
            connection_attempts (int): Max connection attempts
            retry_delay (int): Delay between retry attempts in seconds
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.connection_attempts = connection_attempts
        self.retry_delay = retry_delay
        self.connection = None
        self.channel = None
    
    def connect(self):
        """
        Establish connection to RabbitMQ with retry mechanism
        
        Returns:
            pika.BlockingConnection: RabbitMQ connection
        """
        if self.connection and self.connection.is_open:
            return self.connection

        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        
        for attempt in range(self.connection_attempts):
            try:
                logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt+1}/{self.connection_attempts})...")
                self.connection = pika.BlockingConnection(parameters)
                logger.info("Successfully connected to RabbitMQ")
                return self.connection
            except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
                if attempt < self.connection_attempts - 1:
                    logger.warning(f"Connection attempt {attempt+1} failed: {e}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to connect to RabbitMQ after {self.connection_attempts} attempts: {e}")
                    raise
    
    def get_channel(self, auto_reconnect=True):
        """
        Get or create a channel from the connection
        
        Args:
            auto_reconnect (bool): Whether to reconnect if connection is closed
        
        Returns:
            pika.channel.Channel: RabbitMQ channel
        """
        if self.channel and self.channel.is_open:
            return self.channel
            
        if not self.connection or not self.connection.is_open:
            if auto_reconnect:
                self.connect()
            else:
                raise pika.exceptions.ConnectionClosedError("Connection is closed")
        
        self.channel = self.connection.channel()
        logger.info("Created new channel")
        return self.channel
    
    def close(self):
        """Close the connection and channel"""
        if self.channel and self.channel.is_open:
            logger.info("Closing channel")
            self.channel.close()
        
        if self.connection and self.connection.is_open:
            logger.info("Closing connection")
            self.connection.close()

# Singleton connection instance
connection_manager = RabbitMQConnection()
