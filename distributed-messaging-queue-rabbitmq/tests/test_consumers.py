import unittest
import json
from unittest.mock import MagicMock, patch
from src.consumers.consumer_direct import DirectConsumer
from src.consumers.consumer_topic import TopicConsumer
from src.consumers.consumer_fanout import FanoutConsumer
from src.config import settings

class TestDirectConsumer(unittest.TestCase):
    """Test cases for DirectConsumer"""

    def setUp(self):
        """Set up test environment"""
        self.consumer = DirectConsumer()
        # Mock the connection
        self.consumer.connection = MagicMock()
        self.channel = MagicMock()
        self.consumer.connection.get_channel.return_value = self.channel
    
    def test_setup(self):
        """Test exchange and queue setup"""
        self.consumer.setup()
        
        # Verify exchange declared
        self.channel.exchange_declare.assert_called_with(
            exchange=settings.EXCHANGE_DIRECT,
            exchange_type='direct',
            durable=True
        )
        
        # Verify queue declared
        self.channel.queue_declare.assert_called_with(
            queue=settings.QUEUE_DIRECT,
            durable=True
        )
        
        # Verify binding
        self.channel.queue_bind.assert_called_with(
            queue=settings.QUEUE_DIRECT,
            exchange=settings.EXCHANGE_DIRECT,
            routing_key=settings.ROUTING_KEY_DIRECT
        )
    
    def test_process_message_success(self):
        """Test successful message processing"""
        message = json.dumps({"id": 123, "action": "test"})
        result = self.consumer.process_message(message)
        self.assertTrue(result)
    
    def test_process_message_failure(self):
        """Test failed message processing"""
        # Invalid JSON should cause the processing to fail
        message = "Not a valid JSON"
        result = self.consumer.process_message(message)
        self.assertFalse(result)
    
    @patch('src.consumers.consumer_direct.DirectConsumer.process_message')
    def test_handle_message_success(self, mock_process):
        """Test message handler with successful processing"""
        # Setup
        mock_process.return_value = True
        method = MagicMock()
        method.delivery_tag = "test_tag"
        method.routing_key = settings.ROUTING_KEY_DIRECT
        properties = MagicMock()
        properties.message_id = "test_id"
        body = json.dumps({"test": "data"}).encode('utf-8')
        
        # Execute
        self.consumer.handle_message(self.channel, method, properties, body)
        
        # Verify
        mock_process.assert_called_once()
        self.channel.basic_ack.assert_called_with(delivery_tag="test_tag")
    
    @patch('src.consumers.consumer_direct.DirectConsumer.process_message')
    def test_handle_message_failure(self, mock_process):
        """Test message handler with failed processing"""
        # Setup
        mock_process.return_value = False
        method = MagicMock()
        method.delivery_tag = "test_tag"
        method.routing_key = settings.ROUTING_KEY_DIRECT
        properties = MagicMock()
        properties.message_id = "test_id"
        body = json.dumps({"test": "data"}).encode('utf-8')
        
        # Execute
        self.consumer.handle_message(self.channel, method, properties, body)
        
        # Verify
        mock_process.assert_called_once()
        self.channel.basic_nack.assert_called_with(delivery_tag="test_tag", requeue=True)


class TestTopicConsumer(unittest.TestCase):
    """Test cases for TopicConsumer"""
    
    def setUp(self):
        """Set up test environment"""
        self.consumer = TopicConsumer(settings.QUEUE_TOPIC_1, "orders.new")
        # Mock the connection
        self.consumer.connection = MagicMock()
        self.channel = MagicMock()
        self.consumer.connection.get_channel.return_value = self.channel
    
    def test_setup(self):
        """Test exchange and queue setup"""
        self.consumer.setup()
        
        # Verify exchange declared
        self.channel.exchange_declare.assert_called_with(
            exchange=settings.EXCHANGE_TOPIC,
            exchange_type='topic',
            durable=True
        )
        
        # Verify queue declared
        self.channel.queue_declare.assert_called_with(
            queue=settings.QUEUE_TOPIC_1,
            durable=True
        )
        
        # Verify binding
        self.channel.queue_bind.assert_called_with(
            queue=settings.QUEUE_TOPIC_1,
            exchange=settings.EXCHANGE_TOPIC,
            routing_key="orders.new"
        )
    
    def test_process_message(self):
        """Test message processing based on routing key"""
        message = json.dumps({"id": 123, "action": "create"})
        result = self.consumer.process_message(message, "orders.new")
        self.assertTrue(result)


class TestFanoutConsumer(unittest.TestCase):
    """Test cases for FanoutConsumer"""
    
    def setUp(self):
        """Set up test environment"""
        self.consumer = FanoutConsumer(settings.QUEUE_FANOUT_1)
        # Mock the connection
        self.consumer.connection = MagicMock()
        self.channel = MagicMock()
        self.consumer.connection.get_channel.return_value = self.channel
    
    def test_setup(self):
        """Test exchange and queue setup"""
        self.consumer.setup()
        
        # Verify exchange declared
        self.channel.exchange_declare.assert_called_with(
            exchange=settings.EXCHANGE_FANOUT,
            exchange_type='fanout',
            durable=True
        )
        
        # Verify queue declared
        self.channel.queue_declare.assert_called_with(
            queue=settings.QUEUE_FANOUT_1,
            durable=True
        )
        
        # Verify binding (no routing key for fanout)
        self.channel.queue_bind.assert_called_with(
            queue=settings.QUEUE_FANOUT_1,
            exchange=settings.EXCHANGE_FANOUT
        )
    
    def test_process_message(self):
        """Test broadcast message processing"""
        message = json.dumps({"id": 123, "broadcast": "system update"})
        result = self.consumer.process_message(message)
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
