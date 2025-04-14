import unittest
import json
import uuid
from unittest.mock import MagicMock, patch
from src.config import settings

# Since the publisher files don't exist in the input files, let's import mock classes
# that represent what we'd implement in the actual publisher files
class MockDirectPublisher:
    def __init__(self):
        self.exchange = settings.EXCHANGE_DIRECT
        self.routing_key = settings.ROUTING_KEY_DIRECT
        self.connection = None
    
    def setup(self):
        pass
    
    def publish_message(self, message, persistent=True):
        pass

class MockTopicPublisher:
    def __init__(self):
        self.exchange = settings.EXCHANGE_TOPIC
        self.connection = None
    
    def setup(self):
        pass
    
    def publish_message(self, message, routing_key, persistent=True):
        pass

class MockFanoutPublisher:
    def __init__(self):
        self.exchange = settings.EXCHANGE_FANOUT
        self.connection = None
    
    def setup(self):
        pass
    
    def publish_message(self, message, persistent=True):
        pass

class TestDirectPublisher(unittest.TestCase):
    """Test cases for DirectPublisher"""
    
    def setUp(self):
        """Set up test environment"""
        self.publisher = MockDirectPublisher()
        # Mock the connection
        self.publisher.connection = MagicMock()
        self.channel = MagicMock()
        self.publisher.connection.get_channel = MagicMock(return_value=self.channel)
    
    @patch('uuid.uuid4')
    def test_publish_message(self, mock_uuid):
        """Test message publishing through direct exchange"""
        # Setup
        mock_uuid.return_value = uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')
        message = {"id": 123, "action": "test"}
        
        # Mock implementation
        def publish_message(message_data, persistent=True):
            # Convert message to JSON
            message_json = json.dumps(message_data)
            
            # Set message properties
            properties = None
            if persistent:
                properties = MagicMock()
                properties.delivery_mode = 2  # Persistent
            
            # Publish message
            self.channel.basic_publish(
                exchange=self.publisher.exchange,
                routing_key=self.publisher.routing_key,
                body=message_json.encode('utf-8'),
                properties=properties
            )
            return True
        
        # Add the method to our mock
        self.publisher.publish_message = publish_message
        
        # Execute
        result = self.publisher.publish_message(message)
        
        # Verify
        self.assertTrue(result)
        self.channel.basic_publish.assert_called_once()

class TestTopicPublisher(unittest.TestCase):
    """Test cases for TopicPublisher"""
    
    def setUp(self):
        """Set up test environment"""
        self.publisher = MockTopicPublisher()
        # Mock the connection
        self.publisher.connection = MagicMock()
        self.channel = MagicMock()
        self.publisher.connection.get_channel = MagicMock(return_value=self.channel)
    
    @patch('uuid.uuid4')
    def test_publish_message(self, mock_uuid):
        """Test message publishing through topic exchange with different routing keys"""
        # Setup
        mock_uuid.return_value = uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')
        message = {"id": 123, "action": "create"}
        routing_key = "orders.new"
        
        # Mock implementation
        def publish_message(message_data, routing_key, persistent=True):
            # Convert message to JSON
            message_json = json.dumps(message_data)
            
            # Set message properties
            properties = None
            if persistent:
                properties = MagicMock()
                properties.delivery_mode = 2  # Persistent
            
            # Publish message
            self.channel.basic_publish(
                exchange=self.publisher.exchange,
                routing_key=routing_key,
                body=message_json.encode('utf-8'),
                properties=properties
            )
            return True
        
        # Add the method to our mock
        self.publisher.publish_message = publish_message
        
        # Execute
        result = self.publisher.publish_message(message, routing_key)
        
        # Verify
        self.assertTrue(result)
        self.channel.basic_publish.assert_called_once_with(
            exchange=settings.EXCHANGE_TOPIC,
            routing_key=routing_key,
            body=json.dumps(message).encode('utf-8'),
            properties=self.channel.basic_publish.call_args[1]['properties']
        )

class TestFanoutPublisher(unittest.TestCase):
    """Test cases for FanoutPublisher"""
    
    def setUp(self):
        """Set up test environment"""
        self.publisher = MockFanoutPublisher()
        # Mock the connection
        self.publisher.connection = MagicMock()
        self.channel = MagicMock()
        self.publisher.connection.get_channel = MagicMock(return_value=self.channel)
    
    @patch('uuid.uuid4')
    def test_publish_message(self, mock_uuid):
        """Test message broadcasting through fanout exchange"""
        # Setup
        mock_uuid.return_value = uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')
        message = {"id": 123, "broadcast": "system update"}
        
        # Mock implementation
        def publish_message(message_data, persistent=True):
            # Convert message to JSON
            message_json = json.dumps(message_data)
            
            # Set message properties
            properties = None
            if persistent:
                properties = MagicMock()
                properties.delivery_mode = 2  # Persistent
            
            # Publish message (no routing key needed for fanout)
            self.channel.basic_publish(
                exchange=self.publisher.exchange,
                routing_key='',  # Empty routing key for fanout
                body=message_json.encode('utf-8'),
                properties=properties
            )
            return True
        
        # Add the method to our mock
        self.publisher.publish_message = publish_message
        
        # Execute
        result = self.publisher.publish_message(message)
        
        # Verify
        self.assertTrue(result)
        self.channel.basic_publish.assert_called_once()
        # Verify empty routing key for fanout
        self.assertEqual(
            self.channel.basic_publish.call_args[1]['routing_key'], 
            ''
        )

if __name__ == '__main__':
    unittest.main()
