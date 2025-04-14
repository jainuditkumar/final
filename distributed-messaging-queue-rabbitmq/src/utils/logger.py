import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Set up logger with console and file handlers
    
    Args:
        name (str): Logger name
        log_file (str, optional): Log file path. Defaults to None.
        level (int, optional): Logging level. Defaults to logging.INFO.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log file is specified
    if log_file:
        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Set up rotating file handler (10MB max size, keep 5 backups)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Default application logger
app_logger = setup_logger(
    'rabbitmq_app', 
    log_file='logs/app.log'
)

# Consumer logger
consumer_logger = setup_logger(
    'rabbitmq_consumer', 
    log_file='logs/consumer.log'
)

# Producer logger
producer_logger = setup_logger(
    'rabbitmq_producer', 
    log_file='logs/producer.log'
)
