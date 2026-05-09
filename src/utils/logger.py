import logging
import sys

def get_logger(name:str) -> logging.Logger:
    """
    Creates and returns a configured logger instance
    """

    logger = logging.getLogger(name)

    # Only configure if it hasn't been configured yet to avoid duplicate logs
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
    
    return logger