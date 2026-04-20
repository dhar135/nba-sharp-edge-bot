# src/utils.py
import logging
from logging.handlers import RotatingFileHandler
import time
import os
from functools import wraps

# 1. Ensure a logs directory exists
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def setup_logger():
    """Configures a dual-output logger (Console + Rotating File)."""
    logger = logging.getLogger("SharpEdge")
    
    # Prevent duplicate handlers if imported multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # File Handler: Max 5MB per file, keep 3 historical backups. 
        # Has detailed formatting (Timestamp, Module, Function)
        file_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, "system.log"), 
            maxBytes=5*1024*1024, 
            backupCount=3
        )
        file_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(module)s:%(funcName)s | %(message)s')
        file_handler.setFormatter(file_formatter)
        
        # Console Handler: Keeps your terminal clean and readable
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('[%(levelname)s] %(message)s')
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
    return logger

# Global logger instance to be imported by other files
logger = setup_logger()

def timer(func):
    """Decorator to track and log the execution time of any function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        
        result = func(*args, **kwargs)
        
        elapsed_time = time.perf_counter() - start_time
        logger.info(f"⏱️ [LATENCY] '{func.__name__}' completed in {elapsed_time:.3f}s")
        
        return result
    return wrapper