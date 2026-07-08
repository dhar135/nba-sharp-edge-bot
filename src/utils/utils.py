# src/utils.py
import logging
from logging.handlers import RotatingFileHandler
import time
import os
import re
import unicodedata
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

_SUFFIX_TOKENS = {"jr", "sr", "ii", "iii", "iv"}


def normalize_name(name):
    """
    Normalizes a player name for cross-feed matching (injuries, Vegas odds, PrizePicks).

    Handles:
      - Diacritics (Dončić -> Doncic) via NFKD decomposition + combining-mark strip
      - Periods and apostrophes (P.J. -> PJ, De'Aaron -> DeAaron)
      - Trailing suffixes (Jr., Sr., II, III, IV)
      - Case and whitespace normalization

    Returns a lowercase, whitespace-collapsed string suitable for dict keys / set membership.
    """
    if not name:
        return ""

    # Strip diacritics: NFKD decomposes accented chars into base char + combining mark,
    # then encoding to ascii and ignoring errors drops the combining marks.
    normalized = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()

    normalized = normalized.lower()
    normalized = normalized.replace(".", "").replace("'", "")

    # Drop trailing suffix tokens (Jr, Sr, II, III, IV)
    tokens = normalized.split()
    while tokens and tokens[-1] in _SUFFIX_TOKENS:
        tokens.pop()

    normalized = " ".join(tokens)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    return normalized


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