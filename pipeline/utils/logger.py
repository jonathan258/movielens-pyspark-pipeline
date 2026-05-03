"""
logger.py
---------
Centralized logging configuration for the MovieLens pipeline.

Why logging over print()?
- print() gives you no context — you don't know when it happened,
  how serious it was, or which module it came from
- logging gives you: timestamp, severity level, module name, message
- In production, logs are written to files and monitoring systems
- You can control log level: DEBUG, INFO, WARNING, ERROR, CRITICAL

Log Levels (lowest to highest severity):
  DEBUG    → detailed diagnostic info, only during development
  INFO     → confirmation things are working as expected
  WARNING  → something unexpected but pipeline still running
  ERROR    → serious problem, something failed
  CRITICAL → pipeline cannot continue
"""

import logging
import os
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a configured logger instance.
    
    Args:
        name (str): Name of the module requesting the logger.
                    Best practice: pass __name__ so the logger
                    is named after the module it's used in.
    
    Returns:
        logging.Logger: Configured logger instance
    
    Example usage:
        from utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Pipeline started")
        logger.error("Failed to read file")
    """

    # Create logs directory if it doesn't exist
    # exist_ok=True means no error if folder already exists
    os.makedirs("logs", exist_ok=True)

    # Create a logger with the given name
    logger = logging.getLogger(name)

    # Set minimum level — DEBUG means capture everything
    # In production you'd set this to INFO or WARNING
    logger.setLevel(logging.DEBUG)

    # ── FORMATTERS ────────────────────────────────────────────
    # Format defines what each log line looks like
    # %(asctime)s    → timestamp
    # %(name)s       → logger name (module name)
    # %(levelname)s  → DEBUG/INFO/WARNING/ERROR/CRITICAL
    # %(message)s    → the actual message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # ── CONSOLE HANDLER ───────────────────────────────────────
    # Prints logs to the terminal so you can see them while running
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Only INFO and above in terminal
    console_handler.setFormatter(formatter)

    # ── FILE HANDLER ──────────────────────────────────────────
    # Writes logs to a file so you have a permanent record
    # Named with today's date so each day gets its own log file
    log_filename = f"logs/pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)  # Everything including DEBUG in file
    file_handler.setFormatter(formatter)

    # Add both handlers to the logger
    # Only add if not already added (prevents duplicate logs)
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger