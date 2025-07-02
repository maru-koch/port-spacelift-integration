from loguru import logger
import sys
import os

def setup_logging():
    # Remove default handler
    logger.remove()
    
    # Add console handler with colorized output
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{extra[trace_id]}</cyan> | {message}",
        level="INFO",
        backtrace=True,
        diagnose=True
    )
    
    # Add file handler with JSON format for structured logging
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    logger.add(
        f"{log_dir}/spacelift_integration_{{time:YYYY-MM-DD}}.log",
        format="{time} | {level} | {extra[trace_id]} | {message} | {extra}",
        level="DEBUG",
        rotation="10 MB",
        retention="7 days",
        serialize=True  # JSON output
    )

# Initialize logging on module import
setup_logging()