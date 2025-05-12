# src/utils/helpers.py
"""
Helper Utilities

This module provides utility functions used throughout the application.
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def ensure_directory_exists(directory_path: str) -> bool:
    """Ensure a directory exists, creating it if necessary.
    
    Args:
        directory_path: Path to the directory
        
    Returns:
        True if directory exists or was created successfully
    """
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"Created directory: {directory_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to create directory {directory_path}: {str(e)}")
        return False

def save_json_to_file(data: Dict[str, Any], file_path: str) -> bool:
    """Save data as JSON to a file.
    
    Args:
        data: Dictionary to save
        file_path: Path where to save the file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved JSON data to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save JSON data to {file_path}: {str(e)}")
        return False

def load_json_from_file(file_path: str) -> Optional[Dict[str, Any]]:
    """Load JSON data from a file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary with loaded data or None if loading failed
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded JSON data from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Failed to load JSON data from {file_path}: {str(e)}")
        return None

def get_formatted_datetime(dt: Optional[datetime] = None, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Get a formatted datetime string.
    
    Args:
        dt: Datetime object to format (uses current time if None)
        format_str: Format string to use
        
    Returns:
        Formatted datetime string
    """
    dt = dt or datetime.now()
    return dt.strftime(format_str)

def clean_filename(filename: str) -> str:
    """Clean a string to be used as a filename.
    
    Args:
        filename: Original filename string
        
    Returns:
        Cleaned filename string
    """
    # Replace invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing whitespace
    filename = filename.strip()
    
    # Ensure filename is not empty
    if not filename:
        return "unnamed_file"
    
    return filename

def truncate_string(text: str, max_length: int = 50) -> str:
    """Truncate a string to a maximum length.
    
    Args:
        text: String to truncate
        max_length: Maximum length
        
    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."