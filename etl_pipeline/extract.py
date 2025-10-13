"""
Extract module for the ETL pipeline.
Handles data extraction from CSV files.
"""

import pandas as pd
from typing import Optional
import os


class CSVExtractor:
    """Extracts data from CSV files."""
    
    def __init__(self, file_path: str):
        """
        Initialize the CSV extractor.
        
        Args:
            file_path (str): Path to the CSV file
        """
        self.file_path = file_path
    
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            **kwargs: Additional arguments for pandas.read_csv()
            
        Returns:
            pd.DataFrame: Extracted data
            
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            Exception: If there's an error reading the CSV file
        """
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        
        try:
            data = pd.read_csv(self.file_path, **kwargs)
            print(f"Successfully extracted {len(data)} rows from {self.file_path}")
            return data
        except Exception as e:
            raise Exception(f"Error reading CSV file: {str(e)}")
    
    def get_file_info(self) -> dict:
        """
        Get information about the CSV file.
        
        Returns:
            dict: File information including size and modification time
        """
        if not os.path.exists(self.file_path):
            return {"exists": False}
        
        stat = os.stat(self.file_path)
        return {
            "exists": True,
            "size_bytes": stat.st_size,
            "modified_time": stat.st_mtime,
            "path": self.file_path
        }