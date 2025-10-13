"""
Load module for the ETL pipeline.
Handles data loading into SQLite database.
"""

import sqlite3
import pandas as pd
from typing import Optional, List, Dict
import os


class SQLiteLoader:
    """Loads data into SQLite database."""
    
    def __init__(self, db_path: str):
        """
        Initialize the SQLite loader.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """Establish connection to SQLite database."""
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"Connected to SQLite database: {self.db_path}")
        except Exception as e:
            raise Exception(f"Error connecting to database: {str(e)}")
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def create_table(self, table_name: str, df: pd.DataFrame, 
                    if_exists: str = 'replace'):
        """
        Create table structure based on dataframe.
        
        Args:
            table_name (str): Name of the table
            df (pd.DataFrame): Dataframe to base table structure on
            if_exists (str): What to do if table exists ('replace', 'append', 'fail')
        """
        if not self.connection:
            self.connect()
        
        try:
            # Create an empty table with the same structure
            df.head(0).to_sql(table_name, self.connection, 
                             if_exists=if_exists, index=False)
            print(f"Table '{table_name}' created successfully")
        except Exception as e:
            raise Exception(f"Error creating table: {str(e)}")
    
    def load_data(self, df: pd.DataFrame, table_name: str, 
                 if_exists: str = 'replace', chunk_size: Optional[int] = None):
        """
        Load dataframe into SQLite table.
        
        Args:
            df (pd.DataFrame): Data to load
            table_name (str): Target table name
            if_exists (str): What to do if table exists ('replace', 'append', 'fail')
            chunk_size (int, optional): Size of chunks for batch loading
        """
        if not self.connection:
            self.connect()
        
        try:
            df.to_sql(
                table_name, 
                self.connection, 
                if_exists=if_exists, 
                index=False,
                chunksize=chunk_size
            )
            print(f"Successfully loaded {len(df)} rows into table '{table_name}'")
        except Exception as e:
            raise Exception(f"Error loading data: {str(e)}")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as dataframe.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pd.DataFrame: Query results
        """
        if not self.connection:
            self.connect()
        
        try:
            result = pd.read_sql_query(query, self.connection)
            print(f"Query executed successfully, returned {len(result)} rows")
            return result
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")
    
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get information about a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            Dict: Table information including columns and row count
        """
        if not self.connection:
            self.connect()
        
        try:
            # Get table schema
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            return {
                "table_name": table_name,
                "columns": columns_info,
                "row_count": row_count
            }
        except Exception as e:
            return {"error": str(e)}
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List[str]: List of table names
        """
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            return tables
        except Exception as e:
            print(f"Error listing tables: {str(e)}")
            return []
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()