"""
Main ETL Pipeline module.
Orchestrates the extract, transform, and load operations.
"""

import os
import sys
from typing import Optional, Dict, Any
import pandas as pd

from .extract import CSVExtractor
from .transform import DataTransformer
from .load import SQLiteLoader


class ETLPipeline:
    """Main ETL Pipeline class that orchestrates the entire process."""
    
    def __init__(self, csv_path: str, db_path: str, table_name: str = "transformed_data"):
        """
        Initialize the ETL pipeline.
        
        Args:
            csv_path (str): Path to the input CSV file
            db_path (str): Path to the SQLite database
            table_name (str): Name of the target table
        """
        self.csv_path = csv_path
        self.db_path = db_path
        self.table_name = table_name
        
        self.extractor = CSVExtractor(csv_path)
        self.transformer = DataTransformer()
        self.loader = None
    
    def run(self, transform_config: Optional[Dict[str, Any]] = None, 
            load_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        Run the complete ETL pipeline.
        
        Args:
            transform_config (Dict, optional): Configuration for transformation
            load_config (Dict, optional): Configuration for loading
            
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        try:
            print("=" * 50)
            print("Starting ETL Pipeline")
            print("=" * 50)
            
            # Extract
            print("\n1. EXTRACT PHASE")
            print("-" * 20)
            raw_data = self.extract()
            
            if raw_data is None or raw_data.empty:
                print("No data extracted. Pipeline stopped.")
                return False
            
            # Transform
            print("\n2. TRANSFORM PHASE")
            print("-" * 20)
            transformed_data = self.transform(raw_data, transform_config or {})
            
            # Load
            print("\n3. LOAD PHASE")
            print("-" * 20)
            success = self.load(transformed_data, load_config or {})
            
            if success:
                print("\n" + "=" * 50)
                print("ETL Pipeline completed successfully!")
                print("=" * 50)
                return True
            else:
                print("\nETL Pipeline failed during load phase")
                return False
                
        except Exception as e:
            print(f"\nETL Pipeline failed with error: {str(e)}")
            return False
    
    def extract(self) -> Optional[pd.DataFrame]:
        """
        Extract data from CSV file.
        
        Returns:
            pd.DataFrame or None: Extracted data
        """
        try:
            # Get file info
            file_info = self.extractor.get_file_info()
            if not file_info.get("exists", False):
                print(f"CSV file does not exist: {self.csv_path}")
                return None
            
            print(f"File size: {file_info['size_bytes']} bytes")
            
            # Extract data
            data = self.extractor.extract()
            print(f"Data shape: {data.shape}")
            print(f"Columns: {list(data.columns)}")
            
            return data
            
        except Exception as e:
            print(f"Extract phase failed: {str(e)}")
            return None
    
    def transform(self, data: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Transform the extracted data.
        
        Args:
            data (pd.DataFrame): Raw data from extract phase
            config (Dict): Transformation configuration
            
        Returns:
            pd.DataFrame or None: Transformed data
        """
        try:
            transformed_data = self.transformer.transform(
                data,
                clean=config.get('clean', True),
                add_columns=config.get('add_columns', True),
                filter_conditions=config.get('filter_conditions'),
                aggregate_config=config.get('aggregate_config')
            )
            
            print(f"Transformed data shape: {transformed_data.shape}")
            return transformed_data
            
        except Exception as e:
            print(f"Transform phase failed: {str(e)}")
            return None
    
    def load(self, data: pd.DataFrame, config: Dict[str, Any]) -> bool:
        """
        Load transformed data into SQLite database.
        
        Args:
            data (pd.DataFrame): Transformed data
            config (Dict): Load configuration
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with SQLiteLoader(self.db_path) as loader:
                self.loader = loader
                
                # Load data
                loader.load_data(
                    data, 
                    self.table_name,
                    if_exists=config.get('if_exists', 'replace'),
                    chunk_size=config.get('chunk_size')
                )
                
                # Verify load
                table_info = loader.get_table_info(self.table_name)
                if 'error' not in table_info:
                    print(f"Verification: {table_info['row_count']} rows in table '{self.table_name}'")
                    return True
                else:
                    print(f"Load verification failed: {table_info['error']}")
                    return False
                    
        except Exception as e:
            print(f"Load phase failed: {str(e)}")
            return False
    
    def get_results(self, query: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Query the loaded data from database.
        
        Args:
            query (str, optional): Custom SQL query. If None, selects all from main table.
            
        Returns:
            pd.DataFrame or None: Query results
        """
        try:
            with SQLiteLoader(self.db_path) as loader:
                if query is None:
                    query = f"SELECT * FROM {self.table_name}"
                
                results = loader.execute_query(query)
                return results
                
        except Exception as e:
            print(f"Error querying results: {str(e)}")
            return None


def main():
    """Main function to run the ETL pipeline with sample configuration."""
    
    # Default paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    csv_path = os.path.join(project_root, "data", "sample_data.csv")
    db_path = os.path.join(project_root, "data", "etl_database.db")
    
    # Check if sample data exists, create if not
    if not os.path.exists(csv_path):
        print(f"Sample CSV not found at {csv_path}")
        print("Creating sample data...")
        create_sample_data(csv_path)
    
    # Configuration
    transform_config = {
        'clean': True,
        'add_columns': True,
        # 'filter_conditions': {'status': 'active'},  # Example filter
        # 'aggregate_config': {  # Example aggregation
        #     'group_by': ['category'],
        #     'functions': {'value': 'sum', 'id': 'count'}
        # }
    }
    
    load_config = {
        'if_exists': 'replace',
        'chunk_size': 1000
    }
    
    # Run pipeline
    pipeline = ETLPipeline(csv_path, db_path, "processed_data")
    success = pipeline.run(transform_config, load_config)
    
    if success:
        # Show results
        print("\nSample results:")
        results = pipeline.get_results("SELECT * FROM processed_data LIMIT 10")
        if results is not None:
            print(results.to_string())
    
    return success


def create_sample_data(csv_path: str):
    """Create sample CSV data for testing."""
    import pandas as pd
    
    # Create sample data
    sample_data = {
        'id': range(1, 101),
        'name': [f'Item_{i}' for i in range(1, 101)],
        'category': ['A', 'B', 'C'] * 33 + ['A'],
        'value': [i * 10 + (i % 7) for i in range(1, 101)],
        'status': ['active', 'inactive'] * 50,
        'date': pd.date_range('2024-01-01', periods=100, freq='D')
    }
    
    df = pd.DataFrame(sample_data)
    
    # Add some missing values for testing
    df.loc[5, 'name'] = None
    df.loc[15, 'value'] = None
    df.loc[25, 'category'] = None
    
    # Add some duplicates
    df = pd.concat([df, df.iloc[[10, 20, 30]]], ignore_index=True)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(csv_path, index=False)
    print(f"Sample data created at: {csv_path}")
    print(f"Data shape: {df.shape}")


if __name__ == "__main__":
    main()