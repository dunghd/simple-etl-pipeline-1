"""
Tests for the ETL pipeline components.
"""

import pytest
import pandas as pd
import os
import tempfile
import sqlite3
from unittest.mock import patch, MagicMock

from etl_pipeline.extract import CSVExtractor
from etl_pipeline.transform import DataTransformer
from etl_pipeline.load import SQLiteLoader
from etl_pipeline.main import ETLPipeline


class TestCSVExtractor:
    """Test cases for CSV extractor."""
    
    def test_extract_valid_csv(self):
        """Test extracting data from a valid CSV file."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,value\n1,John,100\n2,Jane,200\n")
            temp_path = f.name
        
        try:
            extractor = CSVExtractor(temp_path)
            data = extractor.extract()
            
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 2
            assert list(data.columns) == ['id', 'name', 'value']
        finally:
            os.unlink(temp_path)
    
    def test_extract_nonexistent_file(self):
        """Test extracting from non-existent file."""
        extractor = CSVExtractor("nonexistent.csv")
        
        with pytest.raises(FileNotFoundError):
            extractor.extract()
    
    def test_get_file_info(self):
        """Test getting file information."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name\n1,test\n")
            temp_path = f.name
        
        try:
            extractor = CSVExtractor(temp_path)
            info = extractor.get_file_info()
            
            assert info['exists'] is True
            assert 'size_bytes' in info
            assert 'modified_time' in info
        finally:
            os.unlink(temp_path)


class TestDataTransformer:
    """Test cases for data transformer."""
    
    def test_clean_data(self):
        """Test data cleaning functionality."""
        # Create test data with duplicates and missing values
        data = pd.DataFrame({
            'id': [1, 2, 2, 3, 4],
            'name': ['A', 'B', 'B', None, 'D'],
            'value': [10, 20, 20, 30, None]
        })
        
        transformer = DataTransformer()
        cleaned = transformer.clean_data(data)
        
        # Check duplicates removed
        assert len(cleaned) == 4
        
        # Check missing values handled
        assert cleaned['name'].isna().sum() == 0
        assert cleaned['value'].isna().sum() == 0
    
    def test_add_calculated_columns(self):
        """Test adding calculated columns."""
        data = pd.DataFrame({
            'name': ['A', 'B', 'C'],
            'value': [10, 20, 30]
        })
        
        transformer = DataTransformer()
        result = transformer.add_calculated_columns(data)
        
        assert 'processed_at' in result.columns
        assert 'id' in result.columns
        assert len(result) == 3
    
    def test_filter_data(self):
        """Test data filtering."""
        data = pd.DataFrame({
            'category': ['A', 'B', 'A', 'C'],
            'status': ['active', 'inactive', 'active', 'active'],
            'value': [10, 20, 30, 40]
        })
        
        transformer = DataTransformer()
        
        # Test single condition
        filtered = transformer.filter_data(data, {'category': 'A'})
        assert len(filtered) == 2
        
        # Test multiple conditions
        filtered = transformer.filter_data(data, {'category': 'A', 'status': 'active'})
        assert len(filtered) == 2


class TestSQLiteLoader:
    """Test cases for SQLite loader."""
    
    def test_load_data(self):
        """Test loading data into SQLite."""
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [10, 20, 30]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            with SQLiteLoader(db_path) as loader:
                loader.load_data(data, 'test_table')
                
                # Verify data was loaded
                result = loader.execute_query("SELECT * FROM test_table")
                assert len(result) == 3
                assert list(result.columns) == ['id', 'name', 'value']
        finally:
            os.unlink(db_path)
    
    def test_get_table_info(self):
        """Test getting table information."""
        data = pd.DataFrame({
            'id': [1, 2],
            'name': ['A', 'B']
        })
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            with SQLiteLoader(db_path) as loader:
                loader.load_data(data, 'test_table')
                info = loader.get_table_info('test_table')
                
                assert info['table_name'] == 'test_table'
                assert info['row_count'] == 2
                assert len(info['columns']) == 2
        finally:
            os.unlink(db_path)


class TestETLPipeline:
    """Test cases for the complete ETL pipeline."""
    
    def test_pipeline_run(self):
        """Test running the complete pipeline."""
        # Create temporary CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,category,value\n1,A,X,10\n2,B,Y,20\n3,C,X,30\n")
            csv_path = f.name
        
        # Create temporary database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            pipeline = ETLPipeline(csv_path, db_path, 'test_results')
            success = pipeline.run()
            
            assert success is True
            
            # Verify results
            results = pipeline.get_results()
            assert results is not None
            assert len(results) == 3
            assert 'processed_at' in results.columns
            
        finally:
            os.unlink(csv_path)
            os.unlink(db_path)