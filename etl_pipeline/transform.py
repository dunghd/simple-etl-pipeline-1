"""
Transform module for the ETL pipeline.
Handles data transformation using Pandas.
"""

import pandas as pd
from typing import Dict, List, Optional, Callable


class DataTransformer:
    """Transforms data using various pandas operations."""
    
    def __init__(self):
        """Initialize the data transformer."""
        pass
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the data by removing duplicates and handling missing values.
        
        Args:
            df (pd.DataFrame): Input dataframe
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        print(f"Original data shape: {df.shape}")
        
        # Remove duplicates
        df_cleaned = df.drop_duplicates()
        print(f"After removing duplicates: {df_cleaned.shape}")
        
        # Fill missing values with appropriate defaults
        for column in df_cleaned.columns:
            if df_cleaned[column].dtype == 'object':
                df_cleaned.loc[:, column] = df_cleaned[column].fillna('Unknown')
            else:
                df_cleaned.loc[:, column] = df_cleaned[column].fillna(0)
        
        print(f"Missing values handled. Final shape: {df_cleaned.shape}")
        return df_cleaned
    
    def add_calculated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add calculated columns to the dataframe.
        
        Args:
            df (pd.DataFrame): Input dataframe
            
        Returns:
            pd.DataFrame: Dataframe with additional calculated columns
        """
        df_transformed = df.copy()
        
        # Add timestamp for when the record was processed
        df_transformed['processed_at'] = pd.Timestamp.now()
        
        # Add row ID if not present
        if 'id' not in df_transformed.columns:
            df_transformed['id'] = range(1, len(df_transformed) + 1)
        
        return df_transformed
    
    def filter_data(self, df: pd.DataFrame, conditions: Dict[str, any]) -> pd.DataFrame:
        """
        Filter data based on conditions.
        
        Args:
            df (pd.DataFrame): Input dataframe
            conditions (Dict[str, any]): Dictionary of column:value conditions
            
        Returns:
            pd.DataFrame: Filtered dataframe
        """
        df_filtered = df.copy()
        
        for column, value in conditions.items():
            if column in df_filtered.columns:
                if isinstance(value, list):
                    df_filtered = df_filtered[df_filtered[column].isin(value)]
                else:
                    df_filtered = df_filtered[df_filtered[column] == value]
                print(f"After filtering {column}: {df_filtered.shape}")
        
        return df_filtered
    
    def aggregate_data(self, df: pd.DataFrame, group_by: List[str], 
                      agg_functions: Dict[str, str]) -> pd.DataFrame:
        """
        Aggregate data by grouping columns.
        
        Args:
            df (pd.DataFrame): Input dataframe
            group_by (List[str]): Columns to group by
            agg_functions (Dict[str, str]): Dictionary of column:function for aggregation
            
        Returns:
            pd.DataFrame: Aggregated dataframe
        """
        try:
            df_agg = df.groupby(group_by).agg(agg_functions).reset_index()
            print(f"Data aggregated by {group_by}. Shape: {df_agg.shape}")
            return df_agg
        except Exception as e:
            print(f"Error during aggregation: {str(e)}")
            return df
    
    def transform(self, df: pd.DataFrame, 
                 clean: bool = True,
                 add_columns: bool = True,
                 filter_conditions: Optional[Dict[str, any]] = None,
                 aggregate_config: Optional[Dict] = None) -> pd.DataFrame:
        """
        Apply complete transformation pipeline.
        
        Args:
            df (pd.DataFrame): Input dataframe
            clean (bool): Whether to clean data
            add_columns (bool): Whether to add calculated columns
            filter_conditions (Dict[str, any], optional): Filter conditions
            aggregate_config (Dict, optional): Aggregation configuration
            
        Returns:
            pd.DataFrame: Transformed dataframe
        """
        print("Starting data transformation...")
        
        result = df.copy()
        
        if clean:
            result = self.clean_data(result)
        
        if filter_conditions:
            result = self.filter_data(result, filter_conditions)
        
        if aggregate_config:
            result = self.aggregate_data(
                result, 
                aggregate_config.get('group_by', []),
                aggregate_config.get('functions', {})
            )
        
        if add_columns:
            result = self.add_calculated_columns(result)
        
        print("Data transformation completed!")
        return result