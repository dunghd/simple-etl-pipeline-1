# Simple ETL Pipeline

A simple yet comprehensive ETL (Extract, Transform, Load) pipeline built with Python, using Pandas for data transformation and SQLite for data storage. This project demonstrates how to build a modular ETL system using Poetry for dependency management.

## Features

- **Extract**: Read data from CSV files with error handling and file validation
- **Transform**: Clean data, handle missing values, add calculated columns, filter and aggregate data
- **Load**: Store transformed data in SQLite database with verification
- **Modular Design**: Separate modules for each ETL phase
- **Error Handling**: Comprehensive error handling throughout the pipeline
- **Testing**: Unit tests for all components
- **Configuration**: Flexible configuration options for transformations and loading

## Project Structure

```
simple-etl-pipeline/
├── etl_pipeline/           # Main ETL package
│   ├── __init__.py        # Package initialization
│   ├── extract.py         # Data extraction module
│   ├── transform.py       # Data transformation module
│   ├── load.py           # Data loading module
│   └── main.py           # Main pipeline orchestrator
├── tests/                 # Test suite
│   ├── __init__.py
│   └── test_etl_pipeline.py
├── data/                  # Data directory
│   ├── sample_data.csv    # Sample input data (auto-generated)
│   └── etl_database.db    # SQLite database (created during run)
├── .github/
│   └── copilot-instructions.md
├── pyproject.toml         # Poetry configuration
└── README.md             # This file
```

## Requirements

- Python 3.9+
- Poetry for dependency management

## Installation

1. Clone or navigate to the project directory:

   ```bash
   cd simple-etl-pipeline-1
   ```

2. Install dependencies using Poetry:

   ```bash
   poetry install
   ```

3. Activate the virtual environment:
   ```bash
   poetry shell
   ```

## Usage

### Quick Start

Run the ETL pipeline with sample data:

```bash
poetry run etl-run
```

This will:

1. Create sample CSV data if it doesn't exist
2. Extract data from the CSV file
3. Transform the data (clean, add columns)
4. Load the data into SQLite database
5. Display sample results

### Using the Pipeline Programmatically

```python
from etl_pipeline.main import ETLPipeline

# Initialize pipeline
pipeline = ETLPipeline(
    csv_path="data/input.csv",
    db_path="data/output.db",
    table_name="processed_data"
)

# Configure transformations
transform_config = {
    'clean': True,
    'add_columns': True,
    'filter_conditions': {'status': 'active'},
    'aggregate_config': {
        'group_by': ['category'],
        'functions': {'value': 'sum', 'id': 'count'}
    }
}

# Configure loading
load_config = {
    'if_exists': 'replace',
    'chunk_size': 1000
}

# Run pipeline
success = pipeline.run(transform_config, load_config)

if success:
    # Query results
    results = pipeline.get_results("SELECT * FROM processed_data LIMIT 10")
    print(results)
```

### Individual Components

You can also use individual components separately:

```python
from etl_pipeline.extract import CSVExtractor
from etl_pipeline.transform import DataTransformer
from etl_pipeline.load import SQLiteLoader

# Extract
extractor = CSVExtractor("data/input.csv")
data = extractor.extract()

# Transform
transformer = DataTransformer()
clean_data = transformer.clean_data(data)
transformed_data = transformer.add_calculated_columns(clean_data)

# Load
with SQLiteLoader("data/output.db") as loader:
    loader.load_data(transformed_data, "my_table")
```

## Configuration Options

### Transform Configuration

- `clean`: Remove duplicates and handle missing values
- `add_columns`: Add calculated columns (id, processed_at)
- `filter_conditions`: Dictionary of column:value filters
- `aggregate_config`: Configuration for data aggregation

### Load Configuration

- `if_exists`: What to do if table exists ('replace', 'append', 'fail')
- `chunk_size`: Size of chunks for batch loading

## Testing

Run the test suite:

```bash
poetry run pytest
```

Run tests with coverage:

```bash
poetry run pytest --cov=etl_pipeline
```

## Sample Data

The pipeline automatically creates sample data with:

- 100+ records with some duplicates
- Multiple categories (A, B, C)
- Status fields (active, inactive)
- Missing values for testing data cleaning
- Date fields for time-based operations

## Database Schema

The transformed data is stored in SQLite with the following additional columns:

- `id`: Auto-generated row ID (if not present in source)
- `processed_at`: Timestamp of when the record was processed

## Error Handling

The pipeline includes comprehensive error handling:

- File existence validation
- Database connection errors
- Data transformation errors
- SQL execution errors

All errors are logged with descriptive messages to help with debugging.

## Development

### Adding New Transformations

1. Add methods to the `DataTransformer` class in `transform.py`
2. Update the `transform` method to include new options
3. Add configuration options to the transform_config
4. Write tests for new functionality

### Adding New Data Sources

1. Create a new extractor class following the pattern in `extract.py`
2. Implement the required methods (`extract`, `get_file_info`)
3. Update the main pipeline to use the new extractor
4. Add tests for the new extractor

### Adding New Destinations

1. Create a new loader class following the pattern in `load.py`
2. Implement the required methods (`connect`, `load_data`, etc.)
3. Update the main pipeline to use the new loader
4. Add tests for the new loader

## Performance Considerations

- Use `chunk_size` parameter for large datasets
- Consider using appropriate pandas dtypes for memory efficiency
- Use database indexes for frequently queried columns
- Monitor memory usage during transformations

## License

This project is for educational purposes. Feel free to use and modify as needed.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Troubleshooting

### Common Issues

1. **Module not found errors**: Make sure you're in the Poetry virtual environment (`poetry shell`)
2. **File permission errors**: Check that the data directory is writable
3. **SQLite locked errors**: Ensure no other processes are using the database file
4. **Memory errors with large files**: Use the `chunk_size` parameter in load configuration

### Getting Help

Check the error messages in the console output. The pipeline provides detailed error information to help diagnose issues.

## Running with Airflow (local development)

If you'd like to orchestrate this pipeline with Apache Airflow for scheduled runs, follow these steps to run Airflow locally using a virtual environment and Poetry.

Prerequisites:

- Python 3.9+
- Poetry

1. Create or activate a Python virtual environment (optional but recommended). You can use your system virtualenv or rely on Poetry's virtual environment:

```bash
# From the project root
poetry install
poetry shell
```

2. Install Airflow into the same environment. Airflow has specific installation constraints; use the official constraints method. Example (adjust versions as needed):

```bash
# Set Airflow version and constraint URL. Replace X.Y.Z with a stable Airflow release.
AIRFLOW_VERSION=2.7.1
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

3. Point Airflow to this repository's `dags/` folder. You can either copy the `dags/` folder into your Airflow home `dags/` directory or configure Airflow to use this folder. Example using environment variable:

```bash
# Set AIRFLOW_HOME to a local folder (optional)
export AIRFLOW_HOME="$HOME/airflow"

# Configure Airflow to use the project's dags folder
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"

# Initialize the metadata database
airflow db init
```

4. Start the scheduler and webserver in separate terminals (or use `airflow standalone` for quick testing):

```bash
# Start scheduler
airflow scheduler

# In another terminal, start webserver
airflow webserver --port 8080

# Alternatively for a quick local dev environment
airflow standalone
```

5. Open the Airflow UI at http://localhost:8080, find the DAG `simple_etl_pipeline`, and trigger it manually to test. The DAG runs daily by default.

Notes and recommendations:

- The DAG uses PythonOperator to call the small wrappers in `etl_pipeline/airflow_tasks.py`. Those wrappers use a temporary pickle file at `data/etl_intermediate.pkl` to pass the dataframe between tasks for local testing. Replace this with a cloud object store (S3/GCS) or a staging table for production.
- Avoid storing large pandas DataFrames in XCom. For production, persist intermediate data to an object store or the database and pass references via XCom.
- Consider running Airflow inside Docker Compose for an isolated, reproducible environment. The official Airflow repo includes a Docker Compose example.
- If you plan to scale or operate in production, create a dedicated DAGs repository or package the DAGs with pinning of dependencies and CI/CD deployment to your Airflow environment.

Example: convert intermediate persistence to CSV and use XCom to pass path (recommended change):

1. Write transformed data to `data/transformed.csv` in `transform_task`.
2. Push the file path to XCom (Airflow will store the string, not the DataFrame).
3. In `load_task`, pull the file path from XCom and load the CSV.

This reduces memory usage and avoids XCom payload limits.
