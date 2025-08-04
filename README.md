# ZST to CSV Converter

A Python utility for converting Zstandard (.zst) compressed files to CSV format, specifically designed for financial data from Databento.

## Features

- **Single file conversion**: Convert individual ZST files to CSV
- **Batch processing**: Convert multiple ZST files in a directory
- **Memory efficient**: Processes large files using streaming decompression
- **Data validation**: Displays data shape and column information
- **Flexible output**: Automatic or custom output file naming
- **Error handling**: Robust error handling with cleanup
- **Logging**: Comprehensive logging for monitoring progress

## Installation

1. Make sure you have Python 3.7+ installed
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install zstandard pandas
```

## Usage

### Data Conversion

Convert a single ZST file to CSV:

```bash
python scripts/zst_to_csv_conversion.py Data/raw/input_file.csv.zst
```

Specify output file:
```bash
python scripts/zst_to_csv_conversion.py Data/raw/input_file.csv.zst Data/processed/output_file.csv
```

Keep intermediate decompressed file:
```bash
python scripts/zst_to_csv_conversion.py Data/raw/input_file.csv.zst --keep-intermediate
```

Convert all ZST files in a directory:

```bash
python scripts/batch_zst_converter.py /path/to/zst/files
```

Convert with custom output directory:
```bash
python scripts/batch_zst_converter.py /path/to/zst/files --output-dir /path/to/csv/files
```

Recursive search for ZST files:
```bash
python scripts/batch_zst_converter.py /path/to/zst/files --recursive
```

### Database Setup

Run the following script to set up the PostgreSQL database and load data:

```bash
python scripts/setup_database.py
```

### dbt Analytics

Navigate to the `dbt/` directory and run dbt commands (e.g., `dbt run`, `dbt test`).

### Testing

Use the following script to validate the dbt setup:

```bash
python scripts/test_dbt_setup.py
```

## Project Structure

This project is organized into the following directories:

- **dbt/**: Contains the dbt project files, including models, macros, seeds, tests, and configurations.
- **scripts/**: Contains Python scripts for data processing, database setup, and dbt testing.
- **Data/raw/**: Stores raw data files (e.g., ZST files).
- **Data/processed/**: Stores processed data files (e.g., CSV files).

## Environment Variables

Ensure the following environment variables are set:
- `DB_HOST`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `DB_PORT`

Refer to the `.env` file for examples.

## File Structure

```
Finance/
├── Data/
│   ├── GLBX-20250604-8Q67AQFEMK/
│   │   ├── glbx-mdp3-20100606-20250603.ohlcv-1m.csv.zst  # Input ZST file
│   │   ├── metadata.json                                  # Data metadata
│   │   └── ...
│   ├── raw/                                              # Raw data directory
│   │   └── ...                                          # Raw ZST files
│   └── processed/                                       # Processed data directory
│       └── ...                                          # Output CSV files
├── dbt/                                                  # dbt project directory
│   ├── models/                                          # dbt models
│   ├── macros/                                          # dbt macros
│   ├── seeds/                                           # dbt seeds
│   ├── tests/                                           # dbt tests
│   └── dbt_project.yml                                  # dbt project configuration
├── scripts/                                             # Python scripts directory
│   ├── zst_to_csv_conversion.py                        # Main converter
│   ├── batch_zst_converter.py                          # Batch processor
│   ├── setup_database.py                                # Database setup
│   └── test_dbt_setup.py                               # dbt setup testing
├── requirements.txt                                     # Dependencies
└── README.md                                           # This file
```

## Data Format

The converter is optimized for OHLCV (Open, High, Low, Close, Volume) financial data with the following columns:

- `ts_event`: Timestamp
- `rtype`: Record type
- `publisher_id`: Data publisher ID
- `instrument_id`: Financial instrument ID
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Trading volume
- `symbol`: Trading symbol

## Command Line Options

### zst_to_csv_conversion.py

- `input_file`: Input ZST file to convert (required)
- `output_file`: Output CSV file (optional)
- `--keep-intermediate`: Keep the intermediate decompressed file
- `--verbose, -v`: Enable verbose logging

### batch_zst_converter.py

- `input_dir`: Input directory containing ZST files (required)
- `--output-dir, -o`: Output directory for CSV files
- `--recursive, -r`: Search for ZST files recursively
- `--keep-intermediate`: Keep intermediate decompressed files
- `--verbose, -v`: Enable verbose logging

## Performance

The converter is designed for efficiency:

- **Streaming decompression**: Processes files in 16KB chunks to minimize memory usage
- **Large file support**: Can handle multi-gigabyte files without memory issues
- **Progress logging**: Shows conversion progress for large files

Example performance with your data:
- Input: 701.61 MB CSV data (compressed as ZST)
- Processing time: ~3 seconds
- Memory usage: Minimal due to streaming

## Error Handling

The converter includes comprehensive error handling:

- File validation (existence, correct extension)
- Automatic cleanup of partial files on errors
- Detailed error messages and logging
- Graceful handling of corrupted data

## Contributing

Feel free to extend the converter with additional features such as:

- Support for other compression formats
- Data filtering and transformation
- Custom output formats
- Performance optimizations

## License

This project is provided as-is for educational and research purposes.
