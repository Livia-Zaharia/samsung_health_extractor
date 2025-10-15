# Samsung Health Extractor

A modern, type-safe script to generate CSV files from Samsung Health data exports.

## Features

- 🚀 Built with modern Python tooling (uv, polars, typer)
- 🔒 Fully type-hinted for better IDE support and code safety
- ⚡ Fast data processing with Polars
- 📊 Configurable data combinations and filtering
- 🪵 Comprehensive logging with Eliot
- 🎯 CLI interface with Typer

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

### Install uv (if not already installed)

```bash
# On macOS and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Install dependencies

```bash
# Create a virtual environment and install dependencies
uv sync
```

## Usage

### Step 1: Download Your Data

Download your data from the Samsung Health app in the settings. It will generate a folder on your phone's storage (usually named like `samsunghealth_username_YYYYMMDDHHMMSS`).

### Step 2: Place the Data

Place the exported folder inside a `Samsung Health` directory at the root of this project:

```
samsung_health_extractor/
├── Samsung Health/
│   └── samsunghealth_username_20241009172448/
│       ├── *.csv files
│       └── ...
├── samsung_health_extractor.py
├── data_combination.json
├── ignore_csvs.txt
└── ...
```

### Step 3: Run the Script

```bash
# Run with default settings
uv run samsung-health-extract

# Or run directly with Python
uv run python samsung_health_extractor.py

# Specify a custom data path
uv run samsung-health-extract "./path/to/Samsung Health"

# Specify a custom log file
uv run samsung-health-extract --log-file "./my-log.log"
```

Logs are automatically saved to the `logs/` directory with timestamps (e.g., `logs/samsung_health_extract_20241015_143022.log`). Each log file contains detailed structured logging in JSON format for easy parsing and debugging.

## Configuration

### Ignore List (`ignore_csvs.txt`)

This file contains CSV file names to ignore during processing. Files listed here are typically:
- Empty or summary files
- Samsung's own evaluations
- Data you don't need

Edit this file to include or exclude specific CSV files. Lines starting with `#` are treated as comments.

### Data Combination (`data_combination.json`)

This JSON file describes how to combine multiple CSV files into unified datasets. You can:
- Define which CSV files to process
- Specify columns to include from each source
- Rename columns for consistency
- Configure merge keys for combining data
- Set output formats and sorting

#### Example Configuration Structure:

```json
{
  "csv_filtering": {
    "enabled_csvs": [
      ".cycle.daily_temperature",
      ".cycle.flow"
    ]
  },
  "data_combinations": {
    "cycle_complete": {
      "output_file": "complete_cycle_data.csv",
      "sources": [
        {
          "csv_name": ".cycle.daily_temperature",
          "priority": 1,
          "merge_key": "update_time",
          "merge_key_rename": "day_time",
          "required": true,
          "columns_to_include": ["create_time", "update_time"],
          "rename_columns": {
            "create_time": "temperature"
          }
        }
      ],
      "output_structure": {
        "primary_sort": "day_time",
        "sort_ascending": true,
        "final_columns": ["day_time", "temperature"],
        "data_processing": {
          "fill_missing_values": "N/A"
        }
      }
    }
  }
}
```

## Technical Details

### Technology Stack

- **[Polars](https://pola.rs/)**: Fast DataFrame library for efficient data processing
- **[Typer](https://typer.tiangolo.com/)**: Modern CLI framework
- **[Eliot](https://eliot.readthedocs.io/)**: Structured logging for better debugging
- **[uv](https://github.com/astral-sh/uv)**: Fast Python package installer and resolver
- **Type Hints**: Full type annotations for better code quality and IDE support
- **Pathlib**: Modern path handling instead of os.path

### How It Works

1. **File Discovery**: Scans the Samsung Health export directory for CSV files
2. **Filtering**: Applies ignore list and data availability checks
3. **Processing**: Reads enabled CSV files and combines them according to configuration
4. **Output**: Generates combined CSV files with processed data
5. **Logging**: All operations are logged to timestamped files in the `logs/` directory

The Samsung Health export structure is complex, but this tool extracts the essential timestamps and values in readable formats, then concatenates data from various sources based on your configuration.

### Logging

The script uses Eliot for structured logging. Each run creates a timestamped log file in the `logs/` directory:
- **Location**: `logs/samsung_health_extract_YYYYMMDD_HHMMSS.log`
- **Format**: JSON (one JSON object per line for easy parsing)
- **Content**: Detailed information about file processing, filtering, data combination, and any errors

You can use tools like `jq` to filter and analyze logs:
```bash
# View all error messages
cat logs/samsung_health_extract_*.log | jq 'select(.message_type == "error")'

# View filtered files
cat logs/samsung_health_extract_*.log | jq 'select(.message_type | startswith("filtered"))'
```

## Development

### Type Checking

```bash
uv run mypy samsung_health_extractor.py
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

