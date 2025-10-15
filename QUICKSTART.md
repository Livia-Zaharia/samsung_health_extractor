# Quick Start Guide

Get up and running with the Samsung Health Extractor in 5 minutes!

## Prerequisites

- Python 3.10 or higher
- Samsung Health data export

## Installation

### Step 1: Install uv

Choose your platform:

**macOS/Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Step 2: Install Dependencies

```bash
cd samsung_health_extractor
uv sync
```

This will:
- Create a virtual environment (`.venv`)
- Install all required dependencies (polars, typer, eliot)

## Usage

### Basic Usage

```bash
# Run with default settings (looks for ./Samsung Health directory)
uv run samsung-health-extract

# Or run directly with Python
uv run python samsung_health_extractor.py
```

### Custom Data Path

```bash
# Specify a custom path to your Samsung Health data
uv run samsung-health-extract "./path/to/Samsung Health"
```

### Custom Log File

```bash
# Specify where to save the log file
uv run samsung-health-extract --log-file "./my-extraction.log"
```

**Note:** By default, logs are saved to `logs/samsung_health_extract_YYYYMMDD_HHMMSS.log`

### Get Help

```bash
uv run samsung-health-extract --help
```

## Project Structure

```
samsung_health_extractor/
├── Samsung Health/              # Place your exported data here
│   └── samsunghealth_username_YYYYMMDDHHMMSS/
│       └── *.csv files
├── logs/                       # Auto-generated log files (ignored by git)
│   └── samsung_health_extract_YYYYMMDD_HHMMSS.log
├── samsung_health_extractor.py  # Main script
├── data_combination.json        # Configuration for combining data
├── ignore_csvs.txt             # Files to ignore
├── pyproject.toml              # Project dependencies
└── README.md                   # Full documentation
```

## Configuration

### 1. Ignore List (`ignore_csvs.txt`)

Edit this file to skip processing certain CSV files:

```text
# Files to ignore
.device_profile
.user_profile
.badge
```

### 2. Data Combinations (`data_combination.json`)

Configure how to combine multiple CSV files:

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
      "sources": [...]
    }
  }
}
```

## Output

The script generates CSV files based on your configuration in `data_combination.json`.

Example output:
```
📝 Logging to: logs/samsung_health_extract_20241015_143022.log
✅ Data extraction completed successfully!
📋 Check log file for details: logs/samsung_health_extract_20241015_143022.log
```

Generated files (based on default configuration):
- `complete_cycle_data.csv` - Menstrual cycle data (temperature, flow, sexual activity)
- `complete_tracker_data_hr_steps_spo2.csv` - Tracker data (heart rate, steps, SpO2)
- `complete_sleep_weight.csv` - Sleep and weight data with calculated sleep duration
- `complete_exercise_respiratory_rate.csv` - Exercise and respiratory rate data
- Timestamped log file in `logs/` directory with detailed processing information

## Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Check [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) if upgrading from v1
- Customize `data_combination.json` for your specific needs

## Troubleshooting

### "No module named 'polars'"

**Solution:** Run `uv sync` to install dependencies

### "No dump directories found"

**Solution:** Ensure your Samsung Health export folder is in `./Samsung Health/`

### Type errors

**Solution:** Ensure you're using Python 3.10+
```bash
python --version
```

## Examples

### Example 1: Process Cycle Data

Default configuration processes cycle data:
```bash
uv run samsung-health-extract
```

Output: `complete_cycle_data.csv`

### Example 2: Custom Path

If your data is elsewhere:
```bash
uv run samsung-health-extract "C:/Users/YourName/Samsung Health Export"
```

### Example 3: Current Enabled CSV Files

The default configuration includes:
```json
{
  "csv_filtering": {
    "enabled_csvs": [
      ".cycle.daily_temperature",
      ".cycle.flow",
      ".cycle.sexual_activity",
      ".tracker.heart_rate",
      ".tracker.pedometer_step_count",
      ".tracker.oxygen_saturation",
      ".weight",
      ".sleep",
      ".exercise",
      ".respiratory_rate"
    ]
  }
}
```

You can edit this list in `data_combination.json` to add or remove CSV files as needed.

## Getting Help

- **Documentation**: See [README.md](README.md)
- **Issues**: Open an issue on GitHub

## Tips

1. **Start small**: Begin with a few CSV files in `enabled_csvs`
2. **Check logs**: Structured logs in `logs/` directory help debug issues (JSON format, one entry per line)
3. **Backup data**: Keep a copy of your original Samsung Health export
4. **Customize**: Edit `data_combination.json` to fit your needs
5. **Analyze logs**: Use `jq` or similar tools to filter and analyze the JSON log files
6. Column names that are with long enumeration like, say, com.samsung.health.sleep.start_time are absolute, while start_time might be influenced by empty columns



IMPORTANT NOTE!
the library is designed to be modular so anyone can activate and deactivate whatver csv they need but you need NOT to forget to define it in the data combination.json file
---

**Happy data processing! 🎉**

