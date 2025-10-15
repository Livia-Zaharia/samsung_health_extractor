# Logging Guide

## Overview

The Samsung Health Extractor uses [Eliot](https://eliot.readthedocs.io/) for structured logging. All logs are written to timestamped files in the `logs/` directory.

## Log File Location

- **Default location**: `logs/samsung_health_extract_YYYYMMDD_HHMMSS.log`
- **Custom location**: Use `--log-file` option

Example:
```bash
uv run samsung-health-extract --log-file "./my-extraction.log"
```

## Log Format

Logs are in JSON format (one JSON object per line). This makes them:
- **Machine-readable**: Easy to parse programmatically
- **Filterable**: Use tools like `jq` to extract specific information
- **Structured**: Each log entry has consistent fields

### Example Log Entry

```json
{
  "action_type": "read_ignore_list",
  "action_status": "succeeded",
  "filename": "ignore_csvs.txt",
  "timestamp": "2024-10-15T14:30:22.123456",
  "count": 15,
  "items": [".badge", ".device_profile", "..."]
}
```

## Common Log Message Types

| Message Type | Description |
|-------------|-------------|
| `logging_initialized` | Logging setup complete |
| `read_ignore_list` | Loading ignore list |
| `read_combination_config` | Loading configuration |
| `filtered_ignored` | File skipped (in ignore list) |
| `filtered_empty` | File skipped (no data) |
| `filtered_not_in_config` | File skipped (not enabled) |
| `processing_combination` | Starting data combination |
| `loaded_data` | CSV data loaded successfully |
| `merged_data` | Data merged from multiple sources |
| `output_created` | Output file created |
| `error` | Error occurred |

## Analyzing Logs

### Using jq

```bash
# View all log entries nicely formatted
cat logs/samsung_health_extract_*.log | jq '.'

# View only errors
cat logs/samsung_health_extract_*.log | jq 'select(.message_type == "error")'

# View filtered files
cat logs/samsung_health_extract_*.log | jq 'select(.message_type | contains("filtered"))'

# View processing statistics
cat logs/samsung_health_extract_*.log | jq 'select(.message_type == "overall_stats")'

# Count how many files were filtered
cat logs/samsung_health_extract_*.log | jq 'select(.message_type | contains("filtered"))' | wc -l

# View all data combination outputs
cat logs/samsung_health_extract_*.log | jq 'select(.message_type == "output_created")'
```

### Using Python

```python
import json
from pathlib import Path

# Read log file
log_file = Path("logs/samsung_health_extract_20241015_143022.log")
with log_file.open() as f:
    for line in f:
        entry = json.loads(line)
        if entry.get("message_type") == "error":
            print(f"Error: {entry}")
```

### Using grep

```bash
# Find all errors
grep '"message_type".*"error"' logs/samsung_health_extract_*.log

# Find specific file processing
grep 'cycle.daily_temperature' logs/samsung_health_extract_*.log

# Count successful operations
grep '"action_status".*"succeeded"' logs/samsung_health_extract_*.log | wc -l
```

## Debugging Tips

### Check for Errors

```bash
# Quick check for any errors
cat logs/samsung_health_extract_*.log | jq 'select(.message_type == "error" or .action_status == "failed")'
```

### View Processing Flow

```bash
# See the sequence of operations
cat logs/samsung_health_extract_*.log | jq -r '[.timestamp, .action_type // .message_type] | @tsv'
```

### Analyze Filtered Files

```bash
# See which files were filtered and why
cat logs/samsung_health_extract_*.log | jq 'select(.message_type | contains("filtered")) | {message_type, name}'
```

### Check Data Combination Results

```bash
# View output statistics
cat logs/samsung_health_extract_*.log | jq 'select(.message_type == "output_created") | {file, rows, columns}'
```

## Log Retention

- Logs are never automatically deleted
- Each run creates a new log file with a unique timestamp
- Consider periodically cleaning old logs:

```bash
# Delete logs older than 30 days (Linux/macOS)
find logs/ -name "*.log" -mtime +30 -delete

# Delete logs older than 30 days (Windows PowerShell)
Get-ChildItem logs/*.log | Where-Object {$_.LastWriteTime -lt (Get-Date).AddDays(-30)} | Remove-Item
```

## Best Practices

1. **Keep recent logs**: Useful for comparing runs and debugging
2. **Review errors**: Check logs after each run for any issues
3. **Archive important runs**: Save logs from successful extractions for reference
4. **Use version control**: Don't commit logs to git (already in `.gitignore`)

## Example Workflow

```bash
# 1. Run extraction
uv run samsung-health-extract

# 2. Check for errors in the latest log
cat logs/samsung_health_extract_*.log | tail -n 1000 | jq 'select(.message_type == "error")'

# 3. View what was processed
cat logs/samsung_health_extract_*.log | tail -n 1000 | jq 'select(.message_type == "output_created")'

# 4. If something went wrong, review the full log
cat logs/samsung_health_extract_20241015_143022.log | jq '.' | less
```

## Advanced: Custom Log Analysis Scripts

Create a `analyze_logs.py` script:

```python
#!/usr/bin/env python3
"""Analyze Samsung Health Extractor logs."""

import json
from pathlib import Path
from collections import Counter

def analyze_log(log_file: Path) -> dict:
    """Analyze a log file and return statistics."""
    stats = {
        "total_entries": 0,
        "errors": 0,
        "filtered_files": 0,
        "output_files": [],
        "message_types": Counter(),
    }
    
    with log_file.open() as f:
        for line in f:
            entry = json.loads(line)
            stats["total_entries"] += 1
            stats["message_types"][entry.get("message_type", "unknown")] += 1
            
            if entry.get("message_type") == "error":
                stats["errors"] += 1
            elif "filtered" in entry.get("message_type", ""):
                stats["filtered_files"] += 1
            elif entry.get("message_type") == "output_created":
                stats["output_files"].append(entry.get("file"))
    
    return stats

# Usage
latest_log = sorted(Path("logs").glob("*.log"))[-1]
stats = analyze_log(latest_log)
print(f"Log analysis for: {latest_log}")
print(f"Total entries: {stats['total_entries']}")
print(f"Errors: {stats['errors']}")
print(f"Filtered files: {stats['filtered_files']}")
print(f"Output files: {stats['output_files']}")
```

---

For more information, see the [Eliot documentation](https://eliot.readthedocs.io/).

