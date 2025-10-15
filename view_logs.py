"""Log Viewer for Samsung Health Extractor.

A simple utility to view Eliot JSON logs in a human-readable format.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import typer

app = typer.Typer(help="View Samsung Health Extractor logs")


def format_value(value: Any, indent: int = 0) -> str:
    """Format a value for display.
    
    Args:
        value: The value to format
        indent: Current indentation level
        
    Returns:
        Formatted string representation
    """
    indent_str = "  " * indent
    
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = ["{"]
        for k, v in value.items():
            formatted_v = format_value(v, indent + 1)
            lines.append(f"{indent_str}  {k}: {formatted_v}")
        lines.append(f"{indent_str}}}")
        return "\n".join(lines)
    elif isinstance(value, list):
        if not value:
            return "[]"
        if len(value) > 10:
            # Truncate long lists
            return f"[{len(value)} items: {', '.join(str(v) for v in value[:5])}...]"
        return f"[{', '.join(str(v) for v in value)}]"
    else:
        return str(value)


def print_log_entry(entry: dict[str, Any], verbose: bool = False) -> None:
    """Print a log entry in a readable format.
    
    Args:
        entry: The log entry dictionary
        verbose: Whether to show all fields
    """
    message_type = entry.get("message_type", "unknown")
    action_type = entry.get("action_type", "")
    action_status = entry.get("action_status", "")
    
    # Build header
    if action_type:
        header = f"🔵 ACTION: {action_type}"
        if action_status:
            status_emoji = "✅" if action_status == "succeeded" else "❌" if action_status == "failed" else "⏳"
            header = f"{status_emoji} ACTION {action_status.upper()}: {action_type}"
    else:
        header = f"  📝 {message_type}"
    
    print(header)
    
    # Filter out metadata fields
    skip_fields = {"message_type", "action_type", "action_status", "task_uuid", "task_level", "timestamp"}
    
    # Print relevant fields
    for key, value in entry.items():
        if key in skip_fields and not verbose:
            continue
            
        formatted_value = format_value(value)
        
        # Special handling for debug messages
        if key.startswith("debug_") or message_type.startswith("debug_"):
            print(f"    🔍 {key}: {formatted_value}")
        elif key in ("error", "warning"):
            print(f"    ⚠️  {key}: {formatted_value}")
        elif key in ("file", "output", "path", "output_file"):
            print(f"    📁 {key}: {formatted_value}")
        elif key in ("rows", "count"):
            print(f"    📊 {key}: {formatted_value}")
        else:
            # Only show non-empty values
            if value or verbose:
                print(f"    • {key}: {formatted_value}")
    
    print()  # Blank line between entries


@app.command()
def main(
    log_file: str = typer.Argument(..., help="Path to the log file to view"),
    filter_debug: bool = typer.Option(False, "--debug", "-d", help="Show only debug messages"),
    filter_error: bool = typer.Option(False, "--errors", "-e", help="Show only errors and warnings"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show all fields including metadata"),
) -> None:
    """View Samsung Health Extractor logs in a readable format.
    
    Examples:
        # View all logs
        python view_logs.py logs/samsung_health_extract_20251015_144051.log
        
        # View only debug messages
        python view_logs.py logs/samsung_health_extract_20251015_144051.log --debug
        
        # View only errors
        python view_logs.py logs/samsung_health_extract_20251015_144051.log --errors
    """
    log_path = Path(log_file)
    
    if not log_path.exists():
        typer.echo(f"❌ Error: Log file '{log_file}' does not exist!", err=True)
        raise typer.Exit(code=1)
    
    typer.echo(f"📖 Reading log file: {log_path}\n")
    typer.echo("=" * 80)
    typer.echo()
    
    try:
        with log_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                try:
                    entry = json.loads(line.strip())
                    
                    # Apply filters
                    message_type = entry.get("message_type", "")
                    
                    if filter_debug:
                        if not message_type.startswith("debug_"):
                            continue
                    
                    if filter_error:
                        if "error" not in entry and "warning" not in entry:
                            continue
                    
                    print_log_entry(entry, verbose=verbose)
                    
                except json.JSONDecodeError as e:
                    typer.echo(f"⚠️  Warning: Could not parse line {line_num}: {e}", err=True)
                    continue
        
        typer.echo("=" * 80)
        typer.echo(f"✅ Finished reading log file")
        
    except Exception as e:
        typer.echo(f"❌ Error reading log file: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()

