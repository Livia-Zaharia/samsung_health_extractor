import json
import os
import os.path
import sqlite3
from functools import reduce
from glob import glob
import numpy as np
import pandas as pd
from datetime import datetime
import re

global samsung_csv_paths
samsung_data_path = './Samsung Health'


def read_ignore_list(filename='ignore_csvs.txt'):
    """
    Read CSV names to ignore from an external file.
    Returns a set of cleaned CSV names to ignore.
    """
    ignore_set = set()
    
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found. No files will be ignored.")
        return ignore_set
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    ignore_set.add(line)
        
        if ignore_set:
            print(f"Loaded ignore list from {filename}: {sorted(ignore_set)}")
        else:
            print(f"No entries found in {filename}")
            
    except Exception as e:
        print(f"Error reading {filename}: {e}")
    
    return ignore_set



def read_combination_config(filename='data_combination.json'):
    """
    Read data combination configuration from JSON file.
    Returns a dictionary with combination configurations.
    """
    config = {}
    
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found. No data combinations will be processed.")
        return config
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            config = data.get('data_combinations', {})
        
        if config:
            print(f"Loaded data combination configuration from {filename}: {len(config)} combinations configured")
            for combo_name, combo_config in config.items():
                output_file = combo_config.get('output_file', 'unknown.csv')
                sources_count = len(combo_config.get('sources', []))
                print(f"  • {combo_name} → {output_file} ({sources_count} sources)")
        else:
            print(f"No data combination configurations found in {filename}")
            
    except Exception as e:
        print(f"Error reading {filename}: {e}")
    
    return config


def get_cleaned_csv_names(filter_ignored=True, ignore_file='ignore_csvs.txt', hierarchical=True, filter_empty=True, filter_config=True, config_file='data_combination.json'):
    """
    Extract CSV file names from samsung_csv_paths and clean them by:
    1. Removing everything before 'health' and 'health' itself
    2. Removing numbers and '.csv' suffix
    3. Optionally filtering out ignored CSV names
    4. Optionally filtering out CSV files with no data (header-only)
    5. Optionally filtering out CSV files not in configuration
    6. Optionally organizing into hierarchical structure by categories
    
    Args:
        filter_ignored (bool): Whether to filter out ignored CSV names
        ignore_file (str): Path to file containing CSV names to ignore
        hierarchical (bool): Whether to return hierarchical structure or flat list
        filter_empty (bool): Whether to filter out CSV files with no data
        filter_config (bool): Whether to only process CSV files in configuration
        config_file (str): Path to data combination JSON file (now includes CSV filtering)
    
    Returns:
        If hierarchical=True: dict with categories and subcategories
        If hierarchical=False: array of cleaned CSV names
    """
    global samsung_csv_paths
    
    # Get ignore list if filtering is enabled
    ignore_set = set()
    if filter_ignored:
        ignore_set = read_ignore_list(ignore_file)
    
    # Get CSV configuration if filtering is enabled
    enabled_csvs = set()
    if filter_config:
        combination_config = read_combination_config(config_file)
        csv_filtering = combination_config.get('csv_filtering', {})
        enabled_csvs = set(csv_filtering.get('enabled_csvs', []))
        if not enabled_csvs:
            print("No enabled CSV files found in configuration. All files will be processed.")
        else:
            print(f"Filtering to {len(enabled_csvs)} enabled CSV files: {sorted(enabled_csvs)}")
    
    if not hierarchical:
        # Return flat list (original behavior)
        cleaned_names = []
        for file_path in samsung_csv_paths:
            name = os.path.basename(file_path)
            cleaned_name = _clean_csv_name(name)
            
            # Filter out ignored names if filtering is enabled
            if filter_ignored and cleaned_name in ignore_set:
                print(f"[FILTERED] Ignoring CSV: {cleaned_name}")
                continue
            
            # Filter out empty files if filtering is enabled
            if filter_empty and not _csv_has_data(file_path):
                print(f"[FILTERED] Empty CSV file: {cleaned_name}")
                continue
            
            # Filter out files not in configuration if filtering is enabled
            if filter_config and cleaned_name not in enabled_csvs:
                print(f"[FILTERED] CSV not in configuration: {cleaned_name}")
                continue
            
            cleaned_names.append(cleaned_name)
        
        return cleaned_names
    else:
        # Return hierarchical structure
        categories = {}
        
        for file_path in samsung_csv_paths:
            name = os.path.basename(file_path)
            cleaned_name = _clean_csv_name(name)
            
            # Filter out ignored names if filtering is enabled
            if filter_ignored and cleaned_name in ignore_set:
                print(f"[FILTERED] Ignoring CSV: {cleaned_name}")
                continue
            
            # Filter out empty files if filtering is enabled
            if filter_empty and not _csv_has_data(file_path):
                print(f"[FILTERED] Empty CSV file: {cleaned_name}")
                continue
            
            # Filter out files not in configuration if filtering is enabled
            if filter_config and cleaned_name not in enabled_csvs:
                print(f"[FILTERED] CSV not in configuration: {cleaned_name}")
                continue
            
            # Split by dots to get category and subcategory
            parts = cleaned_name.split('.')
            
            if len(parts) == 1:
                # Main category (no subcategory)
                category = parts[0]
                if category not in categories:
                    categories[category] = {'subcategories': [], 'files': []}
                categories[category]['files'].append(cleaned_name)
            elif len(parts) == 2:
                # Has one dot - treat as main category with no subcategory
                category = parts[0]
                if category not in categories:
                    categories[category] = {'subcategories': [], 'files': []}
                categories[category]['files'].append(cleaned_name)
            else:
                # Has 2+ dots - use second dot as separator for subcategories
                category = '.'.join(parts[:2])  # First two parts as category
                subcategory = '.'.join(parts[2:])  # Everything after second dot as subcategory
                
                if category not in categories:
                    categories[category] = {'subcategories': [], 'files': []}
                
                # Add to subcategories if not already there
                if subcategory not in categories[category]['subcategories']:
                    categories[category]['subcategories'].append(subcategory)
        
        # Sort categories and subcategories for consistent output
        for category in categories:
            categories[category]['subcategories'].sort()
            categories[category]['files'].sort()
        
        return dict(sorted(categories.items()))


def _clean_csv_name(name):
    """
    Helper function to clean a single CSV name.
    Returns the cleaned name without extension, prefix, and trailing numbers.
    """
    # Remove .csv extension
    name_without_ext = name.replace('.csv', '')
    
    # Find 'health' in the name and remove everything before it including 'health'
    health_index = name_without_ext.find('health')
    if health_index != -1:
        # Remove everything before and including 'health'
        name_without_ext = name_without_ext[health_index + 6:]  # +6 to skip 'health'
    
    # Remove trailing numbers using regex
    # This will remove any sequence of digits at the end of the string
    cleaned_name = re.sub(r'\d+$', '', name_without_ext)
    
    # Remove any trailing dots or underscores that might be left
    cleaned_name = cleaned_name.rstrip('._')
    
    return cleaned_name


def _csv_has_data(file_path, min_rows=10):
    """
    Check if a CSV file contains meaningful data beyond just headers.
    Files with 10 or fewer rows (besides header) are considered to have no data.
    Returns True if the file has meaningful data, False if it's empty or has minimal data.
    
    Args:
        file_path (str): Path to the CSV file
        min_rows (int): Minimum number of data rows required (default: 10)
    """
    try:
        # Read CSV with pandas, skipping the first row (which is usually metadata)
        df = pd.read_csv(file_path, skiprows=1)
        
        # Check if dataframe has any rows
        if df.empty:
            return False
        
        # Check if all rows are NaN or empty
        if df.isnull().all().all():
            return False
        
        # Remove rows that are entirely NaN
        df_clean = df.dropna(how='all')
        if df_clean.empty:
            return False
        
        # Check if we have enough rows for meaningful data
        if len(df_clean) <= min_rows:
            return False
        
        # Check if we have at least one non-empty value in the dataframe
        has_data = False
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':  # String columns
                non_empty = df_clean[col].astype(str).str.strip() != ''
                if non_empty.any():
                    has_data = True
                    break
            else:  # Numeric columns
                if not df_clean[col].isna().all():
                    has_data = True
                    break
        
        return has_data
        
    except Exception as e:
        print(f"Error checking data in {file_path}: {e}")
        return False


def process_data_combinations(combination_config, categories):
    """
    Process data combinations according to the configuration.
    Combines data from multiple CSV sources and outputs to specified files.
    """
    global samsung_csv_paths
    
    for combo_name, combo_config in combination_config.items():
        print(f"\n🔄 Processing combination: {combo_name}")
        print(f"   Output file: {combo_config.get('output_file', 'unknown.csv')}")
        
        try:
            # Get sources for this combination
            sources = combo_config.get('sources', [])
            if not sources:
                print(f"   ❌ No sources defined for {combo_name}")
                continue
            
            # Sort sources by priority
            sources.sort(key=lambda x: x.get('priority', 999))
            
            combined_df = None
            merge_key = None
            
            for i, source in enumerate(sources):
                csv_name = source.get('csv_name', '')
                priority = source.get('priority', i + 1)
                required = source.get('required', False)
                columns_to_include = source.get('columns_to_include', [])
                rename_columns = source.get('rename_columns', {})
                
                print(f"   📊 Source {priority}: {csv_name}")
                
                # Find the CSV file path
                csv_file_path = None
                for file_path in samsung_csv_paths:
                    name = os.path.basename(file_path)
                    cleaned_name = _clean_csv_name(name)
                    if cleaned_name == csv_name:
                        csv_file_path = file_path
                        break
                
                if not csv_file_path:
                    if required:
                        print(f"   ❌ Required source {csv_name} not found!")
                        break
                    else:
                        print(f"   ⚠️  Optional source {csv_name} not found, skipping")
                        continue
                
                # Read the CSV data
                try:
                    df = pd.read_csv(csv_file_path, skiprows=1)
                    
                    # Handle merge key rename for standardization
                    source_merge_key = source.get('merge_key', 'day_time')
                    merge_key_rename = source.get('merge_key_rename', source_merge_key)
                    
                    # Set merge key from first source, or use the renamed key for standardization
                    if merge_key is None:
                        merge_key = merge_key_rename
                    
                    # Ensure merge key is always included in columns_to_include
                    columns_to_include_with_key = columns_to_include.copy() if columns_to_include else []
                    if source_merge_key not in columns_to_include_with_key and source_merge_key in df.columns:
                        columns_to_include_with_key.append(source_merge_key)
                    
                    # Select only required columns (including merge key)
                    if columns_to_include_with_key:
                        available_columns = [col for col in columns_to_include_with_key if col in df.columns]
                        if not available_columns:
                            print(f"   ❌ None of the required columns {columns_to_include_with_key} found in {csv_name}")
                            print(f"   Available columns: {list(df.columns)}")
                            if required:
                                break
                            else:
                                continue
                        df = df[available_columns]
                    
                    # Rename merge key to standardize across sources FIRST
                    if source_merge_key in df.columns and merge_key_rename != source_merge_key:
                        df = df.rename(columns={source_merge_key: merge_key_rename})
                        # Update the source_merge_key to the renamed version
                        source_merge_key = merge_key_rename
                    
                    # Rename columns if specified
                    if rename_columns:
                        df = df.rename(columns=rename_columns)
                    
                    # Convert merge key to datetime for proper merging
                    if merge_key in df.columns:
                        try:
                            # Try to convert to datetime
                            if pd.api.types.is_numeric_dtype(df[merge_key]):
                                df[merge_key] = pd.to_datetime(df[merge_key], unit='ms')
                            else:
                                # Handle different datetime formats
                                df[merge_key] = pd.to_datetime(df[merge_key], errors='coerce')
                            # Normalize to date only for consistent merging
                            df[merge_key] = df[merge_key].dt.date
                        except Exception as e:
                            print(f"   ⚠️  Could not convert {merge_key} to datetime: {e}")
                    
                    print(f"   ✅ Loaded {len(df)} rows from {csv_name}")
                    print(f"   📋 Columns: {list(df.columns)}")
                    
                    # Combine with previous data
                    if combined_df is None:
                        combined_df = df
                        print(f"   🔗 Starting with {csv_name} as base")
                    else:
                        # Convert existing merge key to datetime for consistent merging
                        if merge_key in combined_df.columns:
                            try:
                                if pd.api.types.is_numeric_dtype(combined_df[merge_key]):
                                    combined_df[merge_key] = pd.to_datetime(combined_df[merge_key], unit='ms')
                                else:
                                    # Handle different datetime formats
                                    combined_df[merge_key] = pd.to_datetime(combined_df[merge_key], errors='coerce')
                                # Normalize to date only for consistent merging
                                combined_df[merge_key] = combined_df[merge_key].dt.date
                            except Exception as e:
                                print(f"   ⚠️  Could not convert existing {merge_key} to datetime: {e}")
                        
                        # Merge with existing data
                        combined_df = pd.merge(combined_df, df, on=merge_key, how='outer')
                        print(f"   🔗 Merged with {csv_name}")
                
                except Exception as e:
                    print(f"   ❌ Error processing {csv_name}: {e}")
                    if required:
                        break
                    else:
                        continue
            
            # Process the combined data if we have any
            if combined_df is not None and not combined_df.empty:
                # Apply output structure configuration
                output_config = combo_config.get('output_structure', {})
                
                # Convert merge key back to datetime for sorting
                if merge_key in combined_df.columns:
                    try:
                        combined_df[merge_key] = pd.to_datetime(combined_df[merge_key])
                    except Exception as e:
                        print(f"   ⚠️  Could not convert {merge_key} back to datetime for sorting: {e}")
                
                # Sort the data
                sort_by = output_config.get('primary_sort', merge_key)
                sort_ascending = output_config.get('sort_ascending', True)
                if sort_by in combined_df.columns:
                    combined_df = combined_df.sort_values(by=sort_by, ascending=sort_ascending)
                
                # Convert merge key back to string for CSV output
                if merge_key in combined_df.columns:
                    combined_df[merge_key] = combined_df[merge_key].dt.strftime('%Y-%m-%d')
                
                # Select final columns if specified
                final_columns = output_config.get('final_columns', [])
                if final_columns:
                    available_final_columns = [col for col in final_columns if col in combined_df.columns]
                    if available_final_columns:
                        combined_df = combined_df[available_final_columns]
                        print(f"   📋 Selected final columns: {available_final_columns}")
                    else:
                        print(f"   ⚠️  None of the final columns {final_columns} are available")
                        print(f"   📋 Available columns: {list(combined_df.columns)}")
                
                # Apply data processing options
                data_processing = output_config.get('data_processing', {})
                if data_processing.get('fill_missing_values'):
                    fill_value = data_processing['fill_missing_values']
                    combined_df = combined_df.fillna(fill_value)
                
                # Remove completely empty rows
                combined_df = combined_df.dropna(how='all')
                
                # Save to output file
                output_file = combo_config.get('output_file', f'{combo_name}.csv')
                combined_df.to_csv(output_file, index=False)
                
                print(f"   ✅ Successfully created {output_file} with {len(combined_df)} rows")
                print(f"   📋 Final columns: {list(combined_df.columns)}")
                
                # Show sample of the data
                if len(combined_df) > 0:
                    print(f"   📊 Sample data (first 3 rows):")
                    for idx, (i, row) in enumerate(combined_df.head(3).iterrows()):
                        row_dict = {}
                        for col, val in row.items():
                            if pd.isna(val):
                                row_dict[col] = "N/A"
                            else:
                                row_dict[col] = str(val)
                        print(f"      Row {idx+1}: {row_dict}")
            else:
                print(f"   ❌ No data to output for {combo_name}")
                
        except Exception as e:
            print(f"   ❌ Error processing combination {combo_name}: {e}")
    
    print(f"\n✅ Data combination processing completed!")


def extract_data():
    global samsung_csv_paths
    samsung_base_dir = os.path.join(samsung_data_path)

    samsung_dump_dirs = glob(os.path.join(samsung_base_dir, '*'))
    samsung_dump_dir = os.path.basename(samsung_dump_dirs[0])
    print(len(samsung_dump_dirs), 'dumps found, taking first:', samsung_dump_dir)

    samsung_csv_paths = glob(os.path.join(samsung_base_dir, samsung_dump_dir, '*.csv'))
    print(len(samsung_csv_paths), 'csvs found')

    # Get and display cleaned CSV names in hierarchical structure
    print(f"\n=== FILTERING CSV FILES ===")
    print("Checking for empty files, applying ignore list, and configuration filtering...")
    
    categories = get_cleaned_csv_names()
    print(f"\n=== HIERARCHICAL CSV STRUCTURE (after filtering) ===")
    
    # Load combination configuration
    combination_config = read_combination_config()
    
    # Process data combinations if configuration exists
    if combination_config:
        print(f"\n=== PROCESSING DATA COMBINATIONS ===")
        process_data_combinations(combination_config, categories)
    
    total_files = 0
    for category, data in categories.items():
        print(f"\n📁 {category.upper()}")
        
        # Show subcategories
        if data['subcategories']:
            print("  📂 Subcategories:")
            for subcat in data['subcategories']:
                print(f"    • {subcat}")
        
        # Show main files in this category
        if data['files']:
            print("  📄 Files:")
            for file in data['files']:
                print(f"    • {file}")
        
        category_total = len(data['subcategories']) + len(data['files'])
        total_files += category_total
        print(f"  📊 Total items in {category}: {category_total}")
    
    print(f"\n📊 OVERALL STATISTICS:")
    print(f"  • Total categories: {len(categories)}")
    print(f"  • Total files/subcategories: {total_files}")
    
    
if __name__ == "__main__":
    extract_data()
