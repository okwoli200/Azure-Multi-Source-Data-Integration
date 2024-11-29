#!/usr/bin/env python
# coding: utf-8

############################################################################################################
# Filename-----: Create_daily_parquet_files.py
# Description--: Load flat files and create daily delta files
# Usage--------: Use this script to create and format delta files for loading into Azure
# Dependency---: pandas, os, datetime, json
# Author-------: Data Warehouse Analyst
# Revision-----: 1.1 - 2024-11-19 - Filter for only yesterday's data
############################################################################################################

import os
import pandas as pd
from datetime import datetime, timedelta
import json

# Define the path for error logging
log_file_path = os.path.join(r'C:\Load into Azure', 'Create_daily_parquet_files_error_log.log')

# Function to log messages
def log_error(log_message):
    with open(log_file_path, 'a') as log_file:
        log_file.write(log_message + '\n')
    print(log_message)

# Main script block
def main(file_paths, output_folder):
    current_date_time = datetime.now().strftime('%Y-%m-%d %A %H:%M')
    log_error(f'========= Start Create Daily Parquet Files Script at {current_date_time} ==========')
    
    try:
        for file_path in file_paths:
            log_error(f'Processing file: {file_path}')
            
            # Load data from the flat file
            df = extract_flat_file(file_path, encoding='latin1')
            
            # Remove the index column '__index_level_0__' if it exists
            if '__index_level_0__' in df.columns:
                df = df.drop(columns=['__index_level_0__'])
            
            # Determine the date column(s) based on the file path
            date_columns = determine_date_column(file_path)
            
            # Filter data for only yesterday
            if isinstance(date_columns, list):  # For files with multiple date columns
                filtered_df = filter_data_for_yesterday(df, date_columns)
            else:
                filtered_df = filter_data_for_yesterday(df, [date_columns])
            
            # Generate output file name with yesterday's date
            date_suffix = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            output_file_name = f"{os.path.basename(file_path).split('.')[0]}_{date_suffix}.parquet"
            output_path = os.path.join(output_folder, output_file_name)
            
            # Save the DataFrame as a Parquet file
            filtered_df.to_parquet(output_path, engine='pyarrow', compression='snappy')
            log_error(f"Data saved to {output_path}")
        
    except Exception as e:
        log_error(f"ERROR: Failed to process file {file_path} - {e}")
        
    finally:
        current_date_time = datetime.now().strftime('%Y-%m-%d %A %H:%M')
        log_error(f'========= End Create Daily Parquet Files Script at {current_date_time} ==========')

def extract_flat_file(file_path, encoding='utf-8'):
    try:
        return pd.read_csv(file_path, encoding=encoding)
    except Exception as e:
        log_error(f"ERROR: Could not load file {file_path} - {e}")
        raise

def determine_date_column(file_path):
    try:
        if "OvertimeTrans" in file_path:
            return ["StartOvertime", "EndOvertime"]
        elif "MisconductTrans" in file_path:
            return "misconduct_date"
        elif "AbsentTrans" in file_path:
            return "absent_date"
        else:
            raise ValueError("Date column not defined for this file.")
    except ValueError as e:
        log_error(f"ERROR: {e}")
        raise

def filter_data_for_yesterday(df, date_columns):
    try:
        yesterday = (datetime.now() - timedelta(days=1)).date()
        
        # Ensure the date columns are in datetime format
        for column in date_columns:
            df[column] = pd.to_datetime(df[column], errors='coerce').dt.date

        # Filter rows where any date column matches exactly yesterday
        if len(date_columns) == 1:
            return df[df[date_columns[0]] == yesterday]
        else:
            return df[df[date_columns[0]].eq(yesterday) | df[date_columns[1]].eq(yesterday)]
    except Exception as e:
        log_error(f"ERROR: Failed to filter data for yesterday - {e}")
        raise

# Load configuration from JSON file in the script's directory and run the script
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config_file.json')

with open(config_path) as f:
    config = json.load(f)

file_paths = config['file_paths']
output_folder = config['output_folder']

main(file_paths, output_folder)
