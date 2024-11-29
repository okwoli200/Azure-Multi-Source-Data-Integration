#!/usr/bin/env python
# coding: utf-8

################################################################################################################################
##### Filename----: Load_to_Azure_Lake.py
##### Description-: Copies transformed data from local storage to Azure Data Lake based on configuration settings.
##### Usage-------: Execute this script to transfer processed data to Azure Data Lake for Dev, UAT, or Prod environments.
##### Dependency--: Python 3.x, subprocess, argparse, os, json, datetime
##### Author------: Data Warehouse Analyst
##### Revision---: 2.1 - 2024-11-05 - Dynamic handling of multiple transaction types, optimized for yesterday's data
################################################################################################################################

import os
import json
from datetime import datetime, timedelta
import subprocess
import argparse

# Function to get the configuration from the JSON file
def get_config(json_file):
    if not os.path.exists(json_file):
        raise FileNotFoundError(f"Configuration file not found: {json_file}")
    
    with open(json_file, 'r') as file:
        config = json.load(file)
    return config

# Function to log messages
def log_it(log_message):
    with open(log_file_path, 'a') as log_file:
        log_file.write(log_message + '\n')
    print(log_message)

# Function to check if a specific file exists
def check_required_file(file_path):
    if not os.path.exists(file_path):
        log_it(f'ERROR: Missing required file {file_path}')
        return False
    return True

# Function to copy data to Azure using azcopy
def copy_data_to_azure(env, azcopy_path, source_path, folder_name):
    try:
        config = get_config(json_file)
        azure_sas_key = f'{env.upper()}_SAS_TOKEN'

        if azure_sas_key not in config:
            raise KeyError(f'Missing SAS configuration key for {env}')

        azure_sas_token = config[azure_sas_key]
        container_url = config["DEV_URL"]  # Ensure this is set in your config file
        destination_folder = f'fact/live/{folder_name}/'
        destination_url = f'{container_url}{destination_folder}?{azure_sas_token}'

        # Build and run the AzCopy command
        command = [azcopy_path, 'copy', source_path, destination_url]
        subprocess.run(command, check=True)
        log_it(f"azcopy complete for {source_path} to {destination_folder}")
    except KeyError as e:
        log_it(f'Error: Missing configuration keys for {env}: {e}')
    except subprocess.CalledProcessError as e:
        log_it(f'Warning: azcopy failed: {e}')
    except Exception as e:
        log_it(f'Warning: Unexpected error: {e}')

# Main script block
def main(env):
    transaction_types = {
        "Absent": "Absent_Transaction",
        "Misconduct": "Misconduct_Transaction",
        "Overtime": "Overtime_Transaction"
    }

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    current_date_time = datetime.now().strftime('%Y-%m-%d %A %H:%M')
    log_it(f'========= Copy Data to Azure {env} Script at {current_date_time} ==========')

    for transaction_type, folder_name in transaction_types.items():
        source_path = os.path.join(output_folder, f'{transaction_type}Trans_{yesterday}.parquet')

        if check_required_file(source_path):
            log_it(f"Found {transaction_type} file for {yesterday}. Copying to Azure {folder_name}...")
            copy_data_to_azure(env, azcopy_file_path, source_path, folder_name)
        else:
            log_it(f"No {transaction_type} data available for {yesterday}. Skipping.")

    current_date_time = datetime.now().strftime('%Y-%m-%d %A %H:%M')
    log_it(f'========= Copy Data to Azure {env} Script Completed at {current_date_time} ==========')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy data to Azure Data Lake.')
    parser.add_argument('-AzureEnv', choices=['Dev', 'UAT', 'Prod'], required=True, help='Specify the Azure environment to use.')
    args = parser.parse_args()

    # Update json_file path as needed to ensure it is correctly located
    json_file = os.path.join(os.path.dirname(__file__), 'config_file.json')
    try:
        config = get_config(json_file)
        on_prem_path = config['on_prem_path']
        output_folder = os.path.join(on_prem_path, 'Trans_clean_data')
        azcopy_file_path = 'azcopy'  # Adjust if azcopy path needs specification
        log_file_path = os.path.join(os.getcwd(), 'Azure_data_transfer_log.log')

        main(args.AzureEnv)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        log_it(f"ERROR: Configuration file missing - {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        log_it(f"ERROR: Unexpected error - {e}")
