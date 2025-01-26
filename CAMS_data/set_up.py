#!/usr/bin/env python
# coding: utf-8

# In[70]:


#Import required libraries
import datetime
import os
import json
import pandas as pd
import argparse


# In[71]:


def set_up_folders():
    """
    Sets up the folder structure for storing CAMS data.

    This function creates three directories within the current working directory:
    - `unprocessed`: To store unprocessed data files.
    - `processed`: To store processed data files.
    - `results`: To store the results after fetching the CAMS data

    Returns:
        tuple: A tuple containing the paths to the created folders:
            - unprocessed_folder (str): Path to the 'unprocessed' folder.
            - processed_folder (str): Path to the 'processed' folder.
            - results_folder (str): Path to the 'results' folder.
    """

    try:
        cams_folder = os.getcwd()
        print(cams_folder)
        print(f"Home dir is: {cams_folder}")
        
         #Create folders in 'CAMS_data'
        unprocessed_folder = os.path.join(cams_folder, 'unprocessed')
        processed_folder = os.path.join(cams_folder, 'processed')
        results_folder = os.path.join(cams_folder, 'results')

        os.makedirs(unprocessed_folder, exist_ok=True)
        os.makedirs(processed_folder, exist_ok=True)
        os.makedirs(results_folder, exist_ok=True)
        print("Folder structure created successfully.")
        return unprocessed_folder, processed_folder, results_folder
    except OSError as e:
        print(f"Following error occured while creating directories: {e}")


# In[72]:


def create_config_file(config_params):
    """
    Creates a configuration file in JSON format.

    This function takes a dictionary of configuration parameters and writes them
    to a `config.json` file in the current directory.

    Parameters:
        config_params (dict): A dictionary containing configuration parameters to be saved.

    Returns:
        None
    """
    try:
        with open('config.json', 'w') as json_file:
            json.dump(config_params, json_file, indent=4)
        print(f"Config file created in the {os.getcwd()}.")
    except Exception as e:
        print(f"An error occured {e}")


# In[52]:


def chunk_data(file_path, output_folder):
    """
    Reads a CSV file, splits it into smaller chunks, and saves each chunk as a separate CSV file.

    Parameters:
        file_path (str): The path to the input CSV file.
        output_folder (str): The folder where the chunked CSV files will be saved.
    Returns:
        int: The total number of chunked files created.

    """
    # Read the CSV file into a DataFrame
    chunk_size = 100
    solar_farms_data = pd.read_csv(file_path)

    # Split the DataFrame into chunks
    for i, start in enumerate(range(0, len(solar_farms_data), chunk_size)):
        chunk = solar_farms_data.iloc[start: start + chunk_size]
        
        # Define the file name for the current chunk
        chunk_file = os.path.join(output_folder, f'unprocessed_data_{i+1}.csv')
        
        # Save the chunk to a CSV file
        chunk.to_csv(chunk_file, index=False)

    print(f"Successfully created {i + 1} chunked files in '{output_folder}'.")
    return i + 1


# In[73]:


def main():
    """
    Main function to set up the workflow for fetching CAMS data based on command-line arguments.

    This function performs the following steps:
    1. Parses command-line arguments to collect input file path and parameters required for CAMS data retrieval.
    2. Creates the required folder structure (`unprocessed`, `processed`, and `results`).
    3. Generates a `config.json` file with the provided parameters and folder paths.
    4. Chunks the input data file and saves the chunks in the `unprocessed` folder.
    """

    try:

        parser = argparse.ArgumentParser(description="Enter input file and parameters required to get CAMS data")
        #Parse command-line arguments
        parser.add_argument("--file_path", type=str, required=True, help="Enter path to input csv file.")
        parser.add_argument("--sky_type", type=str, required=True, choices=['mcclear', 'cams_radiation'], help="Sky type.")
        parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format.")
        parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format.")
        parser.add_argument("--time_step", type=str, required=True, choices=['1min', '15min', '1h', '1d', '1M'], help="Time step.")
        parser.add_argument("--time_reference", type=str, required=True, choices=['UT', 'TST'], help="‘UT’ (universal time) or ‘TST’ (True Solar Time)")
        parser.add_argument("--email", type=str, required=True, help="Email registered with the soda pro service.")
    
        args = parser.parse_args()
        #Conver the command line argument to dictionary
        params = vars(args)
        print(params)
        print(os.getcwd())
        
        #Creating required folder structure
        unprocessed_folder, processed_folder, results_folder = set_up_folders()

        #parameters to add to the config file
        #params["start_date"] = params["start_date"].strftime("%Y-%m-%d")
        #params["end_date"] = params["end_date"].strftime("%Y-%m-%d")
        params["unprocessed_folder"] = unprocessed_folder
        params["processed_folder"] = processed_folder
        params["results_folder"] = results_folder
        params["server_name"] = 'api.soda-solardata.com'
        params["timeout"] = 30

        #Creating config file
        create_config_file(params)

        #Chuncking data and saving files in unprocessed folder
        chunk_data(params["file_path"], unprocessed_folder)
        
    except Exception as e:
        print(f"An error occured: {e}")
    


# In[ ]:


if __name__=="__main__":
    main()

