#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Import required libraries
import pvlib
import pandas as pd
import datetime
import json
import os


# In[2]:


def load_config(file_path):
    """
    Loads a configuration file in JSON format.
    Parameters:
        file_path (str): The path to the JSON configuration file.
    Returns:
        dict: The parsed configuration data as a Python dictionary if successful.
        None: If an error occurs during file reading or JSON parsing.
    """
    try:
        with open(file_path, "r") as json_file:
            config = json.load(json_file)
        return config
    except Exception as e:
        print(f"load_config: Error occured: {e}")
        raise


# In[3]:


def get_file_to_process(data_folder):
    """
    Retrieves the oldest file from a specified folder.
    Parameters:
        data_folder (str): The path to the folder containing the files.
    Returns:
        str: The file path of the oldest file in the folder, if found.
        None: If no files are present in the folder or an error occurs.
    """
    try:
        files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if os.path.isfile(os.path.join(data_folder, f))]
        files.sort(key=os.path.getctime)

        if files:
            return files[0]
        else:
            print("There is no datafile to process.")
            return None
    except Exception as e:
        print(f"get_file_to_process: Error occured: {e}")
        raise   
   


# In[4]:


def fetch_cams_data(solar_data, start_date, end_date, email, identifier='mcclear', altitude=None, time_step='1h',
                    time_ref='UT', verbose=False, integrated=False, label=None, map_variables=True, 
                    server=None, timeout=None):
    """
    Fetch CAMS data for each record in a DataFrame and combine into a master DataFrame.
    
    Parameters:
        solar_data (pd.DataFrame): Input DataFrame containing 'latitude' and 'longitude' columns.
        start (str): Start date-time in ISO8601 format (e.g., '2023-01-01T00:00:00').
        end (str): End date-time in ISO8601 format (e.g., '2023-01-01T23:59:59').
        email (str): Email for accessing the CAMS service.
        identifier (str): CAMS identifier ('mcclear' or 'cams_radiation').
        altitude (float): Altitude of the location (optional). 
        time_step (str): Time step for the data ('1h', '15min', etc.).
        time_ref (str): ‘UT’ (universal time) or ‘TST’ (True Solar Time).
        verbose (bool): Whether to print verbose output.
        integrated (bool):  Whether to return radiation parameters as integrated values (Wh/m^2) or 
                            as average irradiance values (W/m^2) (pvlib preferred units)
        label (str): Label for the time index ('left' or 'right').
                     Which bin edge label to label time-step with. The default is ‘left’ for 
                     all time steps except for ‘1M’ which has a default of ‘right’.
        map_variables (bool): When true, renames columns of the DataFrame to pvlib variable names where applicable.
        server (str): Base url of the SoDa Pro CAMS Radiation API.
        timeout (int): Time in seconds to wait for server response before timeout
        References: https://pvlib-python.readthedocs.io/en/stable/reference/generated/pvlib.iotools.get_cams.html#id9
    Returns:
        pd.DataFrame: Master DataFrame containing CAMS data for all records.
    """
    cams_df = pd.DataFrame()  

    for idx, row in solar_data.iterrows():
        try:
            print(row['Latitude'], row['Longitude'], start_date, end_date, email, identifier, altitude, time_step, time_ref, verbose, integrated, label, map_variables, server, timeout)
            # Fetch CAMS data for the current record
            data, metadata = pvlib.iotools.get_cams(
                row['Latitude'], row['Longitude'], start_date, end_date, email,
                identifier, altitude, time_step, time_ref,
                verbose, integrated, label, map_variables,
                server, timeout
            )
            
            # Add latitude and longitude to the returned data
            data.insert(0, 'Latitude', row['Latitude'])
            data.insert(1, 'Longitude', row['Longitude'])

            # Append the current data to the master DataFrame
            cams_df = pd.concat([cams_df, data], ignore_index=True)

        except Exception as e:
            print(f"fetch_cams_data: Error processing record {idx} with latitude {latitude} and longitude {longitude}: {e}")
            raise

    return cams_df


# In[10]:


def validate_output(output_data, start_date, end_date, time_step, output_folder):
    """
    Validates output data against expected row counts and saves a file in the results folder validation is successful.

    Parameters:
        output_data (DataFrame): The processed output data to be validated.
        start_date (str): The start date of the data range in the format "YYYY-MM-DD".
        end_date (str): The end date of the data range in the format "YYYY-MM-DD".
        time_step (str): The time step of the data aggregation (e.g., '1min', '15min', '1h', '1M').
                         - '1min': 1-minute intervals
                         - '15min': 15-minute intervals
                         - '1h': 1-hour intervals
                         - '1M': Monthly intervals
        output_folder (str): The path to the folder where the validated output should be saved.

    Returns:
        bool: Returns `True` if validation is successful and the file is saved.
    """
    try:
        
        total_hours = 24
        #'1min', '15min', '1h', '1d', '1M'
        time_step_ref = {
            "1min": 1, 
            "15min":15, 
            "1h": 60, 
            "1M": None
        }
    
        unique_locations = output_data['Latitude'].nunique()
    
        #Convert date string to datetime
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    
        if time_step == "1M":
            exp_rows = (end_date.year - start_date.year)*12 + (end_date.month - start_date.month) + 1
            exp_rows = exp_rows * unique_locations
        else:
            time_delta = ((end_date + datetime.timedelta(days=1)) - start_date).total_seconds() / 60
            exp_rows = int(time_delta // time_step_ref[time_step])
            exp_rows = exp_rows * unique_locations
    
        if len(output_data) == exp_rows:
            #If validation successful - Saving the processed data to csv file
            cur_date = datetime.datetime.now().strftime("%Y-%m-%d")
            output_file = f"{output_folder}\\processed_cams_data_{cur_date}.csv"
            output_data.to_csv(output_file, index=False)
            print(f"expected row count: {exp_rows} found rows: {len(output_data)}") 
            print(f"Output validation successful. Output saved to: {output_file}")
            return True
        else:
            raise ValueError(f"Expected row count: {exp_rows}, but found {len(output_data)} rows.")
    except Exception as e:
        raise 


# In[11]:


def main():
    """
    Main function to fetch CAMS data for the solar farms data, validate output.

    This function performs the following steps:
    1. Loads configuration parameters from a `config.json` file.
    2. Retrieves a data file from the specified unprocessed folder.
    3. Reads and processes the data file, fetching CAMS data using specified parameters.
    4. Validates the processed output data against expected parameters.
    5. If validation is successful, saves the results to a specified folder and moves the processed file
       from the unprocessed folder to the processed folder.

    Parameters:
        None

    Returns:
        None
    """
    try:
        #Load config file
        print(os.getcwd())
        config = load_config('config.json')
    
        #Getting all the config file parameters
        #Parameters required to fetch CAMS data
        sky_type = config["sky_type"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        time_step = config["time_step"]
        time_reference = config["time_reference"]
        server_name = config["server_name"]
        timeout = config["timeout"]
        email = config["email"]
    
        #Parameters for folder structure
        unprocessed_folder = config["unprocessed_folder"]
        processed_folder = config["processed_folder"]
        results_folder = config["results_folder"]
    
    
        print(f"{sky_type, start_date, end_date, time_step, time_reference, unprocessed_folder, processed_folder, results_folder, server_name, timeout, email }")
    
        #Get the file need to processed from unprocessed folder
        datafile = get_file_to_process(unprocessed_folder)
        file_name = datafile.split("\\")
        processed_file = f"{processed_folder}\\{file_name[-1]}"
        print(processed_file)
    
        if (datafile):
            print(f"Processing {datafile}")
        else:
            print("There is no datafile to process.")
    
        #Load the input file 
        solar_data = pd.read_csv(datafile)
        print(solar_data.head())
        print(processed_folder)
    
        #Fetch CAMS data
        cams_details = fetch_cams_data(solar_data, start_date=start_date, end_date=end_date, email=email, \
                                     identifier=sky_type, time_step=time_step, time_ref=time_reference, server=server_name, \
                                     timeout=timeout)
        if (len(cams_details) == 0):
            raise f"fetch_cams_data functions returned an empty dataframe"
    
        #Validate output 
        status = validate_output(cams_details, start_date, end_date, time_step, results_folder)
        
        if status:
            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)
            print(processed_file)
            os.rename(datafile, processed_file)

            
    except Exception as ex:
        print(f"Main: An error occured {ex}")
        traceback.print_exc()
        


# In[12]:


if __name__ == "__main__":
    main()


# In[ ]:




