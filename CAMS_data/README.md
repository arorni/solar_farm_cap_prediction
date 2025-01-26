# CAMS Data Processing

This project automate the access to CAMS (Copernicus Atmosphere Monitoring Service) solar irradiance data using the [pvlib CAMS API](https://pvlib-python.readthedocs.io/en/stable/reference/generated/pvlib.iotools.get_cams.html#id10). 

It includes utilities for setting up a folder structure, creating configuration file, and processing large solar farms dataset in chunks (keeping api limitation of processing 100 records each day in mind). 


## Features

- **Folder Structure Setup**: Automatically creates required directories for unprocessed, processed, and results data.
- **CAMS Data Fetching**: Retrieves CAMS solar radiation data for specified geographical locations and timeframes.
- **Data Validation**: Validates the retrived data against expected row counts to ensure completeness.
- **Configurable Parameters**: Flexible configuration through JSON files for various parameters like time intervals, server URLs, and more.
- **Sample Data**: Includes `france_solar_farms.csv` for testing CAMS data retrieval in CAMS_data folder, complete dataset for all the solar farms in france location can be found in the dataset folder.

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/arorni/solar_farm_cap_prediction/CAMS_data
   cd CAMS_data
   ```
2. Create a virtual environment (optional but recommended):
   - Using **Anaconda**:
     ```bash
     conda create -n cams_env
     conda activate cams_env
     ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
---
## Usage

### 1. Setting Up the Folder Structure
Run the `set_up.py` script to initialize the folder structure and configuration file:
```bash
python set_up.py --file_path <path-to-input-file> --sky_type <mcclear|cams_radiation> --start_date <YYYY-MM-DD> --end_date <YYYY-MM-DD> --time_step <1min|15min|1h|1d|1M> --time_reference <UT|TST> --email <your-email>
```
Example:
```bash
python set_up.py --file_path "./data/input.csv" --sky_type "mcclear" --start_date "2023-01-01" --end_date "2023-01-10" --time_step "1h" --time_reference "UT" --email "example@example.com" ```
```

### Outputs:
- config.json file created in the CAMS_data folder.
- processed, unprocessed, and results folders are created in the CAMS_data folder.
- csv files with 100 solar farms details are created from the dataset given at the command line. 

---
### 2. Processing CAMS Data
Run the `process_cams_data.py` script to fetch and validate CAMS data:

```bash
python process_cams_data.py
```

### Outputs:
- Processed input files are saved in the `processed` directory.
- CAMS results are saved in the `results` directory with a file name and the date.

---



