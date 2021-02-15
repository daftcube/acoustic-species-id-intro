# -*- encoding: utf-8 -*-
"""
Stratified Random Sampler

Takes data from the Acoustic Monitoring CSV format
and creates a new CSV containing a random sample from
each hour of the day.
"""

import pandas
import sys

DEBUG_MODE = 0 # 0 = Off, 1 = IMPORTANT, 2 = ALL
USE_COMMAND_LINE_ARGS = False # When true, reads file names from command line

MINIMUM_RECORD_DURATION = 60 # 60 sec per clip to be valid.

AUDIOMOTH_ID_COL = "AudioMothCode"
ERROR_COL = "Error"
DURATION_COL = "Duration"
RECORD_START_COL = "StartDateTime"

# Removes all rows that contain an error.
def remove_erroneous_rows(imported_data):
    return imported_data[imported_data[ERROR_COL].isnull()]

# pick_random_sample
# Attempts to select one random sample per hour and places it in the
# random_sample_output DataFrame. If the device_id does not have at least
# one documented sample for every hour, (hours 0-23,) then the function
# returns False. If the operation was successful, then the function
def pick_random_sample(imported_data, device_id):
    
    # Create temp output location.
    this_samples = pandas.DataFrame(columns = imported_data.columns)
    
    # Filter for device samples with minimum length.
    device_id_samples = imported_data[imported_data[AUDIOMOTH_ID_COL] == device_id] #.str.match(device_id)
    device_id_samples_of_min_length = device_id_samples[device_id_samples[DURATION_COL] > MINIMUM_RECORD_DURATION];
    
    # Verify there exists at least one valid entry per hour.
    for hour in range(0, 24):
        data_for_hour = device_id_samples_of_min_length[device_id_samples_of_min_length[RECORD_START_COL].dt.hour == hour]
        if len(data_for_hour.index) < 1:
            print(device_id + " did not have any recordings for hour " + str(hour) +" and is thus invalid.")
            return False, None
        else:
            this_samples = this_samples.append(data_for_hour.sample())
    
    
    return True, this_samples

# If USE_COMMAND_LINE_ARGS, this will return the given file from the command
# line. If not, it will open a text prompt to pick the file.
def get_file_to_open() -> str: 
    if USE_COMMAND_LINE_ARGS:
        if not( len(sys.argv) == 2 ):
            print("Incorrect usage: ./stratifiedRandomSampler.py csvFilePathInput csvFileOutput", file=sys.stderr)
            return None
        else:
            return sys.argv[0]
    else:
        read_input = input("Enter input file path: ")
        return read_input

def get_file_output_path() -> str:
    if USE_COMMAND_LINE_ARGS:
        if not( len(sys.argv) == 2 ):
            print("Incorrect usage: ./stratifiedRandomSampler.py csvFilePath csvFileOutput", file=sys.stderr)
            return None
        else:
            return sys.argv[1]
    else:
        read_output = input("Enter output file path: ")
        return read_output
    

# Main program execution
def main():
    
    # Get File Path to CSV
    input_file_path = get_file_to_open()
    if input_file_path is None:
        return False
    output_file_path = get_file_output_path()
    if output_file_path is None:
        return False
    
    # Try to read CSV
    imported_data = None
    try:
        imported_data = pandas.read_csv(input_file_path)
    except Exception as e:
        sys.stderr.write("Error when opening CSV File. See: " + str(e) + "\n")
        sys.stderr.flush()
        return False
    
    # Filter out errors
    print("Data loaded! Removing Erroneous Rows...")
    imported_data = remove_erroneous_rows(imported_data)
    print("Erroneous rows removed!")
    
    # Convert to DateTime format
    # Fix errors with datetime
    print("Converting strings to dates... (This will take some time.)")
    imported_data[RECORD_START_COL] = pandas.to_datetime(imported_data[RECORD_START_COL], errors='coerce')
    print("Strings converted!")
    
    # Get a list of every device.
    print("Getting devices...")
    device_samples = imported_data[AUDIOMOTH_ID_COL].unique().tolist()
    print("Device list created.")
    
    # Create random sample data frame, using same columns as input but
    # otherwise empty.
    df_output = pandas.DataFrame(columns = imported_data.columns)
    
    # For each device, first check if it has a complete set of samples.
    # If so, then pick a random sample from each hour.
    for device in device_samples:
        print("Attempting to sample from Device ID " + device + "...")
        did_select, samples_from_device = pick_random_sample(imported_data, device)
        
        if did_select:
            print("Sample picked!")
            df_output = df_output.append(samples_from_device)
        else:
            print("Device is invalid.")
    
    
    print("Samples ready, outputting...")
    print("Done. Outputting")
    
    # Try to output and return result.
    try:
        df_output.to_csv(output_file_path)
    except Exception as e:
        sys.stderr.write("Error when writing to CSV File. See: " + str(e) + "\n")
        sys.stderr.flush()
        return False
        
    # Success!
    return True
    
    
main()