import pathlib
import global_constants as gc
from data_utilities import *

global clean_data_path
clean_data_path = pathlib.Path().absolute() / gc.CLEAN_DATA_FOLDER_NAME

def set_print_settings():
    # set window options for print statement
    pandas.set_option('display.max_rows', 500)
    pandas.set_option('display.max_columns', 500)
    pandas.set_option('display.width', 1000)

def main():
    set_print_settings()
    # populate the raw data path
    raw_data_path = pathlib.Path().absolute() / gc.RAW_DATA_FOLDER_NAME
    raw_data_file_paths = []
    # iterate through the csv files in the raw_data folder
    for path in raw_data_path.rglob("*.csv"):
        raw_data_file_paths.append(path)


    ### MAKE TEXT FILE THAT CAN CHANGE
    to_drop = ['Experiment', 'Schedule', 'TestName',
               'MPoint', 'SessionName', 'SessionID', 'LaunchTime',
               'StartTime', 'ResultTime', 'GMTOffset',
               'Exception', 'Remark', 'blktime',
               'blkno', 'block', 'trlno',
               'trial', 'trlspec', 'f1',
               'f2', 'f3', 'f4', 'msgblk_v1', 'msgblk_v2',
               'msgblk_v4', 'msgblk_v5', 'msgblk_v7', 'msgblk_v8',
               'lab1', 'lab2', 'lab3', 'lab4', 'lab5', 'lab6', 'lab7',
               'lab8', 'lab9', 'lab10', 'lab11', 'lab12', 'lab13', 'lab14',
               'lab15', 'lab16', 'lab17', 'lab18', 'lab19'
               ]

    cleaned_data = clean_columns(raw_data_file_paths, to_drop)
    write_csv(cleaned_data)


if __name__ == "__main__":
    main()