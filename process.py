from data_utilities import *

clean_data_path = pathlib.Path().absolute() / gc.CLEAN_DATA_FOLDER_NAME

def set_print_settings():
    """""
    Set print settings for debugging
    """""
    # set window options for print statement
    pandas.set_option('display.max_rows', 500)
    pandas.set_option('display.max_columns', 500)
    pandas.set_option('display.width', 1000)

def main():
    """""
    Use data_utilities to clean raw data, and 
    export raw data to new csv files with the same
    name as their raw data counterparts. 
    """""
    set_print_settings()

    raw_file_info_path = pathlib.Path().absolute() / gc.RAW_FILE_INFO_FOLDER_NAME
    column_structure = []
    for info_path in raw_file_info_path.rglob("*.csv"):
        df = pandas.read_csv(info_path)
        if info_path.name == gc.COLUMN_INFO_FILE_NAME:
            column_structure = df.columns.tolist()

    # populate the raw data path
    raw_data_path = pathlib.Path().absolute() / gc.RAW_DATA_FOLDER_NAME
    raw_data_file_paths = []
    # iterate through the csv files in the raw_data folder
    for path in raw_data_path.rglob("*.csv"):
        # append if and only if the columns match a specific form
        df = pandas.read_csv(path)

        if set(column_structure) == set(df.columns.tolist()):
            raw_data_file_paths.append(path)
        else:
            print('RAW DATA ERROR!\n'+ str(path) + ' was not processed. \nRaw data file column structure must contain the following: \n' + str(column_structure))


    cleaned_data = clean_columns(raw_data_file_paths)
    write_csv(cleaned_data)




if __name__ == "__main__":
    main()