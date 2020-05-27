import csv
import pathlib
import pandas
import global_constants as gc

# for print statement
pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)

raw_data_path = pathlib.Path().absolute() / gc.RAW_DATA_FOLDER_NAME
raw_data = []

for f in raw_data_path.rglob("*.csv"):
    raw_data.append(f)

class NamedDataFrame(object):
    def __init__(self, name, df):
        self.name = name
        self.df = df

def clean_columns(raw_data, to_drop):
    # list of all the clean data data frames
    clean_data = []
    for f in raw_data:
        # SubjectID is the unique identifier for a single participant
        df = pandas.read_csv(f)
        # hard coded - this should be changed
        df.drop(to_drop, inplace=True, axis=1)

        # pivoting data
        # create a row number by group
        df['rn'] = df.groupby('SubjectID').cumcount() + 1

        # pivot the table
        new_df = df.set_index(['SubjectID', 'rn']).unstack()

        # rename columns
        new_df.columns = [x + '_' + str(y) for (x, y) in new_df.columns]

        new_df.reset_index()

        # add the name of the file and the new data frame to the list of clean data data frames
        clean_data.append(NamedDataFrame(f.name, new_df))

    return clean_data


to_drop = ['Experiment', 'Schedule', 'TestName',
                   'MPoint', 'SessionName', 'SessionID', 'LaunchTime',
                   'StartTime', 'ResultTime', 'GMTOffset',
                   'Exception', 'Remark', 'blktime',
                   'blkno', 'block', 'trlno',
                   'trial', 'trlspec', 'trlid' ]

clean_data_path = pathlib.Path().absolute() / gc.CLEAN_DATA_FOLDER_NAME

cleaned_data = clean_columns(raw_data, to_drop)


def write_csv(cleaned_data):
    for named_frame in cleaned_data:
        # create a new csv in the clean_data_path directory, with the same name
        # as the original file
        path = clean_data_path / named_frame.name
        frame = named_frame.df
        frame_index = frame.index.name

        frame.to_csv(path, index=frame_index, header=True)


write_csv(cleaned_data)

