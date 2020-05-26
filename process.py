import csv
import pathlib
import pandas


RAW_DATA_FOLDER_NAME = 'raw_data'


projectPath = pathlib.Path().absolute() / RAW_DATA_FOLDER_NAME
rawData = []

for f in projectPath.rglob("*.csv"):
    rawData.append(f)

def read_csv(rawData):
    for f in rawData:
        # SubjectID is the unique identifier for a single participant
        df = pandas.read_csv(f)
        # hard coded - this should be changed
        to_drop = ['Experiment', 'Schedule', 'TestName',
                   'MPoint', 'SessionName', 'SessionID', 'LaunchTime',
                   'StartTime', 'ResultTime', 'GMTOffset',
                   'Exception', 'Remark', 'blktime',
                   'blkno', 'block', 'trlno',
                   'trial', 'trlspec', 'trlid' ]
        df.drop(to_drop, inplace=True, axis=1)

        # for print statement
        pandas.set_option('display.max_rows', 500)
        pandas.set_option('display.max_columns', 500)
        pandas.set_option('display.width', 1000)

        # pivoting data
        # create a row number by group
        df['rn'] = df.groupby('SubjectID').cumcount() + 1

        # pivot the table
        new_df = df.set_index(['SubjectID', 'rn']).unstack()

        # rename columns
        new_df.columns = [x + '_' + str(y) for (x, y) in new_df.columns]


        new_df.reset_index()

        print(df)
        print(new_df)

read_csv(rawData)