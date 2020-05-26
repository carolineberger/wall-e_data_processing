import csv
import pathlib

RAW_DATA_FOLDER_NAME = 'raw_data'

projectPath = pathlib.Path().absolute() / RAW_DATA_FOLDER_NAME
rawData = []

for p in projectPath.rglob("*.csv"):
    rawData.append(p)

