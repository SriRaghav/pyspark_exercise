import os

import pandas as pd


def convert_json_to_csv(json_file_path, csv_file_path):
    with open('../resources/yt.json', encoding='utf-8') as inputfile:
        df = pd.read_json(inputfile)

    df.to_csv('../resources/yt.csv', encoding='utf-8', index=False)


pandasDF = pd.read_csv('../resources/yt_small.csv')
print(pandasDF.columns)
