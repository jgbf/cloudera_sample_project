import pandas as pd
import os


for file_name in os.listdir('dataverse_files'):
    if '.bz2' in file_name:
        if file_name in ['1995.csv.bz2', '1994.csv.bz2', '2000.csv.bz2', '2001.csv.bz2', '2002.csv.bz2', '2008.csv.bz2']:
            try:
                temp_file = pd.read_csv(f'dataverse_files/{file_name}')
                new_file_name = file_name.replace('.bz2', '')
                temp_file.to_csv(f'decompressed/{new_file_name}')
            except Exception as e:
                print(e, file_name)
