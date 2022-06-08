import pandas as pd
import os


for file_name in os.listdir('dataverse_files'):
    if '.bz2' in file_name:
        temp_file = pd.read_csv(file)
        new_file_name = file_name.replace('.bz2', '')
        temp_file.to_csv('decompressed/file_name')