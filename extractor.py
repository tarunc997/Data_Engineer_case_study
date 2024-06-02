from pyspark.sql import SparkSession
import zipfile
import os

class extractor:
    def __init__(self):
        pass
    # Function to unzip the file and return a list of file paths        
    def extract_zip_files(self,zip_file_path):
        file_paths = []
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Extract all contents to a temporary directory
            zip_ref.extractall('temp_dir')
            # Access and process each file in the temporary directory
            for filename in zip_ref.namelist():
                file_path = os.path.join('temp_dir', filename)
                # Ensure it's a file, not a directory
                if os.path.isfile(file_path):
                    file_paths.append(file_path)
        print('Done extracting')
        return file_paths


if __name__=='__main__':
    zip_file_path = 'Data.zip'
    e=extractor()
    extracted_files = e.extract_zip_files(zip_file_path)





