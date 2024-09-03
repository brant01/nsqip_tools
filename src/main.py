from pathlib import Path

from create_csv_all import create_csv_all_data
from utils import preprocess_files

        
if __name__ == "__main__":

    print("Running main.py")
    
    # define path to dir holding raw data as several tsv files
    data_dir_path_raw: Path = Path("data/raw_data")
    
    # define path to dir holding data as several tsv files
    data_dir_path: Path = Path("data")
    
    # preprocess data files to avoid utf8 errors
    print(f"Preprocessing files in {data_dir_path_raw}")
    preprocess_files(data_dir_path_raw, data_dir_path)
    print(f"Preprocessing done, cleaned files saved in {data_dir_path}")

    # define path to output csv file
    output_csv_path: Path = data_dir_path / "all_data.csv"
    
    # create csv with data from all files
    create_csv_all_data(data_dir_path, output_csv_path)
 