from pathlib import Path

from create_csv_all import create_csv_all_data

        
if __name__ == "__main__":

    print("Running main.py")

    # define path to dir holding data as several tsv files
    data_dir_path: Path = Path("./data")
    
    # define path to output csv file
    output_csv_path: Path = data_dir_path / "all_data.csv"
    
    # create csv with data from all files
    create_csv_all_data(data_dir_path, output_csv_path)
 