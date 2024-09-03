from pathlib import Path
import polars as pl


def create_csv_all_data(data_dir_path: Path,
                        output_csv_path: Path) -> None:
    
    # get all txt files in data_dir_path
    data_files = list(data_dir_path.glob("*.txt"))
    
    # clean all files to utf-8
    clean_files(data_files)
    
    # get all columns from all files   
    all_columns: set[str] = _get_columns_from_all_files(data_files)
    
    # get a list of lazy frames for each file with missing columns added
    lazy_frames: list[pl.LazyFrame] = _add_missing_columns_with_null(data_files, all_columns)

    # concatenate all lazy frames into one
    all_data_lazy = pl.concat(lazy_frames)
    
    all_data_lazy.sink_csv(output_csv_path)
    
    print(f"Data saved to {output_csv_path}")


def clean_files(data_files: list[Path]) -> None:
    try:
        for data_file in data_files:
            with open(data_file, "rb") as file:
                content = file.read().decode('utf-8', 'replace')
                
            # Replace the problematic placeholder characters if necessary
            cleaned_content = content.replace('�', '')  # Adjust based on your needs (replace or ignore)
    
            with open(data_file, 'w', encoding='utf-8') as file:
                file.write(cleaned_content)
                    
            print(f"Cleaned {data_file}")
                    
    except Exception as e:
        print(f"Error cleaning files: {e}")
        raise e
    
def _add_missing_columns_with_null(data_files: list[Path], 
                                  all_columns: set[str]) -> list[pl.LazyFrame]:
    
    lazy_dfs: list[pl.LazyFrame] = []
    
    for data_file in data_files:
        
        print(f"Adding missing columns to {data_file}")
        
        # scan the file lazily
        lazy_df = pl.scan_csv(data_file,
                              separator="\t",
                              has_header=True,
                              infer_schema_length=None,
                              null_values=["BYPASS GRAFT WITH VEIN BRACHIAL-ULNAR/�RADIAL"])
        
        # add missing columns with null values
        missing_cols = all_columns - set(lazy_df.collect_schema().names())
        
        for missing_col in missing_cols:
            lazy_df = (
                lazy_df
                .with_columns(pl.lit(None)
                              .alias(missing_col))
            )
            
        # reorder columns to match all_columns
        lazy_df = lazy_df.select(all_columns)
        
        # append to list
        lazy_dfs.append(lazy_df)
        
        print(f"Added missing columns to {data_file}")
        
    # return list of lazy frames
    return lazy_dfs

def _get_columns_from_all_files(data_files: list[Path]) -> set[str]:
    
    # newer versions have additional columns, go through all 
    # files to ensure all columsn are in schema
    all_columns = set()
    
    # files are lange, for each just get the header
    for data_file in data_files:
        temp_df = pl.scan_csv(data_file,
                         separator="\t",
                         has_header=True,
                         n_rows=0)
        all_columns.update(temp_df.collect_schema().names())
        
    return all_columns