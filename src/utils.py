from pathlib import Path
import polars as pl

def get_column_names(data_file: str) -> list[str]:

    lazy_df = (
        pl.scan_csv(
            data_file,
            n_rows=0,
    )
    )
        
    col_names = lazy_df.collect_schema().names()
    
    return col_names


def preprocess_files(input_path: Path, output_path: Path) -> None:
    # Ensure the output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate the list of .txt files in the input directory
    txt_files = list(input_path.glob("*.txt"))
    
    print(f"Found {len(txt_files)} .txt files in {input_path}")
    
    for data_file in txt_files:
        try:
            with open(data_file, "r", encoding='utf-8', errors='replace') as file:
                content = file.read()
                
            # Replace the problematic placeholder characters if necessary
            cleaned_content = content.replace('�', '')  # Adjust based on your needs (replace or ignore)
            
            # Create new file path with '_cleaned' before the .txt extension
            clean_data_file = output_path / (data_file.stem + '_cleaned' + data_file.suffix)

            # Write the cleaned content to the new file
            with open(clean_data_file, 'w', encoding='utf-8') as file:
                file.write(cleaned_content)
                    
            print(f"Cleaned {data_file} -> {clean_data_file}")
                    
        except Exception as e:
            print(f"Error processing {data_file}: {e}")