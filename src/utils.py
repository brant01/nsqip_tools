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