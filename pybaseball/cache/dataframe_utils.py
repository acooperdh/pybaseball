import polars as pl


def load_df(filename: str) -> pl.DataFrame:
    if filename.lower().endswith('csv'):
        data = pl.read_csv(filename, index_col=0)
    elif filename.lower().endswith('parquet'):
        data = pl.read_parquet(filename)
    else:
        raise ValueError(f"Cache frame {filename} has an unsupported extension.")
    return data


def save_df(data: pl.DataFrame, filename: str) -> None:
    if filename.lower().endswith('csv'):
        data.to_csv(filename)
    elif filename.lower().endswith('parquet'):
        data.to_parquet(filename)
    else:
        raise ValueError(f"DataFrame {filename} is an unsupported type")
