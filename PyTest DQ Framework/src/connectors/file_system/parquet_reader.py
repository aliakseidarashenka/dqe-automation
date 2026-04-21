import pandas as pd


class ParquetReader:
    def process(self, path: str, include_subfolders: bool = False) -> pd.DataFrame:
        """
        Reads parquet data from a file path.

        :param path: path to parquet file
        :param include_subfolders: reserved for future use
        :return: pandas DataFrame
        """
        try:
            df = pd.read_parquet(path)
            return df
        except Exception as e:
            raise Exception(f"Error reading parquet file from '{path}': {e}")