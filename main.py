import os
import random
import timeit
import polars as pl
from utils import get_filestems, export_results
from utils import duckdb_lib, pandas_lib
from utils import polars_lib, polars_lib_lazy, pyarrow_lib


def main():
    if not os.path.exists("./data/parquet"):
        raise FileNotFoundError("Required directory './data/parquet' not found.\nPlease run 'extract_data.py' first to generate required files.")
    
    extracted_data = pl.read_csv("./extracted_files.csv").sort("num_rows")
    num_rows = extracted_data.get_column("num_rows").to_list()
    filestems = extracted_data.get_column("filename").to_list()
    filestems = get_filestems(filenames = filestems, ext = ".parquet")

    combined = list(zip(filestems, num_rows))
    random.shuffle(combined)

    filestems, num_rows = zip(*combined)

    results = {"duckdb_times": {},
               "pandas_times": {},
               "polars_times": {},
               "polars_lz_times": {},
               "pyarrow_times": {}}

    for filestem, n in zip(filestems, num_rows): 
        results["duckdb_times"][n] = timeit.timeit(lambda: duckdb_lib(filestem=filestem), number=1) 
        results["pandas_times"][n] = timeit.timeit(lambda: pandas_lib(filestem=filestem), number=1) 
        results["polars_times"][n] = timeit.timeit(lambda: polars_lib(filestem=filestem), number=1) 
        results["polars_lz_times"][n] = timeit.timeit(lambda: polars_lib_lazy(filestem=filestem), number=1) 
        results["pyarrow_times"][n] = timeit.timeit(lambda: pyarrow_lib(filestem=filestem), number=1)

    for method in results: 
        results[method] = dict(sorted(results[method].items()))
    
    export_results(results = results, filepath="./results.csv")


if __name__ == "__main__":
    main()
