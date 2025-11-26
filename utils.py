import os
import csv
import duckdb
import shutil
import polars as pl
import pandas as pd
import pyarrow.csv as pc
import pyarrow.parquet as pq


def export_results(results: dict, filepath: str) -> None:
    """
    Export performance stats
        
    :param dict df: results dict
    :param str filepath: filepath to exported results (CSV)
    """
    methods = []
    sizes = []
    times = []

    for method, results in results.items():
        for size, time in results.items():
            methods.append(method.replace("_times", ""))
            sizes.append(size)
            times.append(time)

    (
        pl.DataFrame({"method": methods, "size": sizes, "time": times})
            .pivot(index="size", on="method", values="time")
            .write_csv(filepath)
    )

    print(f"Results exported to: {filepath}")


def get_filestems(filenames: list[str], ext: str) -> list[str]:
    """ 
    Get file stems

    :param list[str] filenames: list of filenames
    :param str ext: file extension
    """
    return [f.split(".")[0] for f in filenames if f.endswith(ext)]


def create_samples(df: pl.DataFrame,
                   month_start: int,
                   month_stop: int,
                   sample_sizes: list[int],
                   output_dir: str) -> None:
        """
        Create samples based on supplied sample sizes
        
        :param pl.DataFrame df: polars data frame
        :param int month_start: Month start (1 = Jan; 12 = Dec)
        :param int month_stop: Month stop (1 = Jan; 12 = Dec)
        :param list[int] sample_sizes: list of sample sizes
        :param str output_dir: output directory
        """
        
        os.makedirs(output_dir, exist_ok=True)

        if not (1 <= month_start <= 12) or not (1 <= month_stop <= 12): 
            raise ValueError(f"month_start and month_stop must be between 1 (Jan) and 12 (Dec).") 

        if month_start > month_stop:
            raise ValueError(f"month_start cannot be greater than month_stop.")
        
        records = [] 
        for i in sample_sizes:
            filename = f"taxi_data_2024-{month_start:02d}{month_stop:02d}_{i}.parquet"
            df.sample(n=i, seed=721).write_parquet(f"{os.path.join(output_dir, filename)}")
            records.append((i, filename))

        with open("./extracted_files.csv", "w", newline="") as f:
            writer = csv.writer(f) 
            writer.writerow(["num_rows", "filename"])
            writer.writerows(records)


def extract_taxi_data(month_start: int = 1, 
                      month_stop: int = 4, 
                      sample_sizes: list[int]|None = None, 
                      output_dir: str =  "./data/parquet") -> None:
    """
    Extract 2024 Yellow taxi trip records
        
    :param int month_start: Month start (1 = Jan; 12 = Dec)
    :param int month_stop: Month stop (1 = Jan; 12 = Dec)
    :param list[int] sample_sizes: list of sample sizes
    :param str output_dir: output directory
    """
    if sample_sizes is None:
            sample_sizes = [100, 1000, 10000, 100000, 1000000, 10000000]

    urls = []
    for month in range(month_start,month_stop+1):
        urls.append(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month:02d}.parquet")
    
    with duckdb.connect(database = ":memory:") as con:
        df = con.sql(f"""
                    SELECT *
                    FROM read_parquet({urls}, union_by_name=true, filename=true)
                    """).pl()
    
    create_samples(df = df,
                   month_start = month_start,
                   month_stop = month_stop,
                   sample_sizes = sample_sizes,
                   output_dir = output_dir)
    

def duckdb_lib(filestem: str, 
               input_dir: str = "./data/parquet", 
               output_dir: str = "./data/csv") -> None:
    """
    Convert Parquet file to CSV using DuckDB.
    
    :param str filestem: file stem for parquet file
    :param str input_dir: filepath to input directory
    :param str output_dir: filepath to output directory
    """
    os.makedirs(output_dir, exist_ok=True)
    input_parquet_path = os.path.join(input_dir, f"{filestem}.parquet")
    output_csv_path = os.path.join(output_dir, f"{filestem}.csv")

    try:
        with duckdb.connect(database=":memory:") as con:
            con.execute(f"""
                        COPY (SELECT * FROM read_parquet('{input_parquet_path}')) 
                        TO '{output_csv_path}' WITH (FORMAT csv, HEADER);
                        """)
    except Exception as e:
        print(f"Error processing '{input_parquet_path}' using DuckDB: {e}")
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def pandas_lib(filestem: str, 
               input_dir: str = "./data/parquet", 
               output_dir: str = "./data/csv",
               chunk_size: int = 500000) -> None:
    """
    Convert Parquet file to CSV using Pandas.
    
    :param str filestem: file stem for parquet file
    :param str input_dir: filepath to input directory
    :param str output_dir: filepath to output directory
    :param int chunk_size: maximum number of rows to write 
    at a time
    """
    os.makedirs(output_dir, exist_ok=True)
    input_parquet_path = os.path.join(input_dir, f"{filestem}.parquet")
    output_csv_path = os.path.join(output_dir, f"{filestem}.csv")
  
    try:
        ( 
            pd.read_parquet(input_parquet_path, engine="pyarrow")
                .to_csv(output_csv_path, mode="a", chunksize=chunk_size)
        )
    except Exception as e:
        print(f"Error processing '{input_parquet_path}' using pandas: {e}")
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def pyarrow_lib(filestem: str, 
                input_dir: str = "./data/parquet", 
                output_dir: str = "./data/csv",
                chunk_size: int = 500000) -> None:
    """
    Convert Parquet file to CSV using PyArrow.
    
    :param str filestem: file stem for parquet file
    :param str input_dir: filepath to input directory
    :param str output_dir: filepath to output directory
    """
    os.makedirs(output_dir, exist_ok=True)
    input_parquet_path = os.path.join(input_dir, f"{filestem}.parquet")
    output_csv_path = os.path.join(output_dir, f"{filestem}.csv")

    try:
        tbl = pq.read_table(input_parquet_path)
        pc.write_csv(tbl, output_csv_path, 
                     write_options=pc.WriteOptions(
                         include_header=True, 
                         batch_size=chunk_size))
    except Exception as e:
        print(f"Error processing '{input_parquet_path}' using PyArrow: {e}")
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def polars_lib(filestem: str, 
               input_dir: str = "./data/parquet", 
               output_dir: str = "./data/csv") -> None:
    """
    Convert Parquet file to CSV using Polars.
    
    :param str filestem: file stem for parquet file
    :param str input_dir: filepath to input directory
    :param str output_dir: filepath to output directory
    """
    os.makedirs(output_dir, exist_ok=True)
    input_parquet_path = os.path.join(input_dir, f"{filestem}.parquet")
    output_csv_path = os.path.join(output_dir, f"{filestem}.csv")
  
    try:
       pl.read_parquet(input_parquet_path).write_csv(output_csv_path)
    except Exception as e:
        print(f"Error processing '{input_parquet_path}' using polars: {e}")
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def polars_lib_lazy(filestem: str, 
                    input_dir: str = "./data/parquet",
                    output_dir: str = "./data/csv",
                    chunk_size: int = 500000) -> None:
    """
    Convert Parquet file to CSV using Polars
    (lazy read and sink to csv).
    
    :param str filestem: file stem for parquet file
    :param str input_dir: filepath to input directory
    :param str output_dir: filepath to output directory
    :param int chunk_size: maximum number of rows that each output 
    CSV partition (chunk) should contain
    """
    os.makedirs(f"{output_dir}/{filestem}", exist_ok=True)
    input_parquet_path = os.path.join(input_dir, f"{filestem}.parquet")
  
    try:
       ( 
        pl.scan_parquet(input_parquet_path)
            .sink_csv(pl.PartitionMaxSize(
                base_path = f"{output_dir}/{filestem}", 
                max_size=chunk_size))
       )
    except Exception as e:
        print(f"Error processing '{input_parquet_path}' using polars (lazy): {e}")
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)

