# Parquet → CSV: Four Python Libraries Compared

<img style="margin-top:0px;margin-left:0px;width:70vw" src="https://www.dropbox.com/scl/fi/0jzwama77j7cmns0ylqip/parquet_to_csv_results.png?rlkey=4iqv8ipumneao3g550km2h5cs&raw=1" alt="Conversion times (in seconds) from Parquet to CSV across all row sizes for four Python libraries: Polars, Pandas, PyArrow, and DuckDB.">

tl;dr Polars and DuckDB seem to offer the fastest Parquet-to-CSV conversions across all row sizes.

Steps to run the experiment:

1. Clone the [GitHub repository](https://github.com/anyamemensah/convert-parquet-to-csv).
2. Navigate to the project directory.
3. Install all dependencies using:
```terminal
uv sync
```
4. Extract the experiment data by running:
```terminal
uv run extract_data.py
```
5. Execute the experiment with:
```terminal
uv run main.py
```

--

A while ago, I was asked by a former colleague about the best way to convert Parquet files into comma-separated values (CSV) format using Python. The honest answer? It depends.

It depends on:

* File size, location, access
* Schema complexity 
* How much time you have (to burn)
* The tools you're working with and the ecosystem/environment you're working in (e.g., local vs. cloud, preference for low code vs code forward, libraries available, memory limits, processing power you can <s>spare</s> afford, etc...)

And so on and so on...

A lot of devs and engineers get bent out of shape when talking about file formats. To be fair, they have a reason to. CSVs can be a convenient, simple way to exchange tabular data. But they can also be a pain when the files contain anomalies.

To this day, my favorite explanation of the CSV format is in [the following Hacker News thread](https://news.ycombinator.com/item?id=13265881) from 2016:

> CSV is less a file format and more a hypothetical construct like "peace on Earth".

No data types. No schemas. Just data vibes.

But I digress. 

Whenever someone throws a "theoretical" question my way, I can't help but put on my scientist hat and run a little experiment.

**The mission**: Figure out which Python library can convert a Parquet file into CSV the fastest, especially when the number of rows varies.

**How I'm testing**:

* To keep it simple, I'm running everything locally in VSCode's terminal. No data transformations, just a straight Parquet-to-CSV conversion

* Specs:
    - Operating System: macOS (M4 MacBook Pro) 15.7.1
    - Memory: 24 GB
    - Storage: 1 TB
    - VSCode: 1.106.2

* Python (Version 3.13) libraries used
    - DuckDB (1.4.2)
    - Polars (1.35.2)
    - Pandas (2.3.3)
    - PyArrow (22.0.0)


* Data: [2024 NYC Taxi and Limousine Commission Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). I'll pull random samples to create six datasets with different row sizes:
    - 100 rows
    - 1,000 rows
    - 10,000 rows
    - 100,000 rows
    - 1,000,000 rows
    - 10,000,000 rows

### Extracting the Data and creating the Parquet files

First, I wrote a function called `extract_taxi_data()` to pull data from the NYC Taxi & Limousine Commission site that:

* Let's you choose start and end months for 2024 Yellow Taxi trip data
* Accepts a list of sample sizes (number of rows for each dataset)
* Takes an output directory (where save the sampled datasets)

```python
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
```

The data from the selected months is merged into one large dataset. From there, I use my helper function `create_samples()` to draw random samples for each specified row size and generate Parquet files with the exact number of rows.

```python
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
```

### Testing different libraries

#### DuckDB
DuckDB is an embedded analytics database optimized for fast processing and querying of large datasets. DuckDB is well-known for its speed and local efficiency, so it naturally made my list of libraries to test.

Here's the code I used to convert Parquet files to CSV with DuckDB:


```python
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
```

#### Pandas
Pandas is the go-to library for data processing in Python, so of course, it made the list. (Quick note: Pandas relies on either PyArrow or Fastparquet under the hood to read Parquet files. I went with PyArrow since it's the default.)

Here's the code I used to convert Parquet files to CSV using Pandas:

```python
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
```

#### PyArrow
PyArrow is practically synonymous with Parquet, but here's what surprised me when digging into the docs for this challenge: you can write a PyArrow.Table directly to a CSV file. I'd never thought of using PyArrow for that before, so I was excited to add it to the mix. To make the most of PyArrow's processing power, I bumped the `batch_size` up to 500,000 (the default is 1,024).

Here's the code I used to convert Parquet files to CSV using PyArrow:


```python
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
```


#### Polars 
Polars has emerged as a leading choice for fast, local data crunching in Python. I was curious about how its eager execution stacks up against lazy execution, so this little experiment uses two different Polars approaches.

The first approach uses `read_parquet()` to load the file into a DataFrame, then writes the entire dataset to a single CSV file using `write_csv()`.

```python
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
```

The second approach uses `scan_parquet()` together with `sink_csv()`. And just to make things interesting, I threw in the experimental `PartitionMaxSize()` method to split the data into chunks, each capped at 500,000 rows:

```python
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
```


### The process and results
To avoid processing datasets strictly from smallest to largest, I randomized the order in which each library handled the datasets. Then, I used Python's `timeit` module to measure how long each run took. The results were saved in a dict and then exported to a CSV file.

I created a simple line graph to visualize the conversion times:

<img style="margin-top:0px;margin-left:0px;width:70vw" src="https://www.dropbox.com/scl/fi/0jzwama77j7cmns0ylqip/parquet_to_csv_results.png?rlkey=4iqv8ipumneao3g550km2h5cs&raw=1" alt="Conversion times (in seconds) from Parquet to CSV across all row sizes for four Python libraries: Polars, Pandas, PyArrow, and DuckDB.">

The key takeaway: Polars (both lazy and eager modes) and DuckDB consistently delivered the fastest Parquet-to-CSV conversions across all row sizes. The performance gap compared to Pandas (less so PyArrow) became most noticeable when row counts exceeded 1 million.

Am I surprised? Nope. 

Now, if we wanted to make this *truly* scientific, we would need to repeat the runs many more times and calculate the average execution time for each process and row count.

Here are the final rankings (fastest to slowest) for converting a dataset with 10M rows:

1. Polars (lazy mode): 1.2 seconds
2. DuckDB: 1.7 seconds (~1.4 times slower than Polars lazy mode)
3. Polars (eager mode): 1.8 seconds (~1.4 times slower than Polars lazy mode)
4. PyArrow: 4.9 seconds (~4 times slower than Polars lazy mode)
5. Pandas: 49.5 seconds (~41 times slower than Polars lazy mode)

What’s your go-to library for Parquet-to-CSV conversion in Python? Was it featured? Drop me a note at ama@anyamemensah.com.
