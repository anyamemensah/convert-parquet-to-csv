import os
from utils import extract_taxi_data


def main():
    num_rows = [100, 1000, 10000, 100000, 1000000, 10000000]

    if not os.path.exists("./data/parquet"):
        extract_taxi_data(month_start=1, month_stop=4, 
                          sample_sizes=num_rows,
                          output_dir="./data/parquet")
        print("Data extracted to './data/parquet'.")
    else:
        print("'./data/parquet directory' already exists. Execute 'main.py' to see performance stats.")

if __name__ == "__main__":
    main()
