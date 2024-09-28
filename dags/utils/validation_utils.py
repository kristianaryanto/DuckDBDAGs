import pyarrow.csv as pv
import pyarrow as pa
import asyncio

# 1. Function to read CSV using pyarrow and convert to list of rows and column names
def read_csv_as_pyarrow_table(path_to_csv):
    """Reads a CSV file into a pyarrow.Table and converts it to list of rows and column names"""
    try:
        # Read the CSV into a pyarrow Table
        table = pv.read_csv(path_to_csv)
        
        # Extract column names
        column_names = table.column_names
        
        # Convert pyarrow Table to a list of rows (each row is a list of values)
        rows = [list(row) for row in table.to_pydict().values()]
        rows = list(map(list, zip(*rows)))  # Transpose to get rows as list of lists

        return rows, column_names
    except Exception as e:
        print(f"Error reading CSV {path_to_csv}: {e}")
        return None, None



# 3. Asynchronous function to process multiple CSV files and insert/update rows
async def process_csv_files(pool, list_of_paths, table_name, conflict_column):
    """Processes multiple CSV files concurrently using async pool and inserts or updates rows"""
    semaphore = asyncio.Semaphore(5)  # Control concurrency limit

    async def process_single_csv(path):
        async with semaphore:
            # Read CSV and convert it into rows and column names
            rows, column_names = await asyncio.to_thread(read_csv_as_pyarrow_table, path)
            
            if rows and column_names:
                # Call the insert_or_update function with the rows and column names
                await insert_or_update(pool, table_name, rows, column_names, conflict_column, semaphore)
                print(f"CSV {path} processed and inserted/updated into {table_name}.")
            else:
                print(f"Failed to process CSV {path}")

    tasks = [asyncio.create_task(process_single_csv(path)) for path in list_of_paths]
    await asyncio.gather(*tasks)

# Example usage
async def main(pool, list_of_paths, table_name, conflict_column):
    await process_csv_files(pool, list_of_paths, table_name, conflict_column)

# To run the main function
# asyncio.run(main(pool, ["path/to/csv1.csv", "path/to/csv2.csv"], "my_table", "id"))
