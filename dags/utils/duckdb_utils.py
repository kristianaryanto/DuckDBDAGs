import duckdb
import os

def duckdb_get_conncection(connection =':memory:'):
    """
    Establishes a connection to a DuckDB database.

    Args:
        connection (str): The path to the database file. Defaults to ':memory:'.

    Returns:
        duckdb.DuckDBPyConnection: A connection object to the DuckDB database.
    """
    try:
        con = duckdb.connect(database=connection)
    except:
        con = duckdb.connect(':memory:')
    return con

def duckdb_query(
    connection ,
    sql_file_query = None,
    output_file_path = None,
    type_output = None):
    
    if type_output is not None:
        print(connection)
        result = duckcb_output(
            connection=connection,
            query=sql_file_query,
            output_file_path=output_file_path,
            type_output=type_output
        )
    else:
        connectiondb = duckdb_get_conncection(connection)
        query = open(sql_file_query, "r").read()
        result = connectiondb.execute(query).df()

    return result

def duckcb_output(
    connection,
    query,
    output_file_path,
    type_output)  :
    print(connection)
    connection = duckdb_get_conncection(connection)

    if type_output == 'csv':
        result_output = connection.execute(f"COPY ({query}) TO '{output_file_path}' (DELIMITER ';')")
    elif type_output == 'json':
        result_output = connection.execute(f"COPY ({query}) TO '{output_file_path}' ")
    elif type_output == 'parquet':
        result_output = connection.execute(f"COPY ({query}) TO '{output_file_path}' (FORMAT PARQUET) ")
    
    return result_output

def duckdb_columns_uppercase(
    connection,
    csv_file
    ):
    
    result = connection.execute(f"SELECT * FROM read_csv_auto('{csv_file}', delim=';', header=True) LIMIT 1").df()
    original_columns = result.columns
    
    # Generate the SQL for creating the table with uppercase column names
    uppercased_columns = [f"UPPER('{col}') AS \"{col.upper()}\"" for col in original_columns]
    select_query = ", ".join(uppercased_columns)

    return select_query

def duckdb_validate_columns_csv(
    connection,
    csv_file,
    table_name):
    connection = duckdb_get_conncection(connection)
    # table_name = "validate".join(table_name)
    # Load the CSV into DuckDB
    # con = duckdb.connect(database=':memory:')
    
    # Generate the SQL for creating the table with uppercase column names
    select_query = duckdb_columns_uppercase(connection, csv_file)
    
    # Create the table with uppercased column names
    # print(f"CREATE TABLE {table_name} AS SELECT {select_query} FROM read_csv_auto('{csv_file}', delim=';', header=True);")
    try:
        connection.execute(f"""CREATE TABLE "{table_name}" AS SELECT {select_query} FROM read_csv_auto('{csv_file}', delim=';', header=True);""")
    except:
        print("error")
        connection.execute(f"""DROP TABLE "{table_name}";""")
        connection.execute(f"""CREATE TABLE "{table_name}" AS SELECT {select_query} FROM read_csv_auto('{csv_file}', delim=';', header=True);""")
    
    # Define validation rules for all columns
    validation_rules = {
        "ACCOUNTNUMBER": {
            "regex": "^([0-9]{4})+01+([0-9]{9}$)",
            "maxlen": 15,
            "blacklist": "^([0-9]{4})+01+([0-9]{6})+(3|5|9)+([0-9]{2}$)"
        },
        "REMARK": {
            "regex": "^[A-Za-z0-9 \-\,\.\(\)_]*$",
            "maxlen": 40,
            "default_value": "BMSPRO_"
        },
        "JUMLAHDATA": {
            "regex": "^[0-9]*$",
            "maxlen": 15
        },
        "NOMINAL": {
            "regex": "^[1-9]+[0-9]*(\\.([0-9]{2}$))?$",
            "maxlen": 18
        },
        "FEE": {
            "regex": "^[1-9]+[0-9]*(\\.([0-9]{2}$))?$",
            "maxlen": 18
        },
        "NOMINALNEW": {
            "regex": "^([1-9][0-9]{0,17})(\\.[0-9]{2})?$",
            "maxlen": 18
        },
        "BRANCHDEBET": {
            "regex": "^[0-9]*$",
            "maxlen": 4
        },
        "GLNUMBER": {
            "regex": "^[0-9]*$",
            "maxlen": 12
        },
        "NOMINAL1": {
            "regex": "^\\d{1,15}(\\.\\d{2})?$",
            "maxlen": 18
        },
        "NOMINAL2": {
            "regex": "^(0|[1-9]\\d{0,14})(\\.\\d{2})?$",
            "maxlen": 18
        }
    }
    
    csv_columns = connection.execute(f"""PRAGMA table_info("{table_name}")""").df()['name'].tolist()
   
    # Detect columns and create validation queries
    validation_queries = []
    for column in csv_columns:
        if column in validation_rules:
            rules = validation_rules[column]
            if "regex" in rules:
                validation_queries.append(f"""
                CASE 
                    WHEN {column} ~ '{rules['regex']}' THEN 'valid'
                    ELSE 'not_valid'
                END AS {column}_regex
                """)
            
            if "maxlen" in rules:
                validation_queries.append(f"""
                CASE 
                    WHEN LENGTH({column}) <= {rules['maxlen']} THEN 'valid'
                    ELSE 'not_valid'
                END AS {column}_maxlen
                """)
            
            if "blacklist" in rules:
                validation_queries.append(f"""
                CASE 
                    WHEN {column} ~ '{rules['blacklist']}' THEN 'blacklisted'
                    ELSE 'valid'
                END AS {column}_blacklist
                """)
            
            if "default_value" in rules:

                validation_queries.append(f"""
                CASE 
                    WHEN {column} IS NULL OR {column} = '' THEN '{rules['default_value']}'
                    ELSE 'not_valid'
                END AS {column}_default_value
                """)
    
    # Join all validation queries
    validation_query = ",\n".join(validation_queries)
    
    # Create the final query to add validation columns
    final_query = f"""
    SELECT *,
    {validation_query}
    FROM read_csv_auto('{csv_file}', delim=';', header=True)
    """
    # FROM read_csv_auto('{csv_file}', delim=';', header=True);
    # print(final_query)
    
    # Execute the query and get the results
    # result_df = con.execute(final_query).df()
    
    return final_query

# # Example usage
# csv_file = '/home/kris/vectordb/upsized_data_100MB'
# table_name = 'your_table'
# validated_data = validate_csv_columns(csv_file, table_name)

# # Display or further process the validated data
# print(validated_data)



# if __name__ == "__main__":
#     ducdb_query(
#     connection=':memory:',
#     sql_file_query = """/mnt/c/Users/Lawencon/Downloads/airflow_new/airflow_new/script/sql/duckdbsq.sql""",
#     output_file_path = '/mnt/c/Users/Lawencon/Downloads/airflow_new/airflow_new/data-lake/modified/duckdb_query_result' ,
#     type_output='csv'
#     )
    # airflow_new\