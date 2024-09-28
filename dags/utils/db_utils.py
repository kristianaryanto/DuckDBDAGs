import pyodbc
from airflow.models import DAG, DagTag, DagModel, DagRun, Log, XCom, SlaMiss, TaskInstance, Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import duckdb

# con = duckdb.connect('/opt/airflow/duckdb.db')
def get_connection_odbc(Driver, Server, Port, Database, UID, PWD) -> pyodbc.Connection:
    conn = pyodbc.connect(
        f"DRIVER={Driver};"
        f"SERVER={Server};"
        "TrustServerCertificate=yes;"
        f"PORT={Port};"
        f"DATABASE={Database};"
        f"UID={UID};"
        f"PWD={PWD};"
        "Connection Timeout=30;"
    )
    return conn

def fetch_query(query):
    cursor = cnxn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    # print(rows)
    return rows

def get_and_list_filename(connection):
    con = duckdb.connect(connection)
    con.execute("CREATE TABLE IF NOT EXISTS push_files (filename VARCHAR)")
    first_item = con.execute("SELECT distinct filename FROM push_files LIMIT 1").fetchone()
    
    if first_item:
        # Remove the processed item from the table
        con.execute("DELETE FROM push_files WHERE filename = ?", (first_item[0],))
        
        # Process the first item (e.g., print it)
        result = first_item[0]
        final_value = result.replace('.csv', '')
        print(f"Processing: {final_value}")
        
        return final_value
    else:
        # push = fetch_query(query = "SELECT distinct SourceFileName FROM MASS_GL.dbo.Batch")
        # print(push)
        push = [('1723799445_Coba Mass Credit.csv',), ('1723799809_MAO TA - Normal.csv',), ('1723799947_Coba Mass Credit.csv',), ('1723800064_Coba Mass Credit.csv',), ('1723800077_Coba Mass Credit.csv',), ('1723800494_MAO NEW UI_.csv',), ('1724056224_2_GamauTesting_Type30.csv',), ('1724122576_Huhuhu_Test_Type10.csv',), ('1724124148_Xixixi_Test_Type30.csv',), ('1724128080_BismillahFix_ST2_Type30.csv',), ('1724138716_MASS_GL_TidakValid1juta.csv',), ('1724139690_MASS_GL_Valid1juta.csv',), ('1724139768_MASS_GL_MixValid1juta.csv',), ('1724144319_BismillahFix_ST2_Type30.csv',), ('1724144521_BismillahFix_ST2_Type30.csv',), ('1724146663_MASS_GL_Valid1juta_TypeTrx30.csv',), ('1724147649_SD_NOMINASI_TAHAP35_50000_22500000000_a.csv',), ('1724209073_Ewewewewew_Test_Type30.csv',), ('1724226985_MASS_GL_Valid20Data_TypeTrx10.csv',), ('1724227168_MASS_GL_Valid20Data_TypeTrx30.csv',), ('1724297456_new25.csv',), ('1724300508_BismillahFix_ST2_Type30.csv',), ('1724379916_BismillahFix_ST2_Type30.csv',), ('1724380079_BismillahFix_ST2_Type30.csv',), ('1724380116_SD_NOMINASI_TAHAP35_50000_22500000000_a.csv',), ('1724380198_BismillahFix_ST2_Type30.csv',), ('1724644886_test10.csv',), ('1724645120_test30.csv',), ('1724664470_Type10 (1).csv',), ('1724664698_Type30.csv',), ('1724816106_a178f48f-e216-44a4-b2da-0bfcc40d27f2.csv',), ('1724817664_Scenario - Normal - 1000 Data.csv',), ('1724817722_Scenario - Normal - 1000 Data.csv',), ('a.csv',), ('b.csv',), ('c.csv',), ('d.csv',), ('MASS_GL_Valid_TypeTrx10.csv',), ('MASS_GL_Valid_TypeTrx10_2.csv',), ('MASS_GL_Valid_TypeTrx10_3.csv',), ('MASS_GL_Valid_TypeTrx10_4.csv',), ('MASS_GL_Valid_TypeTrx30.csv',), ('MASS_GL_Valid_TypeTrx30_2.csv',), ('MASS_GL_Valid_TypeTrx30_3.csv',), ('MASS_GL_Valid_TypeTrx30_4.csv',), ('MASS_GL_Valid_TypeTrx30_5.csv',)]
        # Insert data into the DuckDB table
        con.executemany("INSERT INTO push_files VALUES (?)", push)
        filename = get_and_list_filename(connection)
        print("refresh data and start again")
        con.close()
        return filename



