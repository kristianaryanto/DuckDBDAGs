# Airflow DAGs for Data Pipelines

This repository contains Apache Airflow Directed Acyclic Graphs (DAGs) designed for ETL and data processing workflows. These DAGs have been optimized for high-performance data operations using Python and DuckDB.

## Overview

Apache Airflow is an open-source platform for programmatically authoring, scheduling, and monitoring workflows. Airflow allows the creation of complex data pipelines with features such as concurrency, retries, and logging.

In this project, we benchmarked Airflow against SQL Server Integration Services (SSIS) and observed significant improvements in performance.

## Features

- **Efficient Data Processing**: The DAGs leverage concurrency and DuckDB to process large datasets more quickly.
- **Extensibility**: Airflow allows easy customization of workflows to adapt to different data sources and processing requirements.
- **Monitoring**: Each workflow comes with detailed logging and monitoring capabilities via Airflow's built-in dashboard.
- **Version Control and CI/CD**: Supports continuous integration and deployment for updating pipelines in production.

## Benchmark Results

In a test comparing Airflow to SSIS using a dataset of 100,000 rows:

- **SSIS**: Failed to complete the task in a reasonable time, requiring manual intervention to stop.
- **Airflow**: Successfully processed the data in just 6 minutes, 5x faster than SSIS.

### Dataflow Architecture

- **Airflow**: Uses DuckDB for in-memory data processing with concurrency. The processed data is then transferred back to SQL Server, allowing for quicker data ingestion and transformation.
- **SSIS**: Relies solely on SQL Server for processing, resulting in slower performance due to high data loads.

## Installation

To run these DAGs on your own instance of Apache Airflow, follow the steps below:

1. **Clone the repository**:
    ```bash
    git clone https://github.com/yourusername/airflow-dags.git
    cd airflow-dags
    ```

2. **Set up your Airflow environment** (ensure you have Python 3.7+):
    ```bash
    pip install apache-airflow
    ```

3. **Install additional dependencies**:
    ```bash
    pip install duckdb
    ```

4. **Initialize Airflow**:
    ```bash
    airflow db init
    airflow users create --username admin --password admin --firstname Admin --lastname Admin --role Admin --email admin@example.com
    ```

5. **Start the Airflow web server and scheduler**:
    ```bash
    airflow webserver --port 8080
    airflow scheduler
    ```

6. **Access the Airflow Dashboard**:
    Open your browser and navigate to `http://localhost:8080`. You can now trigger and monitor your DAGs.

## Usage

1. Place your DAGs in the `dags/` folder of your Airflow home directory:
    ```bash
    cp -r dags/ $AIRFLOW_HOME/dags/
    ```

2. Start the Airflow scheduler and monitor the DAGs via the Airflow web UI.

## Contribution

Feel free to fork the repository and submit pull requests for improvements or bug fixes. We encourage contributions to add more complex workflows and optimize the existing ones.

## Benchmark Video

Watch the benchmarking test where Airflow processed 100,000 rows in 6 minutes [here](https://docs.google.com/file/d/1Vxco_3sPx048KPj0Y10BnbjC-lEMyENb/preview).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

