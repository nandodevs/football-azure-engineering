# Data Engineering Project - Football Stadiums

This project demonstrates a data engineering pipeline that extracts data from Wikipedia, processes and stores it in Azure Data Lake Gen2, and performs further transformations and analyses using Azure Data Factory and Databricks. The final data is visualized using Tableau, Power BI, and Looker Studio.

## Architecture

### Components

- **Wikipedia**: The source of raw data.
- **Apache Airflow**: Orchestrates the data pipeline, fetching data from Wikipedia and storing it in Azure Data Lake Gen2.
- **Azure Data Lake Gen2**: Stores raw and processed data.
- **Azure Data Factory**: Manages the ETL (Extract, Transform, Load) processes.
- **Databricks**: Performs data processing and transformation.

### Visualization Tools

- **Tableau**
- **Power BI**
- **Looker Studio**

## Prerequisites

- Azure Subscription
- Apache Airflow
- Azure Data Lake Gen2
- Azure Data Factory
- Databricks
- Tableau
- Power BI
- Looker Studio

## Setup

### Apache Airflow

1. **Install Apache Airflow**:

    ```sh
    pip install apache-airflow
    ```

2. **Create a DAG**:

    - Define a DAG to fetch data from Wikipedia and store it in Azure Data Lake Gen2.

### Azure Data Lake Gen2

1. **Create a Storage Account**:

    - Follow these instructions to create a storage account.

2. **Create a Container**:

    - Create a container to store raw and processed data.

### Azure Data Factory

1. **Create a Data Factory**:

    - Follow these instructions to create a Data Factory instance.

2. **Create Pipelines**:

    - Define pipelines to perform ETL processes.

### Databricks

1. **Create a Databricks Workspace**:

    - Follow these instructions to create a Databricks workspace.

2. **Create Notebooks**:

    - Define notebooks to process and transform the data.

## Visualization

- **Tableau**: Connect Tableau to Azure Data Lake Gen2 or Databricks to visualize the data.
- **Power BI**: Connect Power BI to Azure Data Lake Gen2 or Databricks to visualize the data.
- **Looker Studio**: Connect Looker Studio to Azure Data Lake Gen2 or Databricks to visualize the data.

## Running the Pipeline

1. **Start Apache Airflow**:

    ```sh
    airflow webserver -p 8080
    airflow scheduler
    ```

2. **Trigger the DAG**:

    - Trigger the DAG to start fetching data from Wikipedia.

3. **Monitor Data Factory**:

    - Monitor the ETL processes in Azure Data Factory.

4. **Run Databricks Notebooks**:

    - Execute Databricks notebooks for data processing and transformation.

5. **Visualize Data**:

    - Use Tableau, Power BI, or Looker Studio to create visualizations from the processed data.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
