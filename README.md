# Text to SQL Lakehouse

The goal of the project is to shwocase a proof of concept on how text to sql can be applied for iceberg lakehouse. There is a lakehouse architecture set up with Airflow, Spark and Iceberg, and then Duckdb and XiYan-SQL is used to generate the text to sql.

This project is a fork of Joseph Machado's [beginner de project](https://github.com/josephmachado/beginner_de_project)
but with modifications to use iceberg and for text to sql application with [XiYan-SQL](https://github.com/XGenerationLab/XiYan-SQL).



## Run Data Pipeline


To run locally, you need:

1. [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
2. [Github account](https://github.com/)
3. [Docker](https://docs.docker.com/engine/install/) with at least 4GB of RAM and [Docker Compose](https://docs.docker.com/compose/install/) v1.27.0 or later

Clone the repo and run the following commands to start the data pipeline:

```bash
git clone https://github.com/paddelcourt/t2sql-lakehouse
cd t2sql-lakehouse
make up
sleep 30 # wait for Airflow to start
make ci # run checks and tests
uv pip install requirements.txt
```

Go to [http:localhost:8080](http:localhost:8080) to see the Airflow UI. Username and password are both `airflow`.

### Ingesting an example dataset

1. Install the [good wiki dataset](https://huggingface.co/datasets/euirim/goodwiki) from hugging face
2. Rename it to goodwiki.parquet and place it in the data folder. 
3. Then on the Airflow UI located in [http:localhost:8080](http:localhost:8080), run ingest_dag so that the parquet file can be stored into minio s3 which is located in [http:localhost:9080](http:localhost:9080).
4. You should see a bucket called warehouse which has two subfolders: raw and goodwiki. raw stores the parquet file and goodwiki stores the iceberg version.


## Test Text To SQL 

Please pip install requirements.txt to use the notebook.

Follow the steps on the [text_to_sql_iceberg.ipynb](https://github.com/paddelcourt/t2sql-lakehouse/blob/master/t2sql/text_to_sql_iceberg.ipynb) notebook which will:
1. Query the Iceberg table with Duckdb connected to Minio S3
2. Generate an [M-Schema](https://github.com/XGenerationLab/M-Schema) to be used as prompt template for Text to SQL 
3. Inference with XGenerationLab/XiYanSQL-QwenCoder-7B-2504 model



### Example of the interaction

```
Question: 
Can you get me the description of the louvre?

LLM Response: 
SELECT description FROM goodwiki WHERE title = 'Louvre'

SQL Query Result:
'Art museum in Paris, France'

```
## Architecture

This data engineering project, includes the following:

1. **`Airflow`**: To schedule and orchestrate DAGs.
2. **`Postgres`**: To store Airflow's details (which you can see via Airflow UI) and also has a schema to represent upstream databases.
3. **`Spark`**: To ingest data and create Iceberg tables.
4. **`Iceberg`**: To act as our storage method
5. **`DuckDB`**: To act as our querying engine

For simplicity services 1-4 of the above are installed and run in one container defined [here](./containers/airflow/Dockerfile).





For the blog on the data engineering templates by Joseph Machado, check out **[this post](https://www.startdataengineering.com/post/data-engineering-projects-with-free-template/)**.