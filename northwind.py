from posgres_conn import get_engine_from_settings
import pandas as pd
from google.cloud import bigquery

# Construct a BigQuery client object
DATASET = 'northwind'
TABLE = 'orders'
client = bigquery.Client()
dataset_ref = client.dataset(DATASET)
table_ref = dataset_ref.table(TABLE)

engine = get_engine_from_settings()
query = open('./query.sql', 'r')
df = pd.read_sql_query(query.read(), engine)
df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d')
df['required_date'] = pd.to_datetime(df['required_date'], format='%Y-%m-%d')
df['shipped_date'] = pd.to_datetime(df['shipped_date'], format='%Y-%m-%d')
query.close()

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("customer_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("company_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("product_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("category", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("required_date", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("shipped_date", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("ship_via", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("ship_country", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("freight", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("unit_price", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("quantity", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("discount", bigquery.enums.SqlTypeNames.FLOAT)
    ],
    write_disposition="WRITE_APPEND"
)

job = client.load_table_from_dataframe(
    dataframe=df, destination=table_ref, job_config=job_config
)
job.result()
