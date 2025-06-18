from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def big_query_save():
    credentials = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_CREDENTIALS_JSON")
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    df = pd.read_csv("output/sales_summary.csv")

    table_id = "your-project-id.dataset_name.sales_summary"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print("✅ uploded to bigquery")
