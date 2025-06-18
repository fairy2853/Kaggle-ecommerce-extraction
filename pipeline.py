import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from cloud_saving.aws_save import aws_save
from cloud_saving.big_query_save import big_query_save

# Kaggle API
api = KaggleApi()
api.authenticate()

dataset = "carrie1/ecommerce-data"
api.dataset_download_files(dataset, path="data", unzip=True)

# Load CSV
df = pd.read_csv("data/data.csv", encoding="ISO-8859-1")
df.dropna(subset=["CustomerID"], inplace=True)
df = df[df["Quantity"] > 0]

# Process
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df["TotalPrice"] = df["Quantity"] * df["UnitPrice"]
df["Month"] = df["InvoiceDate"].dt.to_period("M")

# Save
os.makedirs("output", exist_ok=True)
df.to_parquet("output/processed_data.parquet", index=False)

summary = (
    df.groupby(["Month", "Country"])["TotalPrice"]
    .sum()
    .reset_index()
    .sort_values("TotalPrice", ascending=False)
)

summary.to_csv("output/sales_summary.csv", index=False)


if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
    aws_save()

if os.getenv("GOOGLE_CREDENTIALS_JSON") and os.path.exists(
    os.getenv("GOOGLE_CREDENTIALS_JSON")
):
    big_query_save()
