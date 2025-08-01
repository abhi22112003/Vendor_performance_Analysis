import os
import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_query(conn):
    query = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost 
        FROM vendor_invoice 
        GROUP BY VendorNumber
    ),
    PurchasesSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.volume,
            pp.price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchasesQuantity,
            SUM(p.Dollars) AS TotalPurchasesDollars
        FROM purchases p 
        JOIN purchase_prices pp 
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.PurchasePrice,
            pp.volume,
            pp.price
    ),
    SalesSummary AS (
        SELECT 
            Brand,
            VendorNo,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesPrice) AS TotalSalesPrice, 
            SUM(ExciseTax) AS TotalExciseTax 
        FROM sales
        GROUP BY 
            Brand,
            VendorNo
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.PurchasePrice,
        ps.volume,
        ps.Description,
        ps.ActualPrice,
        ps.TotalPurchasesQuantity,
        ps.TotalPurchasesDollars,
        ss.TotalSalesDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchasesSummary ps
    LEFT JOIN SalesSummary ss 
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs 
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchasesDollars DESC
    """, conn)
    return query

def clean_data(df):
    df['volume'] = df['volume'].astype('float64')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchasesDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchasesQuantity']
    df['SalesPurchaseRation'] = df['TotalSalesDollars'] / df['TotalPurchasesDollars']
    return df

# MAIN EXECUTION
if __name__ == '__main__':
    try:
        conn = sqlite3.connect('inventory.db')
        logging.info('Creating summary query DataFrame...')
        summary_df = create_query(conn)
        logging.info(f"\n{summary_df.head()}")

        logging.info('Cleaning data...')
        clean_df = clean_data(summary_df)
        logging.info(f"\n{clean_df.head()}")

        logging.info('Ingesting data into database...')
        ingest_db(clean_df, 'query', conn)

        logging.info('Process completed successfully.')
        conn.close()

    except Exception as e:
        logging.exception("An error occurred during the process.")
