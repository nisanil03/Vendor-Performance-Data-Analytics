import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vandor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and adding new colums in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS(
    SELECT
        VendorNumber,
        SUM(Freight) As FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber  
),

PurchaseSummary AS(
    SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price as ActualPrice,
        pp.Volume,
        SUM(p.Quantity) AS TotalPurchaseQuantity,
        SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume     
),

SalesSummary AS(
    SELECT
        s.VendorNo,
        s.Brand,
        SUM(s.SalesQuantity) As TotalSalesQuantity,
        SUM(s.SalesDollars) As TotalSalesDollars,
        SUM(s.SalesPrice) As TotalSalesPrice,
        SUM(s.ExciseTax) As TotalExciseTax
    FROM sales s
    GROUP BY s.VendorNo, s.Brand
)

SELECT
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs
    ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC
""", conn)

    return vendor_sales_summary



def clean_data(df):
    '''This function will clean the data'''
    # changing datatype to float
    df['Volume'] = df['Volume'].astype('float')

    # filling missing value with 0 
    df.fillna(0,inplace = True)

    # removing space from Categorical columns 
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating new colums for better analysis
    vender_sales_summary['GrossProfit'] = vender_sales_summary['TotalSalesDollars'] - vender_sales_summary['TotalPurchaseDollars']
    vender_sales_summary['ProfitMargin'] = (vender_sales_summary['GrossProfit'] / vender_sales_summary['TotalSalesDollars']) *100 
    vender_sales_summary['StockTurnover'] = vender_sales_summary['TotalSalesQuantity'] / vender_sales_summary['TotalPurchaseQuantity']
    vender_sales_summary['SalestoPurchaseRatio'] = vender_sales_summary['TotalSalesDollars']/ vender_sales_summary['TotalPurchaseDollars']


    return df

if __name__ == '__main__':
    # Creating dataabase connection 
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table.....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Clening Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting Data.....')
    ingest_df = (clean_df,'vender_sales_summary',conn)
    logging.info('Completed')






















