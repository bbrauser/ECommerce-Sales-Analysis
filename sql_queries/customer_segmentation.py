import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def cust_seg():
    connection = psycopg2.connect(
        dbname = os.environ['DBNAME'],
        user = 'postgres',
        password = os.environ['PASSWORD'],
        host = 'localhost',
        port = '5433'
    )
    cursor = connection.cursor()
    query = """
    WITH CustomerMetrics AS (
        SELECT
            "CustomerID",
            COUNT(DISTINCT "InvoiceNo") AS PurchaseFrequency,
            AVG("UnitPrice" * "Quantity") AS AvgPurchaseValue
        FROM
            ecommerce_sales
        WHERE
            "CustomerID" IS NOT NULL
        GROUP BY
            "CustomerID"
    )
    SELECT
        "CustomerID",
        PurchaseFrequency,
        AvgPurchaseValue,
        CASE
            WHEN PurchaseFrequency > 5 AND AvgPurchaseValue > 50 THEN 'High Value'
            WHEN PurchaseFrequency <= 5 AND AvgPurchaseValue <= 50 THEN 'Low Value'
            ELSE 'Mid Value'
        END AS Segment
    FROM
        CustomerMetrics;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Get column headers from cursor description
    columns = [desc[0] for desc in cursor.description]
    
    # Convert results into a pandas DataFrame
    df = pd.DataFrame(results, columns=columns)
    
    cursor.close()
    connection.close()
    return df

if __name__ == "__main__":
    sales_df = cust_seg()
    
    # Save the DataFrame to a CSV
    sales_df.to_csv(os.environ['TABLEAU_CUST_SEG'], index=False)