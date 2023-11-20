import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def avg_cust_growth():
    connection = psycopg2.connect(
        dbname = os.environ['DBNAME'],
        user = 'postgres',
        password = os.environ['PASSWORD'],
        host = os.environ['HOST'],
        port = os.environ['PORT']
    )
    cursor = connection.cursor()
    query = """
    WITH MonthlyCustomers AS (
        SELECT
            DATE_TRUNC('month', "InvoiceDate"::timestamp) AS Month,
            COUNT(DISTINCT "CustomerID") AS NumCustomers
        FROM
            ecommerce_sales
        WHERE
            "CustomerID" IS NOT NULL
        GROUP BY
            DATE_TRUNC('month', "InvoiceDate"::timestamp)
    ), 
    MonthlyGrowth AS (
        SELECT
            Month,
            NumCustomers,
            LAG(NumCustomers) OVER (ORDER BY Month) AS LastMonthCustomers
        FROM
            MonthlyCustomers
    )
    SELECT
        Month,
        COALESCE(NumCustomers - LastMonthCustomers, 0) AS CustomerGrowth
    FROM
        MonthlyGrowth;
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
    sales_df = avg_cust_growth()
    
    # Save the DataFrame to a CSV
    sales_df.to_csv('tableau/data_sources/avg_cust_growth.csv', index=False)