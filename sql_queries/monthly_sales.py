import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def monthly_sales():
    connection = psycopg2.connect(
        dbname = os.environ['DBNAME'],
        user = 'postgres',
        password = os.environ['PASSWORD'],
        host = 'localhost',
        port = '5433'
    )
    cursor = connection.cursor()
    query = """
    WITH MonthlySales AS (
        SELECT
            DATE_TRUNC('month', "InvoiceDate"::timestamp) AS Month,
            SUM("UnitPrice" * "Quantity") AS TotalSales
        FROM
            ecommerce_sales
        GROUP BY
            DATE_TRUNC('month', "InvoiceDate"::timestamp)
    )
    SELECT
        Month,
        TotalSales
    FROM
        MonthlySales
    ORDER BY
        Month ASC;
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
    sales_df = monthly_sales()
    
    # Save the DataFrame to a CSV
    sales_df.to_csv(os.environ['TABLEAU_MONTHLY_SALES'], index=False)