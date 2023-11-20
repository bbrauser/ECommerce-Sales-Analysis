import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def top_10_products():
    connection = psycopg2.connect(
        dbname = os.environ['DBNAME'],
        user = 'postgres',
        password = os.environ['PASSWORD'],
        host = os.environ['HOST'],
        port = os.environ['PORT']
    )
    cursor = connection.cursor()
    query = """
    WITH ProductSales AS (
        SELECT
            "StockCode",
            "Description",
            SUM("Quantity") AS SalesVolume,
            SUM("UnitPrice" * "Quantity") AS Revenue
        FROM
            ecommerce_sales
        GROUP BY
            "StockCode", "Description"
    )
    SELECT
        "StockCode",
        "Description",
        SalesVolume,
        Revenue
    FROM
        ProductSales
    ORDER BY
        Revenue DESC, SalesVolume DESC
    LIMIT 10;
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
    sales_df = top_10_products()
    
    # Save the DataFrame to a CSV
    sales_df.to_csv('tableau/data_sources/top_10_products.csv', index=False)