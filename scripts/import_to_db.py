import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

DATABASE_URL = os.environ['DATABASE_URL']
engine = create_engine(DATABASE_URL)

df = pd.read_csv('data/processed/cleaned_data.csv')

df.to_sql('ecommerce_sales', engine, if_exists='replace', index=False)