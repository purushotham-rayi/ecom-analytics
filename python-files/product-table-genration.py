# Import the required libraries
from faker import Faker
from pymongo import MongoClient
import pandas as pd
import math
import os

# connect to Mongodb client
client = MongoClient("mongodb://admin:password123@localhost:27017/")

# Access the database
db=client.ecommerce_db
# Access the collection
collection=db.products

# Create Indexes
collection.create_index('product_id', unique=True)
collection.create_index('brand')
collection.create_index('launch_date')
collection.create_index([('category'),('subcategory')])


# Read the csv
script_path = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_path, '..', 'ecommerce_products.csv')

df = pd.read_csv(csv_path)

# Create a new df to copy the original df and modify prices
df2=df.copy()

# Make all the prices end with .99
df2['price']=df2['price'].map(lambda x: math.floor(x)+0.99)

# Shape of the Dataframe
num_of_columns=df2.shape[1]
num_of_rows=df2.shape[0]

fake = Faker()

# Generate launch date for each product using faker
launch_date=[str(fake.date_between(start_date='-3y',end_date='-1y')) for x in range(num_of_rows)]

# Insert the launch date column
df2.insert(num_of_columns,'launch_date',launch_date)

# Convert the rows in the dataframe to dictionaries for insering to mongodb.
records=df2.to_dict(orient='records')

# Insert the records to the products collection
if collection.count_documents({})>=0 and collection.count_documents({})<1000:
    for record in records:
        collection.insert_one(record)
    print('Inserted ',len(records),' records successfully!')
elif collection.count_documents({})>=1000:
    print('Cannot add anymore products')