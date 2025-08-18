# Import the required libraries
from faker import Faker
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import time

# connect to Mongodb client
client = MongoClient("mongodb://admin:password123@localhost:27017/")

# List database names -- optional only for testing
client.list_database_names()

# Access the database
db=client.ecommerce_db

# Access the collection
collection=db.customers

# Create Indexes
collection.create_index('customer_id', unique=True)
collection.create_index('email', unique=True)
collection.create_index('last_name')

# Call faker
fake = Faker()

# Variable for storing batches
customer_batch = []

# Create a function that checks the existing customer_ids
def customer_id_check():
    last_customer = collection.find_one({},{'customer_id':1,'_id':0},sort=[('customer_id',-1)])
    # When there no data in the collection and writing the first batch of data
    if last_customer is None and len(customer_batch)==0:
        return "CUST000000001"
    elif last_customer is None and len(customer_batch)>0:
        last_batch_customer_id=customer_batch[-1]['customer_id']
        last_batch_customer_id_int=int(last_batch_customer_id[4:])
        return f"CUST{last_batch_customer_id_int+1:09d}"
    # When data already exists in the collection
    else:
        last_customer_id=last_customer['customer_id']
        last_customer_id_int= int(last_customer_id[4:])

        if last_customer_id_int<10000000 and len(customer_batch)==0:
            return f"CUST{last_customer_id_int+1:09d}"
        elif last_customer_id_int<10000000 and len(customer_batch)<10:
            last_batch_customer_id=customer_batch[-1]['customer_id']
            last_batch_customer_id_int=int(last_batch_customer_id[4:])
            return f"CUST{last_batch_customer_id_int+1:09d}"
        elif last_customer_id_int>=10000000:
            raise ValueError
            
    return last_customer_id

# Function that generates the mock data
def generate_customer_data():
    for i in range(0,1000):

        try:
            customer={
            'customer_id': customer_id_check(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'created_at':str(fake.date_time_between(start_date='-3y',end_date='now'))
        }
            customer_batch.append(customer)
        except ValueError as f:
            print('Cannot add anymore documents')
            break

        if len(customer_batch)==10:
            try:
                result=collection.insert_many(customer_batch)
                print('Inserted the next batch---------------------------------')
            except BulkWriteError as e:
                print('BulkWriteError: ',e)
            finally:
                customer_batch.clear()
        time.sleep(0.1)
    

generate_customer_data()

client.close()