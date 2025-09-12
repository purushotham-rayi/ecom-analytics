from pymongo import MongoClient
import datetime
import random
import time
from faker import Faker

# Create Faker instance
fake= Faker()

# Connect to mongodb
mongo_Client = MongoClient("mongodb://admin:password123@localhost:27017/")

# Access the collections
orders_collection=mongo_Client.ecommerce_db.orders
order_items_collection=mongo_Client.ecommerce_db.order_items
products_collection=mongo_Client.ecommerce_db.products

# Personal interest, not needed for order generation
order_performance_collection=mongo_Client.ecommerce_db.order_performance

# Giving weights for generating number of items in the order
items_in_ord=[1,2,3,4,5]
items_in_ord_weights=[75,15,7,2,1]
# Function to generate the number of items in the order
def get_item_count():
    return random.choices(items_in_ord,items_in_ord_weights,k=1)[0]

#Giving weights for item quantity
order_item_quantity=[1,2,3,4,5]
order_item_quantity_weights=[85,9,3,2,1]
# Function to randomly select quantity of each order item
def get_item_quantity():
    return random.choices(order_item_quantity,order_item_quantity_weights,k=1)[0]

# List of functions for various usage later in the code

# Generate order ID
def generate_order_id():
    last_order = orders_collection.find_one({},{"order_id":1,"_id":0},sort=[('order_id',-1)])
    if last_order is None:
        return 'ORD0000000001'
    else:
        last_order_id_int=int(last_order['order_id'][3:])
        next_order_id_int=last_order_id_int+1
        next_order_id=f'ORD{next_order_id_int:010d}'
    return next_order_id

# Get customer_id
def get_customer_id():
    customer_id=mongo_Client.ecommerce_db.customers.aggregate([{"$sample": {"size": 1}}]).next()['customer_id']
    return customer_id

# get product details
def get_product_details():
    product = mongo_Client.ecommerce_db.products.aggregate([{"$sample": {"size": 1}}]).next()
    return product

# For order item generation
def order_item_generation(order_id):
    last_order_item = order_items_collection.find_one({}, {"order_item_id": 1, "_id": 0}, sort=[('order_item_id', -1)])
    order_amount_total=0
    if last_order_item is None:
        order_item_counter = 1
    else:
        order_item_counter = int(last_order_item['order_item_id'][2:]) + 1
    
    unique_items_in_order = get_item_count()

    for i in range(unique_items_in_order):               # ← SINGLE LOOP ONLY
        item_quantity = get_item_quantity()
        product = get_product_details()
        unit_price = product['price']
        total_item_price= round(item_quantity * unit_price,2)
        
        current_order_item_id = f"OI{order_item_counter:011d}"
        
        order_item = {
            'order_item_id': current_order_item_id,
            'order_id': order_id,
            'product_id': product['product_id'],
            'quantity': item_quantity,                   # ← ONE RECORD PER PRODUCT
            'unit_price': unit_price,
            'total_item_price': total_item_price
        }
        order_amount_total+=round(float(order_item['total_item_price']),2)
        
        
        order_items_collection.insert_one(order_item)
        
        products_collection.update_one({'product_id':product['product_id']},{'$inc':{'current_stock':-order_item['quantity']}})
            
        order_item_counter += 1
    return round(order_amount_total,2)

# Generate order data
def generate_order_data():
    order_id = generate_order_id()
    customer_id= get_customer_id()
    total_amount = order_item_generation(order_id)
    order={
        'order_id': order_id,
        'customer_id': customer_id,
        'total_amount': total_amount,
        'order_date': str(fake.date_time_between(start_date='-3y',end_date='-1d')),
        'order_status':'delivered'
    }
    orders_collection.insert_one(order)

if __name__ == "__main__":
    #Create Indexes
    orders_collection.create_index('order_id',unique=True)
    orders_collection.create_index('customer_id')
    orders_collection.create_index('order_date')
    orders_collection.create_index([('order_date', -1), ('total_amount', -1)])
    order_items_collection.create_index('order_item_id',unique=True)
    order_items_collection.create_index('product_id')
    order_items_collection.create_index('quantity')
    order_items_collection.create_index('total_item_price')

    # For monitoring only to check the database performance over time. This is my personal interest
    print("Starting continuous order generation...")
    print("Press Ctrl+C to stop\n")
    counter=0
    sleep_time = 0.025
    start_time=datetime.datetime.now()

    try:
        while True:
            generate_order_data()
            counter+=1
            time.sleep(sleep_time)

            if counter%1000==0:
                end_time= datetime.datetime.now()
                runtime=end_time-start_time
                average_rate=round(counter/runtime.total_seconds(),2)
                log_entry={
                    'session_start_time':start_time,
                    'session_end_time': end_time,
                    'total_runtime_seconds': runtime.__str__(),
                    'orders_generated': counter,
                    'average_rate': average_rate,
                    'sleep_interval': sleep_time,
                    'log_type': '1000 orders generated',
                    'log_created_at': datetime.datetime.now()
                }
                order_performance_collection.insert_one(log_entry)

    except KeyboardInterrupt:
        end_time = datetime.datetime.now()
        runtime=end_time-start_time
        average_rate=round(counter/runtime.total_seconds(),2)
        log_entry={
            'session_start_time':start_time,
            'session_end_time': end_time,
            'total_runtime_seconds': runtime.__str__(),
            'orders_generated': counter,
            'average_rate': average_rate,
            'sleep_interval': sleep_time,
            'log_type': 'Keyboard Interrupt',
            'log_created_at': datetime.datetime.now()
        }
        order_performance_collection.insert_one(log_entry)
        print(f"Total runtime: {runtime} ")
        print(f"Average rate: {average_rate:.2f} orders/second")

