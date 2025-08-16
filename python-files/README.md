# Mock Data Generation

## Mock Customer Data Generation using ```customer-table-generation.py```
Creates mock customer data using faker library in python and writes in batches of 10 documents to MongoDB. The key features are:
- The Customer IDs will follow this pattern and starts with ```CUST000000001```.
- Indexes are created on ```customer_id```, ```email```, ```phone_number```.
- Limits are set so that a maximum of 10 Million documents can be stored in this collection.

### Program Flow
- Install the required dependencies.
- Verify the Mongodb connection and access the database and collection.
- Create Indexes on the required columns.
- The function ```customer_id_check()``` will check the last record in the customer collection.
    - If there are no records then it follows a specific conditional statement path so that it can generate new records based on the last ```customer_id``` written to the collection.
    - If there are existing records it follows another conditional statement path so that it can generate new records based on the last ```customer_id``` written to the collection while limiting the maximum number of records to 10 Million.
- The function ```generate_customer_data()``` generates the mock data using python's ```faker``` library while also handling various exceptions that we might encounter.
    - Only 1000 records are generated at a time to save time. Other than this, there is no specific use for limiting the generation to 1000 records at a time.
    - The first ```try and except``` block makes sure to break the function if the maximum number of customers exceed 10 million records by addressing the ```ValueError```.
    - The second ```try and except``` block addresses the ```BulkWriteError```. This error occured because, faker generated duplicate emails and phone numbers. While inserting the batch with duplicates ```BulkWriteError``` error was raised.
- Call the ```generate_customer_data()``` function.
- Once the data is generated close the connection.