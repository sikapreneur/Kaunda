# import snowflake.connector 
# import pandas as pd 
# import numpy as np 
# import os 
# # Step 1: Install the snowflake connector (pip install snowflake)
# # Step 2: update pip python -m pip install -- upgrade pop

# # Account full Name: NGLQAGW.QX92246
# #Account: QX92246
# # account url: https://yt07225.uk-south.azure.snowflakecomputing.com
# # account url proper: https://nglqagw-qx92246.snowflakecomputing.com
# #Username: kaunda
# #Password: (Udiot@2020)

# # Step 1: Install Anaconda3 for Python from google
# # Step 2: Install Snowflake connector for Python, run below command from cmd prompt
# #  pip install snowflake-connector-python==3.0.0
# # Step 3: Import that connector in your python program
# # Step 4: Use snowflake.connector.connect to connect to your snowflake account.

# my_connection = snowflake.connector.connect(
#     user = 'kaunda'
#     ,password = '(Udiot@2020)'
#     ,account = 'nglqagw-qx92246'
#     ,warehouse = 'COMPUTE_WH'
#     ,database='SNOWLOADER'
#     ,schema ='csv'
# )

# # path to CSV
# csv_path = 'C:/Users/HP/Desktop/Snowloader/NSMQ_Analysis.csv'

# # Table name to import to
# target_table ='SNOWLOAD'

# # password = os.getenv('SNOWSQL_PWD')
# # print(password)

# # Read sample records into data frame
# df = pd.read_csv(csv_path, nrows=200) # remove the limit to load the entire file

# # Replace invalid or missing values - just in case.
# df.replace({np.nan: None}, inplace=True)


# # Create a cursor object
# cursor_object = my_connection.cursor()

# for _, row in df.iterrows():
#     placeholders = ', '.join(['%s']* len(row))
#     insert_qry = f"INSERT INTO {target_table} VALUES ({placeholders})"
#     cursor_object.execute(insert_qry, tuple(row))

# my_connection.commit()

# cursor_object.close()
# my_connection.close()


# import snowflake.connector
# import pandas as pd
# import numpy as np

# # Define Snowflake connection parameters
# connection_params = {
#     'user': 'kaunda',
#     'password': '(Udiot@2020)',
#     'account': 'nglqagw-qx92246',
#     'warehouse': 'COMPUTE_WH',
#     'database': 'SNOWLOADER',
#     'schema': 'csv'
# }

# # Establish the Snowflake connection
# my_connection = snowflake.connector.connect(**connection_params)

# # Path to the CSV file
# csv_path = 'C:/Users/HP/Desktop/Snowloader/NSMQ_Analysis.csv'

# # Table name to import data into
# target_table = 'SNOWLOAD'

# # Read the CSV file into a DataFrame
# df = pd.read_csv(csv_path, nrows=20)  # Remove 'nrows=200' to load the entire file

# # Replace invalid or missing values with None
# df.replace({np.nan: None}, inplace=True)

# # Create a cursor object
# cursor_object = my_connection.cursor()

# # Insert DataFrame records into Snowflake table
# for _, row in df.iterrows():
#     placeholders = ', '.join(['%s'] * len(row))
#     insert_qry = f"INSERT INTO {target_table} VALUES ({placeholders})"
#     cursor_object.execute(insert_qry, tuple(row))

# # Commit the transaction
# my_connection.commit()

# # Close the cursor and connection
# cursor_object.close()
# my_connection.close()








# import snowflake.connector
# import pandas as pd
# from snowflake.connector.pandas_tools import write_pandas
# import time
# import schedule
# csv_path = 'C:/Users/HP/Desktop/Snowloader/NSMQ_Analysis.csv'
# target_table = 'SNOWLOAD'

# df = pd.read_csv(csv_path)  # Remove nrows to load the entire file


# conn = snowflake.connector.connect(
#     user = 'kaunda'
#     ,password = '(Udiot@2020)'
#     ,account = 'nglqagw-qx92246'
#     ,warehouse = 'COMPUTE_WH'
#     ,database='SNOWLOADER'
#     ,schema ='csv'
#     )

# start_time = time.time()

# success, nchunks, nrows, _ = write_pandas(conn,df,'SNOWLOAD',quote_identifiers=False)
# end_time = time.time()
# if success:
#     elapsed_time = end_time-start_time
#     print(f"Successfully loaded {nrows} rows into Snowflake table '{target_table}' in {elapsed_time:.2f} seconds.")
# else:
#     print("Failed to load data into Snowflake table.")
# conn.close()






import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import time
import schedule
#pip install snowflake-connector-python[pandas]

def load_data_to_snowflake():
    csv_path = 'C:/Users/HP/Desktop/Snowloader/NSMQ_Analysis.csv'
    target_table = 'SNOWLOAD'

    df = pd.read_csv(csv_path)  # Remove nrows to load the entire file

    conn = snowflake.connector.connect(user='wewew',password='(wewerwe)',account='wewewe',warehouse='wewew',database='wewew', schema='wewew')

    start_time = time.time() # record when the import started

    success, nchunks, nrows, _ = write_pandas(conn, df, 'SNOWLOAD', quote_identifiers=False)
    # Make quote_identifiers True if your CSV column names has spaces or special characters in it.
    # That will allow the connector to put all your CSV column names in quote.

    end_time = time.time() # record when the import ended

    if success:
        elapsed_time = end_time - start_time
        print(f"Successfully loaded {nrows} rows into Snowflake table '{target_table}' in {elapsed_time:.2f} seconds.")
    else:
        print("Failed to load data into Snowflake table.")

    conn.close()
#Variables 
#Constant
# Schedule the job to run every morning at 8 AM
schedule.every().day.at("08:18").do(load_data_to_snowflake)

# Run the scheduled jobs
while True:
    schedule.run_pending()
    time.sleep(60)  # Wait for 60 seconds before checking again