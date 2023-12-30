import json
import multiprocessing
import os
import random
import re
import time

import mysql.connector
from mysql.connector import Error
from datetime import datetime
from mysql.connector import pooling

db_config = {
    "host": "10.0.0.81",
    "user": "crs",
    "password": "Omnibis.1234",
    "database": "claro",
}

# Create a connection pool
pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **db_config)


def execute_query(query, data=None):
    """
    Execute a query using a connection from the pool.
    """
    try:
        # Get a connection from the pool
        connection = pool.get_connection()
        
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
                
            # Fetch results or perform other operations
            result = cursor.fetchall()
            
            # Commit the transaction
            connection.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Release the connection back to the pool
        if connection.is_connected():
            cursor.close()
            connection.close()
# Using readlines()

US_data_path = 'E:\\Python\\Claro POC\\newJson1.json'
with open(US_data_path, 'r') as json_file:
    # Load the JSON data into a dictionary
    US_data = json.load(json_file)


def insert_to_table(data, short_code,user_identifier,unique_id):   #03
    id_counter = 1
    source = ''
    prev_source = ''
    prev_us_id = ''
    record_list = []
    array_list = []
    rep =0
    id = 0
    arra=[]
    asdasf=[]
   
  

    for items in data:
        key = items[1] + '_' + items[5]
        if (key in US_data) and prev_source == '':
            source = items[1]
            
            prev_source = items[5]
            
            prev_us_id = US_data[key]["us_id"]
            
    
            destination_US = US_data.get(key, {}).get("us_id", source)
            array_list.append(destination_US)
            source_flow = f'{short_code}'
            destination_flow = f'{destination_US}_{id_counter}'
         
            
                



            timestamp = datetime.strptime(items[0], '%Y%m%d%H%M%S')
            formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:00:00')
            insert_single_record(formatted_timestamp,user_identifier,unique_id, source_flow, destination_flow, short_code, source,id)
            id_counter += 1
            # id+=1


        if (key in US_data) and prev_us_id != US_data[key]["us_id"]:
      
       
            
            destination = items[1]
            timestamp = datetime.strptime(items[0], '%Y%m%d%H%M%S')
            formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:00:00')

            source_key = source + '_' + prev_source
            # Use get() with a default value to retrieve the value for source_US
            source_US = US_data.get(source_key, {}).get("us_id", source)

            # Use get() with a default value to retrieve the value for destination_US
            destination_US = US_data.get(key, {}).get("us_id", source)


            if destination_US in array_list:
                rep = 1
            array_list.append(destination_US)

            source_flow = f'{source_US}_{id_counter-1}'
            destination_flow = f'{destination_US}_{id_counter }'
            record_list.append((formatted_timestamp,user_identifier,unique_id, source_flow, destination_flow, 1,source, destination,id,rep))
            rep=0
            id_counter += 1
            source = destination
            prev_source = items[5]
            prev_us_id = US_data[key]["us_id"]
            id+=1
            


    if record_list:
        insert_records(record_list)


def insert_records(record_list):  #04   

    sql_values = ', '.join(map(lambda x: f"({', '.join(map(repr, x))})", record_list))
    sql_query = f"""
                        INSERT INTO sankey_bycdr (time_hour,user_identifier, unique_id,src, dest, count, start_flow, end_flow,us,repetitive)
                        VALUES {sql_values}
                        ON DUPLICATE KEY UPDATE count = count + 1
                        """
    execute_query(sql_query)

    # connection = None
    # try:
    #     connection = mysql.connector.connect(
    #         host='10.0.0.81',
    #         database='claro',
    #         user='crs',
    #         password='Omnibis.1234'
    #     )

    #     if connection.is_connected():
    #         cursor = connection.cursor()
    #         sql_values = ', '.join(map(lambda x: f"({', '.join(map(repr, x))})", record_list))
    #         sql_query = f"""
    #                     INSERT INTO sankey_bycdr (time_hour,user_identifier, unique_id,src, dest, count, start_flow, end_flow,us,repetitive)
    #                     VALUES {sql_values}
    #                     ON DUPLICATE KEY UPDATE count = count + 1
    #                     """
 
    #         execute_query(sql_query)
    #         connection.commit()
    # except Error as e:
    #     print(f"Error: {e}")
    # finally:
    #     if connection is not None and connection.is_connected():
    #         cursor.close()
    #         connection.close()


def insert_single_record(formatted_timestamp, user_identifier,unique_id,source_flow, destination_flow, source, destination,destination_US):
    sql_query = f"""
                                INSERT INTO sankey_bycdr (time_hour,user_identifier,unique_id, src, dest, count, start_flow, end_flow,us)
                                VALUES (%s,%s,%s, %s, %s, 1, %s, %s,%s)
                                ON DUPLICATE KEY UPDATE count = count + 1
                            """
    data_tuple = (formatted_timestamp,user_identifier, unique_id,source_flow, destination_flow, source, destination,destination_US)
    execute_query(sql_query, data_tuple)
    
    
    
    
    # connection = None  # Initialize connection outside the try block
    # try:
    #     connection = mysql.connector.connect(
    #         host='10.0.0.81',
    #         database='claro',
    #         user='crs',
    #         password='Omnibis.1234'
    #     )

    #     if connection.is_connected():
    #         cursor = connection.cursor()
    #         sql_query = f"""
    #                             INSERT INTO sankey_bycdr (time_hour,user_identifier,unique_id, src, dest, count, start_flow, end_flow,us)
    #                             VALUES (%s,%s,%s, %s, %s, 1, %s, %s,%s)
    #                             ON DUPLICATE KEY UPDATE count = count + 1
    #                         """
    #         data_tuple = (formatted_timestamp,user_identifier, unique_id,source_flow, destination_flow, source, destination,destination_US)
    #         execute_query(sql_query, data_tuple)
    #         connection.commit()
    # except Error as e:
    #     print(f"Error: {e}")

    # finally:
    #     if connection is not None and connection.is_connected():
    #         cursor.close()
    #         connection.close()


def process_log_file(file_path):   #02
    cdr_file = open(file_path, 'r')
    Lines = cdr_file.readlines()

    for line in Lines:
        data = json.loads(line)
        user_identifier=data.get('user_identifier')
        unique_id=data.get('unique_id')
        short_code = data.get('access_code')
        comp_hit = data.get('component_hits',
                            [])  # Use .get() to provide a default empty list if 'component_hits' key doesn't exist
        if comp_hit is not None:
            pattern = ".*(?:QUESTION|QUICK_REPLY|SCRIPT|TEXT)"
            r = re.compile(pattern)
            filtered_list = list(filter(r.match, comp_hit))
            my_array = [element.split('$') for element in filtered_list]
            if my_array:
                insert_to_table(my_array, short_code,user_identifier,unique_id)


def process_log_files_in_parallel(log_folder):  #01
    files = [os.path.join(log_folder, file) for file in os.listdir(log_folder) if file.endswith('.log')]

    # Define the number of processes to use
    num_processes = multiprocessing.cpu_count()

    # Create a pool of worker processes
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(process_log_file, files)
    return


if __name__ == "__main__":
    #log_folder = '/opt/projects/claro/poc/ransika/cdr_logs'
#    log_folder = '/opt/projects/claro/poc/ransika/onefile'
    log_folder = 'E:\\Python\\Claro POC\\city'
    start_time = time.time()
    process_log_files_in_parallel(log_folder)
    end_time = time.time()
    print("Total duration:", end_time - start_time)
