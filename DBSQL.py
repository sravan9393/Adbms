from pymongo import MongoClient
import cryptography
import time
import json
import boto3
import decimal
import numpy
import sys
import random
import pyodbc

current_milli_time = lambda: int(round(time.time() * 1000))

connection = None

def connect_to_sql():
    global connection 
    try:
        connection = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                    'Server=DESKTOP-49H8FDT;'
                                    'Database=flight_dbms;'
                                    'Trusted_Connection=yes;')
    except Exception as e:
        print(str(e))

        
def close_sql_connection():
    connection.close()

def sql_write_throughput(num_samples):
    with connection.cursor() as cursor:
        sql = "INSERT INTO FARE VALUES (?,17571.20,69813.74,'CLP',74.29,265.41)"
            
        before = current_milli_time()

        FLIGHT_TRIP_ID = 100000
        for i in range(1,num_samples):
            FLIGHT_TRIP_ID += 1
            cursor.execute(sql, FLIGHT_TRIP_ID)
            # connection.commit()
        connection.commit()
        
        after = current_milli_time()
        throughput = num_samples * 1000 / (after - before)
        latency = (after - before)/num_samples

        print("Throughput: " + str(throughput))
        print("Latency: " + str(latency))
        return throughput
        


def sql_read_throughput(num_samples):
    try:
        with connection.cursor() as cursor:
            sql = '''SELECT * FROM AIRLINE_CMPNY;
                     SELECT * FROM  AIRPORT;
                     SELECT * FROM FARE;
                     SELECT * FROM FLI_PORT_ACCESS;
                     SELECT * FROM FLIGHT;
                     SELECT * FROM FLIGHT_TRIP;
                     SELECT * FROM HOP;
                     SELECT * FROM HOP_FLIGHT_TRIP;
                     SELECT * FROM SEAT;
                     SELECT * FROM TRAVELLER;
                     SELECT * FROM TRAVELLER_ITINERARY;
                     SELECT * FROM USERS;
                  '''
            
            before = current_milli_time()
            for i in range(1, num_samples):
                sql = "SELECT TOP " + str(num_samples) + " * FROM AIRPORT"
                cursor.execute(sql)
                while True:
                    result = cursor.fetchone()
                    if result:
                        # print(result)
                        pass
                    else:
                        break
            after = current_milli_time()

            throughput = num_samples * 1000 / (after - before)
            latency = (after - before)/num_samples

            print("Throughput: " + str(throughput))
            print("Latency: " + str(latency))
            return throughput
    except Exception as e:
        print(str(e))

def sql_read_write_throughput(num_samples):
    try:
        before = current_milli_time()
        with connection.cursor() as cursor:

            for i in range(0, num_samples):

                index = random.randint(0,1)

                if index == 0:
                    sql = "SELECT TOP " + str(num_samples) + " * FROM AIRPORT"
                    cursor.execute(sql)
                    while True:
                        result = cursor.fetchone()
                        if result:
                            # print(result)
                            pass
                        else:
                            break
                else:
                    sql = "INSERT INTO FARE VALUES (100000,17571.20,69813.74,'CLP',74.29,265.41)"
                    cursor.execute(sql)
                    # connection.commit()
                    connection.commit()
                    
            after = current_milli_time()
            throughput = num_samples * 1000 / (after - before)
            latency = (after - before)/num_samples

            print("Throughput: " + str(throughput))
            print("Latency: " + str(latency))
            return throughput
        
    except Exception as e:
        print(str(e))
    

if __name__ == "__main__":

    connect_to_sql()

    print("------ SQL SERVER -----------")
    print("---- WRITE THROUGHPUT------")
    sql_write_throughput(2000)

    print("---READ THROUGHPUT---")
    sql_read_throughput(2000)

    print("-----RANDOM READ/WRITE THROGHPUT----")
    sql_read_write_throughput(2000)

    close_sql_connection()
    
