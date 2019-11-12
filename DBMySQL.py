from pymongo import MongoClient
import cryptography
import time
import pymysql.cursors
import json
import boto3
import decimal
import numpy
import sys
import random

current_milli_time = lambda: int(round(time.time() * 1000))

connection = None

def connect_to_mysql():
    global connection 
    try:
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='Sailaja@3012',
                                     db='filght_dbms',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
    except Exception as e:
        print(str(e))

        
def close_mysql_connection():
    connection.close()

def mysql_write_throughput(num_samples):
    with connection.cursor() as cursor:
        sql = "INSERT INTO FARE VALUES (%s,17571.20,69813.74,'CLP',74.29,265.41)"

        FLIGHT_TRIP_ID = 39603
        before = current_milli_time()

        for i in range(1,num_samples):
            FLIGHT_TRIP_ID += 1
            cursor.execute(sql, (FLIGHT_TRIP_ID))
            # connection.commit()
        connection.commit()
        
        after = current_milli_time()
        throughput = num_samples * 1000 / (after - before)
        latency = (after - before)/num_samples
        print("Throughput: " + str(throughput))
        print("Latency: " + str(latency))
        
        return throughput
        


def mysql_read_throughput(num_samples):
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
                sql = "SELECT COUNT(*) FROM AIRPORT"
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

def mysql_read_write_throughput(num_samples):
    try:
        before = current_milli_time()
        with connection.cursor() as cursor:

            for i in range(0, num_samples):

                index = random.randint(0,1)

                if index == 0:
                    sql = "SELECT * FROM AIRPORT limit " + str(num_samples);
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

    connect_to_mysql()

    print("---------MYSQL--------")
    print("---- WRITE THROUGHPUT------")
    mysql_write_throughput(2000)

    print("---READ THROUGHPUT---")
    mysql_read_throughput(2000)

    print("-----RANDOM READ/WRITE THROGHPUT----")
    mysql_read_write_throughput(2000)

    close_mysql_connection()
    
