import psycopg2
# importing os module for environment variables
import os
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

# Loading variables from .env file
load_dotenv('sample.env')
connection=None
cursor=None
try:
    connection = psycopg2.connect(user=os.getenv("user"),
                                password=os.getenv("password"),
                                host=os.getenv("host"),
                                port=os.getenv("port"),
                                database=os.getenv("database"))
    cursor = connection.cursor()
    create_script='''create table if not exists publisher(
    id int,name varchar,estd int,location varchar,type varchar)'''

    cursor.execute(create_script)
    connection.commit()


    while True:
        print("1.select 2.insert 3.update 4.delete")
        n=int(input())
        
        if(n==1):
            #postgreSQL_select_Query = "select * from publisher"
            #cursor.execute(postgreSQL_select_Query)
            print("Selecting rows from publisher table using cursor.fetchall")
            #publisher_records = cursor.fetchall()   
            print("Print each row and it's columns values")
            '''for row in publisher_records:
                print("publisher_Id = ", row[0], )
                print("publisher_name = ", row[1])
                print("publisher_estd  = ", row[2])
                print("publisher_location  = ", row[3])
                print("publisher_type  = ", row[4], "\n")
                print("Record:")
                for i in row:
                    print(i,"\n")'''
            try:
                cursor = connection.cursor(cursor_factory=DictCursor)
                postgreSQL_select_Query = "SELECT * FROM publisher"
                cursor.execute(postgreSQL_select_Query)
                publisher_records = cursor.fetchall()
                if not publisher_records:
                    raise Exception("Table doesnot conatin any records")
                print("Selecting rows from publisher table using cursor.fetchall")
                #publisher_records = cursor.fetchall()   
                print("Print each row and it's columns values")
                for record in publisher_records:
                    print(dict(record))
            except Exception as error:
                print(error)
                    
        elif(n==2):
            print("Input")
            s1 = int(input("Enter the ID: "))
            s2 = input("Enter the name: ")
            s3 = int(input("Enter the established year: "))
            s4 = input("Enter the location: ")
            s5 = input("Enter the type: ")
            postgres_insert_query = """
                INSERT INTO publisher (id, name, estd, location, type)
                VALUES (%s, %s, %s, %s, %s)
                """
                
            record_to_insert = (s1, s2, s3, s4, s5)
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()

            count = cursor.rowcount
            print(count, "Record inserted successfully into publisher table")
        elif(n==3):
            u1=int(input("enter the estd"))
            u2=int(input("enter the id "))
            sql_update_query = """Update publisher set estd = %s where id = %s"""
            cursor.execute(sql_update_query,(u1,u2))
            connection.commit()
            count = cursor.rowcount
            print(count, "Record Updated successfully ")
        elif(n==4):
            d1=int(input("enter the id to be deleted "))
            sql_delete_query = """Delete from publisher\
                where id = %s"""
            cursor.execute(sql_delete_query, (d1,))
            connection.commit()
            count = cursor.rowcount
            print(count, "Record deleted successfully ")
        else:
            print("invalid input ")
            break
except Exception as error:
    print(error)
finally:
    if connection is not None:
        connection.close()
    if cursor is not None:
        cursor.close()

    