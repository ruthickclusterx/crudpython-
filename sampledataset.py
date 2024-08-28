import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

# Loading variables from .env file
load_dotenv('sample.env')

connection = None
cursor = None
try:
    connection = psycopg2.connect(user=os.getenv("user"),
                                  password=os.getenv("password"),
                                  host=os.getenv("host"),
                                  port=os.getenv("port"),
                                  database=os.getenv("database"))
    cursor = connection.cursor()

    #cursor.execute('drop table if exists stagingtable ')
    stagingtablecreation = '''CREATE TABLE if not exists stagingtable (
        practice VARCHAR(250),
        provider VARCHAR(250),
        clinic VARCHAR(250),
        specialty VARCHAR(250),
        patient_id INT,
        parent_appt_id INT,
        appt_id INT, 
        appt_type VARCHAR(250),
        appt_type_class VARCHAR(250),
        appt_status VARCHAR(250),
        dos DATE,	 
        claim_dos DATE,
        claim_id INT, 	
        procedure_code VARCHAR(250),
        procedure_code_name TEXT,
        is_jcode boolean,
        is_no_show boolean,
        is_rescheduled boolean,
        is_rescheduled_within_24 boolean,
        is_cancelled boolean,
        is_canceled_within_48 boolean,
        rescheduled_appt_id INT,
        appt_cancel_reason VARCHAR(250),
        rescheduled_date_time VARCHAR(250), 
        appt_cancelled_date_time VARCHAR(250), 
        charge NUMERIC,
        collection NUMERIC,
        adjustment NUMERIC,
        transfer_in NUMERIC, 
        transfer_out NUMERIC,	
        voided VARCHAR(250)
    );'''
    cursor.execute(stagingtablecreation)
    connection.commit()
    
    # Copy data into staging table
    with open(r"D:\Downloads\data.csv", "r") as file:
        copystagingtable = '''COPY stagingtable(practice,provider,clinic,specialty,patient_id,parent_appt_id,appt_id,appt_type,appt_type_class,
                            appt_status,dos,claim_dos,claim_id,procedure_code,procedure_code_name,is_jcode,is_no_show,
                            is_rescheduled,is_rescheduled_within_24,is_cancelled,is_canceled_within_48,rescheduled_appt_id,
                            appt_cancel_reason,rescheduled_date_time,appt_cancelled_date_time,charge,collection,adjustment,
                            transfer_in,transfer_out,voided)
        FROM STDIN
        DELIMITER ','
        CSV HEADER;'''
        cursor.copy_expert(copystagingtable, file)
        connection.commit()

    cursor.execute('drop table if exists finaltable ')
    finaltablecreation = '''CREATE TABLE if not exists finaltable (
        practice VARCHAR(250),
        provider VARCHAR(250),
        clinic VARCHAR(250),
        specialty VARCHAR(250),
        patient_id INT,
        parent_appt_id INT,
        appt_id INT, 
        appt_type VARCHAR(250),
        appt_type_class VARCHAR(250),
        appt_status VARCHAR(250),
        dos DATE,	 
        claim_dos DATE,
        claim_id INT, 	
        procedure_code VARCHAR(250),
        procedure_code_name TEXT,
        is_jcode boolean,
        is_no_show boolean,
        is_rescheduled boolean,
        is_rescheduled_within_24 boolean,
        is_cancelled boolean,
        is_canceled_within_48 boolean,
        rescheduled_appt_id INT,
        appt_cancel_reason VARCHAR(250),
        rescheduled_date_time TIMESTAMP, 
        appt_cancelled_date_time TIMESTAMP, 
        charge NUMERIC,
        collection NUMERIC,
        adjustment NUMERIC,
        transfer_in NUMERIC, 
        transfer_out NUMERIC,	
        voided TIMESTAMP
    );'''
    cursor.execute(finaltablecreation)
    connection.commit()
    
    # Insert data into final table with 24-hour format conversion
    insertionfinaltable = '''INSERT INTO finaltable (
        practice, provider, clinic, specialty, patient_id, parent_appt_id, appt_id, appt_type, appt_type_class, appt_status,
        dos, claim_dos, claim_id, procedure_code, procedure_code_name, is_jcode, is_no_show, is_rescheduled,
        is_rescheduled_within_24, is_cancelled, is_canceled_within_48, rescheduled_appt_id, appt_cancel_reason,
        rescheduled_date_time, appt_cancelled_date_time, charge, collection, adjustment, transfer_in, transfer_out, voided)
    SELECT
        practice, provider, clinic, specialty, patient_id, parent_appt_id, appt_id, appt_type, appt_type_class, appt_status,
        dos, claim_dos, claim_id, procedure_code, procedure_code_name, is_jcode, is_no_show, is_rescheduled,
        is_rescheduled_within_24, is_cancelled, is_canceled_within_48, rescheduled_appt_id, appt_cancel_reason,
        TO_TIMESTAMP(rescheduled_date_time, 'MM/DD/YYYY HH24:MI:SS')::timestamp,
        TO_TIMESTAMP(appt_cancelled_date_time, 'MM/DD/YYYY HH24:MI:SS')::timestamp,
        charge, collection, adjustment, transfer_in, transfer_out,
        TO_TIMESTAMP(voided, 'MM/DD/YYYY HH24:MI:SS')::timestamp
    FROM stagingtable;'''

    cursor.execute(insertionfinaltable)
    connection.commit()
    
except Exception as error:
    print(error)

finally:
    if cursor is not None:
        cursor.close()
    if connection is not None:
        connection.close()
