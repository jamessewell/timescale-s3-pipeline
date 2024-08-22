import json
import boto3
import psycopg2
from psycopg2 import sql
from psycopg2 import errors as psycopg2_errors
import csv
import logging
import base64
import time
import os
import traceback
from datetime import timedelta
from botocore.exceptions import ClientError
from contextlib import closing

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
secretsmanager = boto3.client('secretsmanager')

# Environment variables
SECRET_NAME = os.environ['SECRET_NAME']
REGION_NAME = os.environ['AWS_REGION']  # This is automatically set by Lambda
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
PROCESSED_FILES_TABLE = os.environ['PROCESSED_FILES_TABLE']

def get_secret():
    logger.info(f"Attempting to retrieve secret: {SECRET_NAME}")
    try:
        get_secret_value_response = secretsmanager.get_secret_value(
            SecretId=SECRET_NAME
        )
        logger.info("Successfully retrieved secret")
        return json.loads(get_secret_value_response['SecretString'])
    except ClientError as e:
        logger.error(f"Error retrieving secret: {str(e)}\nTraceback: {traceback.format_exc()}")
        raise

class StreamingBuffer(object):
    def __init__(self, stream):
        self.stream = stream
        self.buffer = b''
    
    def read(self, size):
        while len(self.buffer) < size:
            chunk = self.stream.read(size)
            if not chunk:
                ret, self.buffer = self.buffer, b''
                return ret
            self.buffer += chunk
        ret, self.buffer = self.buffer[:size], self.buffer[size:]
        return ret

def create_table_if_not_exists(cursor, table_name):
    logger.info(f"Checking if table {table_name} exists")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            bucket TEXT NOT NULL,
            key TEXT NOT NULL,
            target_table TEXT NOT NULL,
            processed_at TIMESTAMP NOT NULL,
            processing_time INTERVAL NOT NULL,
            rows_copied INTEGER NOT NULL,
            s3_size_bytes BIGINT NOT NULL,
            UNIQUE (bucket, key)
        )
    """)
    logger.info(f"Table {table_name} created or already exists")

def stream_copy_to_postgres(cursor, s3_object, table_name):
    logger.info(f"Starting COPY operation to table {table_name}")
    
    streaming_buffer = StreamingBuffer(s3_object['Body'])
    
    start_time = time.time()
    try:
        cursor.copy_expert(
            sql.SQL("COPY {} FROM STDIN WITH CSV HEADER DELIMITER as ','").format(sql.Identifier(table_name)),
            streaming_buffer
        )
        end_time = time.time()
        processing_time = timedelta(seconds=end_time - start_time)
        
        # Get the number of rows copied from the cursor
        rows_copied = cursor.rowcount
        
        logger.info(f"COPY operation to table {table_name} completed successfully in {processing_time}. Rows copied: {rows_copied}")
        return processing_time, rows_copied
    except psycopg2_errors.UndefinedTable:
        logger.error(f"Table {table_name} does not exist")
        raise
    except psycopg2_errors.InsufficientPrivilege:
        logger.error(f"Insufficient privileges for COPY operation on table {table_name}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Error during COPY operation: {str(e)}\nTraceback: {traceback.format_exc()}")
        raise

def is_file_processed(cursor, bucket, key):
    logger.info(f"Checking if file {key} from bucket {bucket} has been processed")
    try:
        cursor.execute(
            sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE bucket = %s AND key = %s)").format(sql.Identifier(PROCESSED_FILES_TABLE)),
            (bucket, key)
        )
        result = cursor.fetchone()[0]
        logger.info(f"File processed check result: {result}")
        return result
    except psycopg2_errors.UndefinedTable:
        logger.error(f"Table {PROCESSED_FILES_TABLE} does not exist")
        raise
    except psycopg2.Error as e:
        logger.error(f"Error checking processed files: {str(e)}\nTraceback: {traceback.format_exc()}")
        raise

def mark_file_as_processed(cursor, bucket, key, processing_time, rows_copied, file_size, target_table):
    logger.info(f"Marking file {key} from bucket {bucket} as processed")
    try:
        cursor.execute(
            sql.SQL("INSERT INTO {} (bucket, key, processed_at, processing_time, rows_copied, file_size_bytes, target_table) VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s)").format(sql.Identifier(PROCESSED_FILES_TABLE)),
            (bucket, key, processing_time, rows_copied, file_size, target_table)
        )
        logger.info(f"File marked as processed successfully. Processing time: {processing_time}, Rows copied: {rows_copied}, File size: {file_size} bytes, Target table: {target_table}")
    except psycopg2_errors.UniqueViolation:
        logger.warning(f"File {key} from bucket {bucket} already marked as processed")
    except psycopg2.Error as e:
        logger.error(f"Error marking file as processed: {str(e)}\nTraceback: {traceback.format_exc()}")
        raise

def get_target_table(key):
    parts = key.split('/')
    if len(parts) < 2:
        raise ValueError(f"Invalid S3 key format: {key}")
    return parts[0]

def process_file(bucket, key):
    logger.info(f"Starting to process file {key} from bucket {bucket}")
    secret = get_secret()
    
    try:
        with closing(psycopg2.connect(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password'],
            port=secret['port'],
            connect_timeout=10
        )) as conn:
            logger.info("Successfully connected to the database")
            
            with conn.cursor() as cursor:
                conn.autocommit = False
                logger.info("Started database transaction")
                
                create_table_if_not_exists(cursor, PROCESSED_FILES_TABLE)
                
                target_table = get_target_table(key)
                logger.info(f"Determined target table: {target_table}")
                
                if is_file_processed(cursor, bucket, key):
                    logger.info(f"File {key} from bucket {bucket} has already been processed. Skipping.")
                    conn.commit()
                    return False
                
                logger.info(f"Retrieving object {key} from bucket {bucket}")
                s3_object = s3.get_object(Bucket=bucket, Key=key)
                file_size = s3_object['ContentLength']
                logger.info(f"Successfully retrieved S3 object. File size: {file_size} bytes")
                
                # Stream copy to PostgreSQL and get processing time and rows copied
                processing_time, rows_copied = stream_copy_to_postgres(cursor, s3_object, target_table)
                
                mark_file_as_processed(cursor, bucket, key, processing_time, rows_copied, file_size, target_table)
                
                conn.commit()
                logger.info(f"Successfully copied data from {key} to table {target_table}")
                return True
                
    except Exception as e:
        logger.error(f"Error processing file {key} from bucket {bucket}: {str(e)}\nTraceback: {traceback.format_exc()}")
        raise

def validate_sqs_event(event):
    if 'Records' not in event:
        raise ValueError("Invalid event structure: 'Records' not found")
    for record in event['Records']:
        if 'body' not in record:
            raise ValueError("Invalid record structure: 'body' not found")
        try:
            body = json.loads(record['body'])
            if 'Records' not in body or not body['Records']:
                raise ValueError("Invalid S3 event structure in SQS message body")
            s3_record = body['Records'][0]
            if 's3' not in s3_record or 'bucket' not in s3_record['s3'] or 'object' not in s3_record['s3']:
                raise ValueError("Invalid S3 event structure in SQS message body")
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON in SQS message body")

def lambda_handler(event, context):
    logger.info("Lambda function started")
    logger.info(f"Event: {json.dumps(event)}")
    
    try:
        validate_sqs_event(event)
    except ValueError as e:
        logger.error(f"Event validation failed: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid event structure')
        }
    
    for record in event['Records']:
        try:
            sqs_message = json.loads(record['body'])
            logger.info(f"Processing SQS message: {json.dumps(sqs_message)}")
            
            s3_record = sqs_message['Records'][0]['s3']
            bucket = s3_record['bucket']['name']
            key = s3_record['object']['key']
            logger.info(f"Extracted bucket: {bucket}, key: {key}")
            
            processed = process_file(bucket, key)
            
            if processed:
                logger.info(f"Deleting message from SQS queue: {SQS_QUEUE_URL}")
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=record['receiptHandle']
                )
                logger.info("SQS message deleted successfully")
                logger.info(f"Successfully processed and deleted message for file {key}")
            else:
                logger.info(f"Skipped already processed file {key}")
            
        except (psycopg2_errors.UndefinedTable, psycopg2_errors.InsufficientPrivilege) as e:
            # These are errors we don't want to retry
            logger.error(f"Non-retryable database error. Error: {str(e)}")
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=record['receiptHandle']
            )
        except Exception as e:
            logger.error(f"Failed to process message. Error: {str(e)}\nTraceback: {traceback.format_exc()}")
            # Not deleting the message from SQS, allowing SQS to handle the retry
    
    logger.info("Lambda function completed")
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }
