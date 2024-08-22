import json
import boto3
import psycopg2
from psycopg2 import sql
from psycopg2 import errors as psycopg2_errors
import logging
import os
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
secretsmanager = boto3.client('secretsmanager')

# Environment variables
SECRET_NAME = os.environ['SECRET_NAME']
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
SQS_DLQ_URL = os.environ['SQS_DLQ_URL']
PROCESSED_FILES_TABLE = os.environ['PROCESSED_FILES_TABLE']
MAPPING_TABLE = os.environ['MAPPING_TABLE']

def get_secret():
    try:
        response = secretsmanager.get_secret_value(SecretId=SECRET_NAME)
        return json.loads(response['SecretString'])
    except ClientError as e:
        logger.error(f"Failed to retrieve database credentials: {str(e)}")
        raise

def get_db_connection(secret):
    try:
        conn = psycopg2.connect(
            host=secret['host'],
            port=secret['port'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password']
        )
        return conn
    except psycopg2_errors.OperationalError as e:
        logger.error(f"Failed to connect to the database: {str(e)}")
        raise

def is_file_processed(cursor, bucket, key):
    try:
        cursor.execute(
            sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE bucket = %s AND key = %s)").format(sql.Identifier(PROCESSED_FILES_TABLE)),
            (bucket, key)
        )
        return cursor.fetchone()[0]
    except psycopg2_errors.UndefinedTable:
        logger.error(f"Table {PROCESSED_FILES_TABLE} does not exist")
        raise
    except (psycopg2_errors.InFailedSqlTransaction, psycopg2_errors.SyntaxError) as e:
        logger.error(f"SQL error when checking processed files: {str(e)}")
        raise

def mark_file_as_processed(cursor, bucket, key):
    try:
        cursor.execute(
            sql.SQL("INSERT INTO {} (bucket, key, processed_at) VALUES (%s, %s, CURRENT_TIMESTAMP)").format(sql.Identifier(PROCESSED_FILES_TABLE)),
            (bucket, key)
        )
    except psycopg2_errors.UniqueViolation:
        logger.warning(f"File already marked as processed: s3://{bucket}/{key}")
    except (psycopg2_errors.InFailedSqlTransaction, psycopg2_errors.SyntaxError) as e:
        logger.error(f"SQL error when marking file as processed: {str(e)}")
        raise

def get_target_table(cursor, key):
    try:
        cursor.execute(
            sql.SQL("SELECT table_name FROM {} WHERE %s LIKE prefix || '%%' ORDER BY LENGTH(prefix) DESC LIMIT 1").format(sql.Identifier(MAPPING_TABLE)),
            (key,)
        )
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError(f"No matching table found for key: {key}")
    except psycopg2_errors.UndefinedTable:
        logger.error(f"Table {MAPPING_TABLE} does not exist")
        raise
    except (psycopg2_errors.InFailedSqlTransaction, psycopg2_errors.SyntaxError) as e:
        logger.error(f"SQL error when getting target table: {str(e)}")
        raise

def process_file(bucket, key, conn, cursor):
    logger.info(f"Processing file: s3://{bucket}/{key}")
    
    if is_file_processed(cursor, bucket, key):
        logger.info(f"File already processed: s3://{bucket}/{key}")
        return False

    target_table = get_target_table(cursor, key)
    logger.info(f"Target table for s3://{bucket}/{key}: {target_table}")

    try:
        s3_object = s3.get_object(Bucket=bucket, Key=key)
        cursor.copy_expert(
            sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier(target_table)),
            s3_object['Body'].iter_lines()
        )
        mark_file_as_processed(cursor, bucket, key)
        conn.commit()
        logger.info(f"Successfully processed file: s3://{bucket}/{key}")
        return True
    except psycopg2_errors.UniqueViolation:
        conn.rollback()
        logger.warning(f"Duplicate key violation when processing file: s3://{bucket}/{key}")
        return False
    except psycopg2_errors.InFailedSqlTransaction as e:
        conn.rollback()
        logger.error(f"Transaction failed when processing file s3://{bucket}/{key}: {str(e)}")
        raise
    except psycopg2_errors.SyntaxError as e:
        conn.rollback()
        logger.error(f"SQL syntax error when processing file s3://{bucket}/{key}: {str(e)}")
        raise
    except ClientError as e:
        conn.rollback()
        logger.error(f"AWS client error when processing file s3://{bucket}/{key}: {str(e)}")
        raise

def send_to_dlq(message, error):
    try:
        dlq_message = {
            'original_message': message,
            'error': str(error)
        }
        sqs.send_message(
            QueueUrl=SQS_DLQ_URL,
            MessageBody=json.dumps(dlq_message)
        )
        logger.info(f"Message sent to DLQ: {message}")
    except ClientError as e:
        logger.error(f"Failed to send message to DLQ: {str(e)}")

def lambda_handler(event, context):
    logger.info("S3 to Timescale transfer started")
    
    secret = get_secret()
    conn = get_db_connection(secret)
    cursor = conn.cursor()

    try:
        for record in event['Records']:
            sqs_message = json.loads(record['body'])
            s3_event = sqs_message['Records'][0]['s3']
            bucket = s3_event['bucket']['name']
            key = s3_event['object']['key']

            try:
                processed = process_file(bucket, key, conn, cursor)
                sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=record['receiptHandle'])
                if processed:
                    logger.info(f"File processed and SQS message deleted: s3://{bucket}/{key}")
                else:
                    logger.info(f"File already processed, SQS message deleted: s3://{bucket}/{key}")
            except Exception as e:
                logger.error(f"Error processing file s3://{bucket}/{key}: {str(e)}")
                send_to_dlq(record['body'], e)
                # Not deleting SQS message to allow for retry
    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("S3 to Timescale transfer completed")
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }
