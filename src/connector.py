import datetime
import os
import pytz
import time
import snowflake.connector
from snowflake.connector import DictCursor
from flask import Blueprint, request, abort, jsonify, make_response, render_template
import logging
import Adyen
import json
from Adyen.util import is_valid_hmac_notification
from dateutil import parser
import threading

# global buffer
transaction_buffer = []
buffer_lock = threading.Lock()

# create adyen object to use adyen functions
adyen = Adyen.Adyen()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Make the Snowflake connection
def connect() -> snowflake.connector.SnowflakeConnection:
    if os.path.isfile("/snowflake/session/token"):
        creds = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': "https",
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': "oauth",
            'token': open('/snowflake/session/token', 'r').read(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    else:
        creds = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    return snowflake.connector.connect(**creds)

conn = connect()

# Make the API endpoints
connector = Blueprint('connector', __name__)

@connector.route('/email_history', methods=["GET"])
def email_history():
    parentId = request.args.get('ParentId')
    relatedToId = request.args.get('RelatedToId')

    # If neither ParentId nor RelatedToId is provided, return early
    if not parentId and not relatedToId:
        return jsonify({"message": "Both ParentId and RelatedToId are empty"}), 200

    where_clause = []
    if parentId:
        where_clause.append(f"record_data:ParentId = '{parentId}'")
    if relatedToId:
        where_clause.append(f"record_data:RelatedToId = '{relatedToId}'")

    # Combine conditions with AND
    where_clause_str = " AND ".join(where_clause)

    # SQL query with dynamic WHERE clause
    query = f"""
    SELECT record_data 
    FROM SALESFORCE.INBOUND_RAW."salesforce-live-three-streams_main_EmailMessage"
    WHERE {where_clause_str}
    ORDER BY record_data:CreatedDate DESC
    """

    try:
        # Attempt to connect to the database
        conn = connect()  # Assuming this is your custom connect function
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch all results
        raw_emails = cursor.fetchall()

        # Convert the raw emails into a list of dictionaries (still with the full record_data)
        emails = []
        for row in raw_emails:
            email_data_str = row[0]  # This is the raw JSON string
            email_data = json.loads(email_data_str)  # Parse it into a dictionary
            emails.append(email_data)  # Append the entire JSON object (not just specific fields)

    except Exception as e:
        # Catch any other unforeseen errors
        return jsonify({"error": "Error selecting from Snowflake", "message": str(e)}), 500
    finally:
        # Ensure the cursor and connection are properly closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # Return the full email history to the template
    return render_template('archived_email.html', emails=emails)


@connector.route("/insertOneTransaction", methods=["POST"])
def insertOneTransaction():
    # get transaction from request
    transaction = request.json

    # get adyen hmac key and validate
    key = os.getenv("ADYEN_HMAC_KEY") #setup environment variable in snowflake
    hmac_validate = is_valid_hmac_notification(transaction, key) # might need to change the eventCode into Operations

    # if validate failed, abort
    if not hmac_validate:
        abort(400, "Invalid hmac signature.")

    params = ['pspReference', 'live', 'currency', 'value', 'eventCode', 'eventDate', 
              'merchantAccountCode', 'merchantReference', 'originalReference', 
              'paymentMethodVariant', 'paymentMethod', 'reason', 'success'] # get hmacsignature for nested notifications items
    
    transaction = extract_nested_values(transaction, params)
    missing_params = [param for param in params if param not in transaction]

    if missing_params:
        logger.info(f"Missing params: {missing_params}")
        # abort(400, "Missing one or more required parameters.")

    # convert time zone to sydney time zone
    if transaction.get('eventDate'):
        incoming_time = parser.parse(transaction['eventDate'])
        # Convert to Sydney time
        sydney_tz = pytz.timezone('Australia/Sydney')
        sydney_time = incoming_time.astimezone(sydney_tz)

        # Update the transaction dictionary
        transaction['eventDate'] = sydney_time.isoformat()

    # convert value
    if transaction.get('value'):
        transaction['value'] = transaction.get('value') / 100
    
    for param in params:
        if param not in transaction:
            transaction[param] = None

    with buffer_lock:
        transaction_buffer.append(transaction)
        logger.info("Transaction received")
        # Check if buffer size has reached 100 transactions
        if len(transaction_buffer) >= 100:
            flush_buffer()
        return make_response(jsonify({"message": "Transaction received"}), 201)

def extract_nested_values(data, keys):
    """
    Recursively extract specified keys and their values from a nested dictionary.

    :param data: The dictionary to traverse.
    :param keys: A list of keys to extract.
    :return: A dictionary with the extracted key-value pairs.
    """
    extracted = {}

    if isinstance(data, dict):
        for key, value in data.items():
            if key in keys:
                extracted[key] = value
            elif isinstance(value, dict):
                extracted.update(extract_nested_values(value, keys))
            elif isinstance(value, list):
                for item in value:
                    extracted.update(extract_nested_values(item, keys))
    elif isinstance(data, list):
        for item in data:
            extracted.update(extract_nested_values(item, keys))

    return extracted

def batch_insert_transactions(transactions):
    logger.info(f"Inserting batch of {len(transactions)} into DB")
    sql_string = '''
    INSERT INTO ADYEN_API.PUBLIC.TRANSACTIONS (
        pspReference, live, currency, value, eventCode, eventDate, 
        merchantAccountCode, merchantReference, originalReference, 
        paymentMethodVariant, paymentMethod, reason, success
    ) VALUES (
        %(pspReference)s, %(live)s, %(currency)s, %(value)s, %(eventCode)s, %(eventDate)s, 
        %(merchantAccountCode)s, %(merchantReference)s, %(originalReference)s, 
        %(paymentMethodVariant)s, %(paymentMethod)s, %(reason)s, %(success)s
    );
    '''
    
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.executemany(sql_string, transactions)
            conn.commit()
        logger.info("Transactions inserted successfully")
    except Exception as e:
        logger.error(f"Error inserting into Snowflake: {str(e)}")
        abort(500, f"Error inserting into Snowflake: {str(e)}")

def flush_buffer():
    global transaction_buffer
    with buffer_lock:
        if not transaction_buffer:
            logger.info("No transactions to insert.")
            return
        transaction_to_insert = transaction_buffer.copy()
        transaction_buffer = []
    batch_insert_transactions(transaction_to_insert) 

def schedule_flush(interval_seconds):
    def flush_periodically():
        while True:
            time.sleep(interval_seconds)
            # flush_buffer()
    
    t = threading.Thread(target=flush_periodically, daemon=True)
    t.start()

schedule_flush(3600)