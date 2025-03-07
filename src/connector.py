import datetime
import os

import snowflake.connector
from snowflake.connector import DictCursor
from flask import Blueprint, request, abort, jsonify, make_response
import logging
import Adyen
from Adyen.util import is_valid_hmac_notification

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

## Top 10 customers in date range
dateformat = '%Y-%m-%d'

@connector.route('/customers/top10')
def customers_top10():
    # Validate arguments
    sdt_str = request.args.get('start_range') or '1995-01-01'
    edt_str = request.args.get('end_range') or '1995-03-31'
    try:
        sdt = datetime.datetime.strptime(sdt_str, dateformat)
        edt = datetime.datetime.strptime(edt_str, dateformat)
    except:
        abort(400, "Invalid start and/or end dates.")
    sql_string = '''
        SELECT
            o_custkey
          , SUM(o_totalprice) AS sum_totalprice
        FROM snowflake_sample_data.tpch_sf10.orders
        WHERE o_orderdate >= '{sdt}'
          AND o_orderdate <= '{edt}'
        GROUP BY o_custkey
        ORDER BY sum_totalprice DESC
        LIMIT 10
    '''
    sql = sql_string.format(sdt=sdt, edt=edt)
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")

## Monthly sales for a clerk in a year
@connector.route('/clerk/<clerkid>/yearly_sales/<year>')
def clerk_montly_sales(clerkid, year):
    # Validate arguments
    try: 
        year_int = int(year)
    except:
        abort(400, "Invalid year.")
    if not clerkid.isdigit():
        abort(400, "Clerk ID can only contain numbers.")
    clerkid_str = f"Clerk#{clerkid}"
    sql_string = '''
        SELECT
            o_clerk
          ,  Month(o_orderdate) AS month
          , SUM(o_totalprice) AS sum_totalprice
        FROM snowflake_sample_data.tpch_sf10.orders
        WHERE Year(o_orderdate) = {year}
          AND o_clerk = '{clerkid}'
        GROUP BY o_clerk, month
        ORDER BY o_clerk, month
    '''
    sql = sql_string.format(year=year_int, clerkid=clerkid_str)
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")

@connector.route("/getLatestTransaction",methods=["GET"])
def getLatestTransactions():
    # if request.header.X-API-KEY != os.getenv("ADYEN_API_KEY"): #TODO check api key in request matches our api key.
            #abort!

    sql_string = '''
    SELECT * FROM ADYEN_API.PUBLIC.TRANSACTIONS
    ORDER BY eventDate DESC
    LIMIT 1;
    '''
    try:
        logger.info("Executing SQL query to get the latest transaction.")
        res = conn.cursor(DictCursor).execute(sql_string)
        result = res.fetchall()
        logger.info("Query executed successfully. Returning result.")
        return make_response(jsonify(result))
    except Exception as e:
        logger.error(f"Error reading from Snowflake: {str(e)}", exc_info=True)
        abort(500, "Error reading from Snowflake. Check the logs for details.")

@connector.route("/insertOneTransaction", methods=["POST"])
def insertOneTransaction():
    # get transaction from request
    transaction = request.json

    # get adyen hmac key and validate
    key = os.getenv("ADYEN_HMAC_KEY") #setup environment variable in snowflake
    logger.info(f"adyen_hmac_key is : {key}")
    hmac_validate = is_valid_hmac_notification(transaction, key) # might need to change the eventCode into Operations

    # if validate failed, abort
    if not hmac_validate:
        # logger.info(f"invalid hmac signature: " + str(expected_hmac))
        # logger.info(f"invalid hmac signature: ").

        abort(400, "Invalid hmac signature.")

    # select only necessary data
    params = ['pspReference', 'live', 'currency', 'value', 'eventCode', 'eventDate', 
              'merchantAccountCode', 'merchantReference', 'originalReference', 
              'paymentMethod', 'reason', 'success'] # get hmacsignature for nested notifications items
    
    data = {param: transaction.get(param) for param in params} # get param from json body 
    
    if not all(data.values()):
        logger.info(f"missing params: " + str(data))
        abort(400, "Missing one or more required parameters.")
    
    # insert into Snowflake
    sql_string = '''
    INSERT INTO ADYEN_API.PUBLIC.TRANSACTIONS (
        pspReference, live, currency, value, eventCode, eventDate, 
        merchantAccountCode, merchantReference, originalReference, 
        paymentMethod, reason, success
    ) VALUES (
        %(pspReference)s, %(live)s, %(currency)s, %(value)s, %(eventCode)s, %(eventDate)s, 
        %(merchantAccountCode)s, %(merchantReference)s, %(originalReference)s, 
        %(paymentMethod)s, %(reason)s, %(success)s
    );
    '''

    # check if missing data
    try:
        conn.cursor(DictCursor).execute(sql_string, data)
        conn.commit()
        return make_response(jsonify({"message": "Transaction inserted successfully"}), 201)
    except Exception as e:
        abort(500, f"Error inserting into Snowflake: {str(e)}")