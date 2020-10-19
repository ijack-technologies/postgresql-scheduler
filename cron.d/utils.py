
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import platform
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from twilio.rest import Client
import boto3
import json

LOG_LEVEL = logging.INFO
# logger = logging.getLogger(__name__)


class Config():
    """Main config class"""
    TEST_FUNC = False
    TEST_ERROR = False
    DEV_TEST_PRD = 'production'
    PHONE_LIST_DEV = ['+14036897250']
    EMAIL_LIST_DEV = ['smccarthy@myijack.com']
    # For returning values in the "c" config object
    TEST_DICT = {}


def configure_logging(name, logfile_name, path_to_log_directory='/var/log/'):
    """Configure logger"""
    global LOG_LEVEL

    logger = logging.getLogger(name)
    # Override the default logging.WARNING level so all messages can get through to the handlers
    logger.setLevel(logging.DEBUG) 
    formatter = logging.Formatter('%(asctime)s : %(module)s : %(lineno)d : %(levelname)s : %(funcName)s : %(message)s')

    date_for_log_filename = datetime.now().strftime('%Y-%m-%d')
    # log_filename = f"{date_for_log_filename}_{logfile_name}.log"
    log_filename = f"{logfile_name}.log"
    log_filepath = os.path.join(path_to_log_directory, log_filename)

    if platform.system() == 'Linux':
        # fh = logging.FileHandler(filename=log_filepath)
        fh = TimedRotatingFileHandler(filename=log_filepath, 
            when='H', interval=1, backupCount=48, encoding=None, delay=False, utc=False, atTime=None)
        fh.setLevel(LOG_LEVEL)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # sh = logging.StreamHandler(sys.stdout)
    sh = logging.StreamHandler()
    sh.setLevel(LOG_LEVEL)
    sh.setFormatter(formatter)
    # print(f"logger.handlers before adding streamHandler: {logger.handlers}")
    logger.addHandler(sh) 
    # print(f"logger.handlers after adding streamHandler: {logger.handlers}")

    # Test logger
    sh.setLevel(logging.DEBUG)
    logger.debug(f"Testing the logger: platform.system() = {platform.system()}")
    sh.setLevel(LOG_LEVEL)
    
    return logger


def get_conn(c, sql, db='ijack'):
    """
    """

    if db == 'ijack':
        host = os.getenv("HOST_IJ") 
        port = int(os.getenv("PORT_IJ"))
        dbname = os.getenv('DB_IJ')
        user = os.getenv("USER_IJ")
        password = os.getenv("PASS_IJ")
    elif db == 'timescale':
        host = os.getenv("HOST_TS") 
        port = int(os.getenv("PORT_TS"))
        dbname = os.getenv('DB_TS')
        user = os.getenv("USER_TS")
        password = os.getenv("PASS_TS")

    conn = psycopg2.connect(
        host=host, 
        port=port, 
        dbname=dbname, 
        user=user, 
        password=password, 
        connect_timeout=5, 
        # cursor_factory=psycopg2.extras.DictCursor
    ) 

    return conn



def run_query(c, sql, db='ijack', fetchall=False, commit=False):
    """Run and time the SQL query"""
    conn = get_conn(c, sql, db)
    columns = None
    rows = None

    # with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        c.logger.info(f"Running query now... SQL to run: \n{sql}")
        time_start = time.time()
        cursor.execute(sql)
        if commit:
            conn.commit()
        if fetchall:
            columns = [str.lower(x[0]) for x in cursor.description]
            rows = cursor.fetchall()

    time_finish = time.time()
    c.logger.info(f"Time to execute query: {round(time_finish - time_start)} seconds")

    conn.close()
    del conn

    return columns, rows


def error_wrapper(c, func, *args, **kwargs):
    """So the loop can continue even if a function fails"""

    try:
        func(*args, **kwargs)
    except Exception:
        c.logger.exception(f"Problem running function: {func}")
        # Keep going regardless
        pass 

    return None


def send_twilio_sms(c, sms_phone_list, body):
    """Send SMS messages with Twilio from +13067003245"""
    message = ''
    if c.TEST_FUNC:
        return message

    # The Twilio character limit for SMS is 1,600
    twilio_character_limit_sms = 1600
    if len(body) > twilio_character_limit_sms:
        body = body[:(twilio_character_limit_sms - 3)] + '...'
        
    twilio_client = Client(os.environ['TWILIO_ACCOUNT_SID'], os.environ['TWILIO_AUTH_TOKEN'])
    for phone_num in sms_phone_list:
        message = twilio_client.messages.create(
            to=phone_num, 
            from_="+13067003245",
            body=body
        )
        c.logger.info(f"SMS sent to {phone_num}")

    return message


def send_twilio_phone(c, phone_list, body):
    """Send phone call with Twilio from +13067003245"""
    call = ''
    if c.TEST_FUNC:
        return call

    twilio_client = Client(os.environ['TWILIO_ACCOUNT_SID'], os.environ['TWILIO_AUTH_TOKEN'])
    for phone_num in phone_list:
        call = twilio_client.calls.create(
            to=phone_num, 
            from_="+13067003245",
            twiml=f"<Response><Say>Hello. The {body}</Say></Response>"
            # url=twiml_instructions_url
        )
        c.logger.info(f"Phone call sent to {phone_num}")

    return call


def send_mailgun_email(c, text='', html='', emailees_list=None, subject='IJACK Alert', images=None):
    """Send email using Mailgun"""
    # Initialize the return code
    rc = ''
    if c.TEST_FUNC:
        return rc
    
    if emailees_list is None:
        emailees_list = ['smccarthy@myijack.com']

    # If html is included, use that. Otherwise use text
    if html == '':
        key = 'text'
        value = text
    else:
        key = 'html'
        value = html
    
    # Add inline attachments, if any
    images2 = images
    if images is not None:
        images2 = []
        for item in images:
            images2.append(('inline', open(item, 'rb')))

    # if c.DEV_TEST_PRD in ['testing', 'production']:
    # c.logger.debug(f"c.DEV_TEST_PRD: {c.DEV_TEST_PRD}")
    if len(emailees_list) > 0:
        rc = requests.post(
            "https://api.mailgun.net/v3/myijack.com/messages",
            auth=("api", os.environ['MAILGUN_API_KEY']),
            files=images2,
            data={
                "h:sender": "alerts@myijack.com",
                "from": "alerts@myijack.com",
                "to": emailees_list,
                "subject": subject,
                key: value
            }
        )
        c.logger.info(f"Email sent to emailees_list: '{str(emailees_list)}' \nSubject: {subject} \nrc.status_code: {rc.status_code}")
        assert rc.status_code == 200
    
    return rc


def get_client_iot():
    """"""
    client_iot = boto3.client(
        "iot-data",
        region_name="us-west-2",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
    )
    # Change the botocore logger from logging.DEBUG to INFO,
    # since DEBUG produces too many messages
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)

    return client_iot


def get_iot_device_shadow(c, client_iot, aws_thing):
    """This function gets the current thing state"""
    
    response_payload = {}
    try:
        response = client_iot.get_thing_shadow(thingName=aws_thing)
        streamingBody = response["payload"]
        response_payload = json.loads(streamingBody.read())
    except Exception:
        c.logger.exception(f"ERROR! Probably no shadow exists...")

    return response_payload
    