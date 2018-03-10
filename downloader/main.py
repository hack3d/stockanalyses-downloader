#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import os
import json
import logging.handlers
import time
import requests
import pika
import queue
import threading
import socket
import uuid
from pathlib import Path

# Third-party
from btfxwss import BtfxWss

# Homebrew
from plugins.bitstamp.client import Public
from plugins.bitfinex.client import Bitfinex
from plugins.IsinToCurrencyPair.currency import Currency


# config
dir_path = os.path.dirname(os.path.realpath(__file__))

# Check if config file exists
if Path(dir_path + '/config').is_file():
    config = configparser.ConfigParser()
    config.read(dir_path + '/config')

    # set all variables by config file.
    storage_logs = config['path']['storage_logs']
    logs_filename = config['path']['logs_filename']
    logs_max_size = config['path']['logs_max_size']
    logs_rotated_files = config['path']['logs_rotated_files']
    prod_log_level = config['prod']['log_level']
    prod_url = config['prod']['url']
    prod_url_username = config['prod']['username']
    prod_url_password = config['prod']['password']
    prod_rabbitmq_host = config['prod']['rabbitmq_host']
    prod_rabbitmq_username = config['prod']['rabbitmq_username']
    prod_rabbitmq_password = config['prod']['rabbitmq_password']
    prod_rabbitmq_queue = config['prod']['rabbitmq_queue']
    heartbeat_appname = config['heartbeat']['app_name']
else:
    # We use environment variables
    storage_logs = os.environ.get('STOXYGEN_STORAGE_LOGS')
    logs_filename = os.getenv('STOXYGEN_LOGS_FILENAME', 'Importer.log')
    logs_max_size = os.getenv('STOXYGEN_LOGS_MAX_SIZE', 11000000)
    logs_rotated_files = os.getenv('STOXYGEN_LOGS_ROTATED_FILES', 5)
    prod_log_level = os.getenv('STOXYGEN_LOG_LEVEL', logging.INFO)
    prod_url = os.getenv('STOXYGEN_URL', 'http://localhost/api/v1')
    prod_url_username = os.getenv('STOXYGEN_URL_USERNAME', 'sql-user')
    prod_url_password = os.getenv('STOXYGEN_URL_PASSWORD', '123456')
    prod_rabbitmq_host = os.getenv('STOXYGEN_RABBITMQ_HOST', 'localhost')
    prod_rabbitmq_username = os.getenv('STOXYGEN_RABBITMQ_USERNAME', 'guest')
    prod_rabbitmq_password = os.getenv('STOXYGEN_RABBITMQ_PASSWORD', 'guest')
    prod_rabbitmq_queue = os.getenv('STOXYGEN_RABBITMQ_QUEUE', 'test')
    heartbeat_appname = os.getenv('STOXYGEN_HEARTBEAT_APPNAME', 'downloader')

# Default handling
# Minimum database version this application can handle
database_version = 1


##########
# Logger
##########
log_level = logging.INFO
if logs_filename == "":
    logs_filename = 'Importer.log'

if logs_max_size == "":
    logs_max_size = 11000000

if logs_rotated_files == "":
    logs_rotated_files = 5


if prod_log_level == 'DEBUG':
    log_level = logging.DEBUG

if prod_log_level == 'INFO':
    log_level = logging.INFO

if prod_log_level == 'WARNING':
    log_level = logging.WARNING

if prod_log_level == 'ERROR':
    log_level = logging.ERROR

if prod_log_level == 'CRITICAL':
    log_level = logging.CRITICAL

LOG_FILENAME = storage_logs + logs_filename
# create a logger with the custom name
logging.basicConfig(filename=LOG_FILENAME, level=log_level,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("stockanalyses.Downloader")
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=int(logs_max_size),
                                               backupCount=int(logs_rotated_files))
logger.addHandler(handler)


def getJob():
    """
        Try to get a job from the database
    """
    try:
        url = prod_url + 'job/downloader_jobs'
        logger.info('URL to get job: %s', url)

        r = requests.get(url, auth=(prod_url_username, prod_url_password))
        print(r.text)

        return r.json()

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]", s)


def getWssJob(exchange):
    """
        Try to get coin pairs for websocket subscription
    """
    exchange = exchange.lower()
    try:
        url = prod_url + 'job/wss_downloader_jobs/' + exchange
        logger.info("URL for Websocket-Job: %s", url)
        r = requests.get(url, auth=(prod_url_username, prod_url_password))
        print(r.text)
        logger.debug("Result Websocket-Job: %s", r.text)

        return r.json()

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]", e)


def getDatabaseVersion():
    try:
        url = prod_url + 'dbversion'
        logger.info("URL for database version: %s", url)
        r = requests.get(url, auth=(prod_url_username, prod_url_password))
        print(r.text)
        logger.debug('Result database version: %s', r.text)

        return r.json()
    except requests.exceptions.RequestException as e:
        logger.error('Error [%s]', e)


def register_application(app_id, json_data):
    try:
        url = prod_url + "services/" + app_id
        logger.info("URL for register application: %s", url)
        r = requests.post(url, data=json.dumps(json_data), auth=(prod_url_username, prod_url_password),
                          headers={'Content-Type': 'application/json'})
        logger.debug("Result status code: %s", r.status_code)
        return r.status_code
    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]", e)


def update_application(app_id, instance_id):
    """
    Send heartbeat
    :param app_id: 
    :param instance_id: 
    :return: 
    """

    try:
        url = prod_url + "/services/" + app_id + "/" + instance_id
        logger.info("URL for update application: %s", url)
        r = requests.put(url, auth=(prod_url_username, prod_url_password))
        logger.debug("Result status code: %s", r.status_code)
        return r.status_code
    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]", e)


def updateJob(job_id, new_action, value):
    """
        Update the column action for a specific job
    """
    try:
        json_data = []
        json_data.append({'action': str(new_action)})
        url = prod_url + 'job/set_downloader_jobs_state/' + str(job_id)
        logger.info('URL update job: %s', url)
        r = requests.put(url, auth=(prod_url_username, prod_url_password), data=json.dumps(json_data),
                         headers={'Content-Type': 'application/json'})

        result_text = r.text
        result_text = result_text.encode('utf-8')
        print(result_text)

        return new_action

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))


def sendMessage2Queue(queue_message, exchange, isin):
    credentials = pika.PlainCredentials(prod_rabbitmq_username, prod_rabbitmq_password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(prod_rabbitmq_host, 5672, '/', credentials))

    try:
        # add to json the exchange
        queue_message.update({'exchange': exchange})
        queue_message.update({'isin': isin})
        logger.debug('Added exchange to json: %s' % queue_message)
        json_decoded = json.dumps(queue_message)
        channel = connection.channel()
        channel.queue_declare(queue=prod_rabbitmq_queue, durable=True)

        channel.basic_publish(exchange='', routing_key=prod_rabbitmq_queue, body=json_decoded, properties=pika.BasicProperties(
            content_type='application/json', delivery_mode=2))
        logger.info('Publish message %s to queue %s' % (json_decoded, prod_rabbitmq_queue))
        return True
    except pika.exceptions as e:
        logger.error("Error [%s]" % e)
        return False

    finally:
        connection.close()


def handle_hearbeat():
    """
    We will handle here the heartbeat process.
    1. register
    2. sent heartbeat
    :return: 
    """
    # register application
    hostname = socket.gethostname()
    ipaddr = socket.gethostbyname(hostname)
    app_id = service_heartbeat['app_name']
    instance_id = str(uuid.uuid4())

    json_data = {}
    json_data['hostname'] = hostname
    json_data['ipaddr'] = ipaddr
    json_data['instance_id'] = instance_id
    json_data['state'] = 0

    if register_application(app_id, json_data) == 200:
        # we can continue with heartbeat
        while 1:
            status_heartbeat = update_application(app_id, instance_id)
            if status_heartbeat == 200:
                logger.info("Successful send heartbeat.")
            else:
                logger.warning("Something went wrong with sending heartbeat. Status code: %s", status_heartbeat)
    else:
        logger.warning("Something went wrong in registering the application %s", app_id)


def handle_receiving_data():
    """
    This is our application logic
    :return: 
    """
    # We try to not check the type at application start.
    # First get a job.
    logger.info('Get initial a job...')
    init_job = getJob()
    downloader_type = ''
    isin = ''
    exchange = ''

    if init_job['downloader_jq_id'] != 0 and init_job['action'] == 1000:
        array = init_job['value'].split('#')
        isin = array[1]
        exchange = array[0]

        if exchange == 'btsp':
            downloader_type = 'rest'

        if exchange == 'btfx':
            downloader_type = 'wss'

    if downloader_type == 'rest':
        # We have to update the state of the job. So we are working on it.
        # Start heartbeat thread?!

        try:
            if updateJob(init_job['downloader_jq_id'], '1001', init_job['value']) == '1001':
                while True:
                    action_tmp = "-1"
                    status = False
                    # result = getJob()
                    # isin = ''
                    # exchange = ''

                    if result['downloader_jq_id'] != 0 and result['action'] == 1000 and result['type_idtype'] == 2:
                        # array = result['value'].split("#")
                        # isin = array[1]
                        # exchange = array[0]

                        logger.info('Job for cryptocurrency with id %s and action: %s, isin: %s, exchange: %s' % (
                            result['downloader_jq_id'], result['action'], isin, exchange))
                        action_tmp = updateJob(result['downloader_jq_id'], '1100', result['value'])

                        logger.info('Set action to %s' % '1100')
                    else:
                        logger.info('No job for me...')

                    if action_tmp == '1100':
                        exchange = exchange.lower()
                        # Download data from exchange
                        if exchange == "btsp":
                            bitstamp_client = Public()
                            data_json = bitstamp_client.ticker(isin)

                        if exchange == "btfx":
                            bitfinex_client = Bitfinex()
                            data_json = bitfinex_client.ticker(isin)

                        logger.debug("HTTP Status: %s; JSON - Data: %s" % (data_json[0], data_json[1]))

                        # Data successfully downloaded
                        if data_json[0] == 200:
                            action_tmp = updateJob(result['downloader_jq_id'], '1200', result['value'])
                            logger.info('Set action to %s' % '1200')

                    # Add data to RabbitMQ
                    if action_tmp == '1200':
                        status = sendMessage2Queue(data_json[1], exchange, isin)
                        logger.debug('Returnvalue from function `sendMessage2Queue`: %s' % status)
                        logger.info('Add data to rabbitmq queue `importer` for exchange %s and isin %s' %
                                    (exchange, isin))

                        if status:
                            action_tmp = updateJob(result['downloader_jq_id'], '1300', result['value'])
                            logger.info('Set action to %s' % '1300')
                        else:
                            # we have to send a mail
                            updateJob(result['downloader_jq_id'], '1900', result['value'])
                            logger.warning('Set action to %s' % '1900')
                    else:
                        logger.warning('Something went wrong on downloading data.')
                        updateJob(result['downloader_jq_id'], '1000', result['value'])
                        logger.info('Reset downloader job.')

                    if action_tmp == '0':
                        # we have to send a mail
                        updateJob(result['downloader_jq_id'], '1900', result['value'])
        except Exception as e:
            logger.error(e.args)

    if downloader_type == 'wss':
        logger.info("Websocket")
        if exchange == 'btfx':
            logger.info("Exchange Bitfinex.")

            #job_data = getWssJob('btfx')
            #print(len(job_data['jobs_wss']))
            #logger.info('Found %s jobs', len(job_data['jobs_wss']))

            currency_client = Currency()

            #for i in range(0, len(job_data['jobs_wss'])):
            logger.info('ISIN: %s', isin)
            #print("ISIN: %s" % job_data['jobs_wss'][i]['isin'])
            c = currency_client.getCurrencyPair(isin)
            pair = c['base'] + c['quote']
            pair = pair.upper()

            wss = BtfxWss()
            wss.start()

            while not wss.conn.connected.is_set():
                time.sleep(1)

            # Subscribe to some channels
            # btfx_pairs = ['BTCEUR', 'BTCUSD', 'LTCUSD']


            wss.subscribe_to_ticker(pair)

            # Wait 10 seconds after subscription. Needed for this library.
            time.sleep(10)

            # Accessing data stored in BtfxWss:
            while 1:

                # while not ticker_q.empty():
                try:
                    isin = currency_client.getIsin(pair.lower())
                    logger.debug("Search for %s Isin: %s", pair.lower(), isin)
                    logger.debug("Before access data queue %s length: %s", pair, wss.tickers(pair).qsize())
                    data = wss.tickers(i).get(block=False)
                    print(data)
                    logger.debug("After access data queue %s length: %s", i, wss.tickers(i).qsize())
                    logger.debug("Low: %s, High: %s, Volume: %s, Last Price: %s, Daily change(%%): %s, "
                                     "Daily change: %s, Ask: %s, Bid: %s,  Timestamp: %s", data[0][0][9], data[0][0][8],
                                     data[0][0][7], data[0][0][6], (data[0][0][5] * 100), data[0][0][4], data[0][0][2],
                                     data[0][0][0], data[1])

                    # Create json
                    json_data = {}
                    json_data['high'] = data[0][0][8]
                    json_data['low'] = data[0][0][9]
                    json_data['volume'] = data[0][0][7]
                    json_data['last_price'] = data[0][0][6]
                    json_data['ask'] = data[0][0][2]
                    json_data['bid'] = data[0][0][0]
                    json_data['mid'] = (json_data['bid'] + json_data['ask']) / 2
                    json_data['timestamp'] = data[1]
                    logger.info("JSON: %s", json.dumps(json_data))

                    # for debugging to slow down
                    # time.sleep(10)
                    status = sendMessage2Queue(json_data, exchange, isin)
                    if not status:
                        logger.error("Something went wrong to insert data to rabbitmq. Please send mail.")

                except queue.Empty:
                    pass
                except Exception as e:
                    logger.error(e.args)

            # Unsubscribing from channels:
            #for i in btfx_pairs:
            wss.unsubscribe_from_ticker(pair)

            # Shutting down the client:
            wss.stop()


def main():
    logger.info('Start StockanalysesDownloader...')
    logger.info('Database version in application: %s' % database_version)
    logger.info('Type: %s' % prod_server['type'])

    # we will check if the application can handle the database schema.
    dbversion_data = getDatabaseVersion()
    if dbversion_data['versions'][0]['version_number'] >= int(database_version):
        logger.info('Database version: %s' % dbversion_data['versions'][0]['version_number'])

        try:
            # First thread is for heartbeat
            threading._start_new_thread(handle_hearbeat())

            # Second thread is for application
            threading._start_new_thread(handle_receiving_data())
        except:
            logger.warning("Error: unable to start thread.")

        while 1:
            pass

    else:
        logger.warning("Database version is to low. database: %s, config: %s",
                       dbversion_data['versions'][0]['version_number'], database_version)

if __name__ == '__main__':
    main()
