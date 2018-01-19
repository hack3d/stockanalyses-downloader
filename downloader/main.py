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

# Third-party
from btfxwss import BtfxWss

# Homebrew
from plugins.bitstamp.client import Public
from plugins.bitfinex.client import Bitfinex
from plugins.IsinToCurrencyPair.currency import Currency


# config
dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(dir_path + '/config')

prod_server = config['prod']
storage = config['path']

##########
# Logger
##########
LOG_FILENAME = storage['storage_logs'] + 'Downloader.log'
# create a logger with the custom name
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("stockanalyses.Downloader")
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=11000000, backupCount=5)
logger.addHandler(handler)


def getJob():
    """
        Try to get a job from the database
    """
    try:
        print(prod_server['url'] + 'job/downloader_jobs')
        r = requests.get(prod_server['url'] + 'job/downloader_jobs',
                         auth=(prod_server['username'], prod_server['password']))
        print(r.text)

        return r.json()

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))


def getWssJob(exchange):
    """
        Try to get coin pairs for websocket subscription
    """
    exchange = exchange.lower()
    try:
        print(prod_server['url'] + 'job/wss_downloader_jobs/' + exchange)
        logger.info("URL for Websocket-Job: %s", prod_server['url'] + 'job/wss_downloader_jobs/' + exchange)
        r = requests.get(prod_server['url'] + 'job/wss_downloader_jobs/' + exchange,
                         auth=(prod_server['username'], prod_server['password']))
        print(r.text)
        logger.debug("Result Websocket-Job: %s", r.text)

        return r.json()

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))


def updateJob(job_id, new_action, value):
    """
        Update the column action for a specific job
    """
    try:
        json_data = []
        json_data.append({'action': str(new_action)})
        print(prod_server['url'] + 'job/set_downloader_jobs_state/' + str(job_id))
        r = requests.put(prod_server['url'] + 'job/set_downloader_jobs_state/' + str(job_id),
                         auth=(prod_server['username'], prod_server['password']),
                         data=json.dumps(json_data), headers={'Content-Type': 'application/json'})

        result_text = r.text
        result_text = result_text.encode('utf-8')
        print(result_text)

        return new_action

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))


def sendMessage2Queue(queue_message, exchange, isin):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    try:
        # add to json the exchange
        queue_message.update({'exchange': exchange})
        queue_message.update({'isin': isin})
        logger.debug('Added exchange to json: %s' % queue_message)
        json_decoded = json.dumps(queue_message)
        channel = connection.channel()
        channel.queue_declare(queue='importer', durable=True)

        channel.basic_publish(exchange='', routing_key='importer', body=json_decoded, properties=pika.BasicProperties(
            content_type='application/json', delivery_mode=2))
        logger.info('Publish message %s to queue %s' % (json_decoded, 'importer'))
        return True
    except pika.exceptions as e:
        logger.error("Error [%s]" % e)
        return False

    finally:
        connection.close()


def main():
    logger.info('Start StockanalysesDownloader...')
    logger.info('Type: %s' % prod_server['type'])

    if prod_server['type'] == 'rest':

        while True:
            logger.debug('Get a job...')
            action_tmp = "-1"
            status = False
            result = getJob()
            isin = ''
            exchange = ''

            if result['downloader_jq_id'] != 0 and result['action'] == 1000 and result['type_idtype'] == 2:
                array = result['value'].split("#")
                isin = array[1]
                exchange = array[0]

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
                        logger.info('Add data to rabbitmq queue `importer` for exchange %s and isin %s' % (exchange, isin))

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

            # no sleep time required on prod
            #t.sleep(5)

    if prod_server['type'] == 'wss':
        logger.info("Websocket")
        if prod_server['exchange'] == 'btfx':
            logger.info("Exchange Bitfinex.")

            job_data = getWssJob('btfx')
            print(len(job_data['jobs_wss']))
            logger.info('Found %s jobs', len(job_data['jobs_wss']))

            btfx_pairs = []
            currency_client = Currency()

            for i in range(0, len(job_data['jobs_wss'])):
                logger.info('ISIN: %s', job_data['jobs_wss'][i]['isin'])
                print("ISIN: %s" % job_data['jobs_wss'][i]['isin'])
                c = currency_client.getCurrencyPair(job_data['jobs_wss'][i]['isin'])
                pair = c['base'] + c['quote']
                btfx_pairs.append(pair.upper())

            wss = BtfxWss()
            wss.start()

            while not wss.conn.connected.is_set():
                time.sleep(1)

            # Subscribe to some channels
            #btfx_pairs = ['BTCEUR', 'BTCUSD', 'LTCUSD']

            for i in btfx_pairs:
                wss.subscribe_to_ticker(i)

            # Wait 10 seconds after subscription. Needed for this library.
            time.sleep(10)

            # Accessing data stored in BtfxWss:
            while 1:

                #while not ticker_q.empty():
                for i in btfx_pairs:
                    try:
                        isin = currency_client.getIsin(i.lower())
                        logger.debug("Search for %s Isin: %s", i.lower(), isin)
                        logger.debug("Before access data queue %s length: %s", i, wss.tickers(i).qsize())
                        data = wss.tickers(i).get(block=False)
                        print(data)
                        logger.debug("After access data queue %s length: %s", i, wss.tickers(i).qsize())
                        logger.debug("Low: %s, High: %s, Volume: %s, Last Price: %s, Daily change(%%): %s, "
                                     "Daily change: %s, Ask: %s, Bid: %s,  Timestamp: %s", data[0][0][9], data[0][0][8],
                                     data[0][0][7], data[0][0][6], (data[0][0][5]*100), data[0][0][4], data[0][0][2],
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
                        #time.sleep(10)
                        status = sendMessage2Queue(json_data, 'btfx', isin)
                        if not status:
                            logger.error("Something went wrong to insert data to rabbitmq. Please send mail.")

                    except queue.Empty:
                        pass
                    except Exception as e:
                        logger.error(e.args)

            # Unsubscribing from channels:
            for i in btfx_pairs:
                wss.unsubscribe_from_ticker(i)

            # Shutting down the client:
            wss.stop()

if __name__ == '__main__':
    main()
