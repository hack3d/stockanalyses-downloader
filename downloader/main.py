#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import os
import json
import logging.handlers
import time as t
import requests
import pika

from downloader.plugins.bitstamp.client import Public
from downloader.plugins.bitfinex.client import Bitfinex


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
                    logger.warn('Set action to %s' % '1900')

            if action_tmp == '0':
                # we have to send a mail
                updateJob(result['downloader_jq_id'], '1900', result['value'])

            # no sleep time required on prod
            #t.sleep(5)


if __name__ == '__main__':
    main()
