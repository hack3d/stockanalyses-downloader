#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import os
import json
import logging.handlers
import time as t
from datetime import datetime
import pymysql.cursors
import requests

from downloader.plugins.bitstamp import client


# config
dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(dir_path + '/config')

prod_server = config['prod']
storage = config['path']


##########
# Logger
##########
LOG_FILENAME = storage['storage_logs']+'Downloader.log'
# create a logger with the custom name
logger = logging.getLogger('stockanalyses.Downloader')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler(LOG_FILENAME)
fh.setLevel(logging.DEBUG)
# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=11000000, backupCount=5)
#create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(handler)
#logger.addHandler(ch)


'''
    Try to get a job from the database
'''
def getJob():
    try:
        print(prod_server['url'] + 'job/downloader_jobs')
        r = requests.get(prod_server['url'] + 'job/downloader_jobs')
        print(r.text)

        return r.json()

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))


'''
    Update the column action for a specific job
'''
def updateJob(job_id, new_action, value):
    try:
        json_data = []
        json_data.append({'action': str(new_action)})
        print(prod_server['url'] + 'job/set_downloader_jobs_state/' + str(job_id))
        r = requests.put(prod_server['url'] + 'job/set_downloader_jobs_state/' + str(job_id), data=json.dumps(json_data), headers={'Content-Type': 'application/json'})

        result_text = r.text
        result_text = result_text.encode('utf-8')
        print(result_text)

        return new_action

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))


'''
    Generate a unique filename
'''
def generateFilename(filename):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y%m%d%H%M%S")

    return filename+"_"+timestamp

'''
    Downloading bitcoin data
'''
def downloadBitcoindata_Current(data, exchange):

    filename_json = generateFilename(exchange)

    logger.debug("Filename to save: %s" % filename_json)

    # save json to file
    with open(storage['store_data'] + filename_json, 'w') as f:
        json.dump(data[1], f)

    f.close()

    return data[0], filename_json

'''
    Insert the file into the import job queue
'''
def insertImportJQ(filename, action, value):
    try:
        json_data = []
        json_data.append({"action": str(action), "id_stock": str(value), "filename": str(filename)})
        logger.debug('insertImportJQ JSON-Data: %s' % (json_data))

        print(prod_server['url'] + 'job/add-import-data')
        r = requests.post(prod_server['url'] + 'job/add-import-data', data=json.dumps(json_data), headers={'Content-Type': 'application/json'})

        result_text = r.text
        result_text = result_text.encode('utf-8')
        print(result_text)
        logger.debug('Result for insert into import_jq: %s' % (result_text))
        return True

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))
        return False




def main():
    logger.info('Start StockanalysesDownloader...')
    bitstamp_client = client.Public()

    while True:
        logger.debug('Get a job...')
        action_tmp = "-1"
        status = False
        result = getJob()
        base = ''
        quote = ''
        exchange = ''


        if result['downloader_jq_id'] != 0 and result['action'] == 1000:
            array = result['value'].split("#")
            base = array[1]
            quote = array[2]
            exchange = array[0]

            logger.info('Job with id %s and action: %s, base: %s, quote: %s, exchange: %s' % (result['downloader_jq_id'], result['action'], base, quote, exchange))
            action_tmp = updateJob(result['downloader_jq_id'], '1100', result['value'])

            logger.info('Set action to %s' % ('1100'))
        else:
            logger.info('No job for me...')

        if action_tmp == '1100':
            if exchange == "bitstamp":
                data_json = bitstamp_client.ticker(base, quote)

            logger.debug("HTTP Status: %s; JSON - Data: %s" % (data_json[0], data_json[1]))

            status_code = downloadBitcoindata_Current(data_json, exchange)
            logger.debug("Status returned from download Bitcoindata_Current() is %s" % (status_code[0]))

            if status_code[0] == 200:
                action_tmp = updateJob(result['downloader_jq_id'], '1200', result['value'])
                logger.info('Set action to %s' % ('1200'))

            if action_tmp == '1200':
                status = insertImportJQ(status_code[1], result['action'], result['value'])
                logger.info('Add file: %s to import_jq' % (status_code[1]))

            if status:
                action_tmp = updateJob(result['downloader_jq_id'], '1300', result['value'])
            else:
                # we have to send a mail
                updateJob(result['downloader_jq_id'], '1900', result['value'])

        if action_tmp == '0':
            # we have to send a mail
            updateJob(result['downloader_jq_id'], '1900', result['value'])

        t.sleep(5)


if __name__ == '__main__':
    main()