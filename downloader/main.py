#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import os
import json
import logging.handlers
import time as t
from datetime import datetime
import pymysql.cursors

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
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'], prod_server['database'])

        # prepare cursor
        cursor_job = db.cursor()

        sql = 'select action, value, type_idtype, timestamp from downloader_jq where state = 0 and timestamp <= now() ' \
              'order by timestamp desc limit 1;'
        logger.debug(sql)
        cursor_job.execute(sql)

        # check if we get a result
        if cursor_job.rowcount > 0:
            result = cursor_job.fetchone()
        else:
            result = 0

        return result

    except pymysql.Error as e:
	    db.rollback()
	    logger.error("Error [%s]" % (e))

    finally:
        cursor_job.close()
        db.close()

'''
    Update the column action for a specific job
'''
def updateJob(current_action, new_action, value):
    try:
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'], prod_server['database'])

        # prepare cursor
        cursor_job = db.cursor()

        sql = "update `downloader_jq` set `action`= %s, `modify_timestamp` = now(), `modify_user` = 'downloader' where `action` = %s and `value` = %s;"
        logger.debug(sql % (new_action, current_action, value))
        cursor_job.execute(sql,(new_action, current_action, value))

        db.commit()

    except pymysql.Error as e:
        db.rollback()
        new_action = 0
        logger.error("Error [%s]" % (e))

    finally:
        return new_action
        db.close()

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
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'],
                             prod_server['database'])

        # prepare cursor
        cursor_job = db.cursor()

        sql = "insert into import_jq(`action`, `id_stock`, `filename`, `timestamp`, `insert_timestamp`, `insert_user`, " \
              "`modify_timestamp`, `modify_user`) values (%s, %s, %s, now(), now(), 'downloader', now(), 'downloader');"

        cursor_job.execute(sql, (action, value, filename))

        db.commit()
        return True

    except pymysql.Error as e:
        db.rollback()
        logger.error("Error [%s]" % (e))
        return False

    finally:
        db.close()



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


        if result != 0 and result[0] == 1000:
            array = result[1].split("#")
            base = array[1]
            quote = array[2]
            exchange = array[0]

            logger.info('Job action: %s, base: %s, quote: %s, exchange: %s' % (result[0], base, quote, exchange))
            action_tmp = updateJob(result[0], '1100', result[1])

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
                action_tmp = updateJob(action_tmp, '1200', result[1])
                logger.info('Set action to %s' % ('1200'))

            if action_tmp == '1200':
                status = insertImportJQ(status_code[1], result[0], result[1])

            if status == True:
                action_tmp = updateJob(action_tmp, '1300', result[1])
            else:
                # we have to send a mail
                updateJob(action_tmp, '1900', result[1])

        if action_tmp == '0':
            # we have to send a mail
            updateJob(action_tmp, '1900', result[1])

        t.sleep(5)


if __name__ == '__main__':
    main()