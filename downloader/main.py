#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql.cursors
import os, sys, configparser
import logging.handlers
import json, requests
from datetime import datetime, time
import time as t

# config
config = configparser.ConfigParser()
config.read('config')

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
def downloadBitcoindata_Current():
    r = requests.get("http://api.bitcoincharts.com/v1/markets.json")
    logger.info("Status from download: %s" % str(r.status_code))

    filename_json = generateFilename('bitcoin')

    # save json to file
    data = r.json()
    with open(storage['store_data'] + filename_json, 'w') as f:
        json.dump(data, f)

    return r.status_code, filename_json

'''
    Insert the file into the import job queue
'''
def insertImportJQ(filename, action):
    try:
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'],
                             prod_server['database'])

        # prepare cursor
        cursor_job = db.cursor()

        sql = "insert into import_jq(`action`, `idstock`, `filename`, `timestamp`, `insert_timestamp`, `insert_user`, " \
              "`modify_timestamp`, `modify_user`) values (%s, 'bitcoin', %s, now(), 'downloader', now(), 'downloader');"

        cursor_job.execute(sql, (action, filename))

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
    while True:
        logger.debug('Get a job...')
        status = False
        result = getJob()


        if result != 0 and result[0] == 1000:
            logger.info('Job action %s, value %s' % (result[0], result[1]))
            action_tmp = updateJob(result[0], '1100', result[1])

            logger.info('Set action to %s' % ('1100'))
        else:
            logger.info('No job for me...')

        if action_tmp == '1100':
            status_code = downloadBitcoindata_Current()
            logger.debug("Status returned from downloadBitcoindata_Current() is %s" % (status_code[0]))

            if status_code[0] == '200':
                action_tmp = updateJob(action_tmp, '1200', result[1])
                logger.info('Set action to %s' % ('1200'))

            if action_tmp == '1200':
                status = insertImportJQ(status_code[1], result[1])

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