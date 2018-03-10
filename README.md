It's a microservice which download stock and bitcoin data. In the file 'config.ini' you can configure the microservice 
for RESTful-API or Websocket. For RESTful-API you have to set the key 'type' to 'rest' and for Websocket to 'websocket'.

Possible actions for RESTful API:
 - current data
    - 1000 [current data]
    - 1100 [job is in processing]
    - 1200 [data successful downloaded]
    - 1300 [data inserted in import_jq]
    - 1900 [error with the job]
 - weekly data
 - historical data
 
 ## Exchanges
 The following exchanges are supported at the moment:
 - Bitstamp
 - Bitfinex
 
 
 ## Update dependencies
 `pipreqs --force /opt/stockanalyses-downloader/`
 
 ## Environment Variables
 - `STOXYGEN_STORAGE_LOGS`
 - `STOXYGEN_LOGS_FILENAME`
 - `STOXYGEN_LOGS_MAX_SIZE`
 - `STOXYGEN_LOGS_ROTATED_FILES`
 - `STOXYGEN_LOG_LEVEL`
 - `STOXYGEN_URL`
 - `STOXYGEN_URL_USERNAME`
 - `STOXYGEN_URL_PASSWORD`
 - `STOXYGEN_RABBITMQ_HOST`
 - `STOXYGEN_RABBITMQ_USERNAME`
 - `STOXYGEN_RABBITMQ_PASSWORD`
 - `STOXYGEN_RABBITMQ_QUEUE`
 - `STOXYGEN_HEARTBEAT_APPNAME`