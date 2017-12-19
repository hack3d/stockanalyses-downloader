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