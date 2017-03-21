It's a microservice which download stock and bitcoin data.

Possible actions:
 - current data
  - 1000 [current data]
  - 1100 [job is in processing]
  - 1200 [data successful downloaded]
  - 1300 [data inserted in import_jq]
  - 1900 [error with the job]
 - weekly data
 - historical data