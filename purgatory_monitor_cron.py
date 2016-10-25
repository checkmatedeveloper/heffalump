from Levy_DB import Levy_Db
from db_connection import DbConnection
from Levy_integrations_worker import LevyIntegrationWorker

from config import CheckMateConfig

import redis

checkmateconfig = CheckMateConfig()
host = checkmateconfig.REDIS_CACHE_HOST
port = checkmateconfig.REDIS_CACHE_PORT
db = checkmateconfig.REDIS_CACHE_DB
password = checkmateconfig.REDIS_CACHE_PASSWORD
redisInstance = redis.Redis(host, port, db, password)

conn = DbConnection().connection


levyDB = Levy_Db(conn, redisInstance)

rowsToAppy =  levyDB.countRowsToApply()

if rowsToAppy != 0:
    
    liw = LevyIntegrationWorker(levyDB)
    liw.main()
else:
    print "Nothing to do"
