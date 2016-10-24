from Levy_DB import Levy_Db
from db_connection import DbConnection
from Levy_ProcessPurgatory import LevyIntegrationWorker

from config import CheckMateConfig

import redis
import HipChat

checkmateconfig = CheckMateConfig()
host = checkmateconfig.REDIS_CACHE_HOST
port = checkmateconfig.REDIS_CACHE_PORT
db = checkmateconfig.REDIS_CACHE_DB
password = checkmateconfig.REDIS_CACHE_PASSWORD
redisInstance = redis.Redis(host, port, db, password)

conn = DbConnection().connection


levyDB = Levy_Db(conn, redisInstance)

rowsToApply =  levyDB.countRowsToApply()

if rowsToApply != 0:
    
    HipChat.sendMessage("Attempting to apply: " + str(rowsToApply) + " purgatory rows", "IntCron", 1066556, "green")
    liw = LevyIntegrationWorker(levyDB)
    liw.main()

