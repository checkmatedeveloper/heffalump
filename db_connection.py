#!/usr/bin/env python

import MySQLdb
import os
from config import CheckMateConfig

class DbConnection:
    connection = None
    class Singleton:
        def __init__(self):
            checkmateconfig = CheckMateConfig()
            self.HOST = checkmateconfig.DB_HOST
            self.USER = checkmateconfig.DB_USER
            self.PASS = checkmateconfig.DB_PASS

            '''
            env_var = os.getenv('Chkm8WorkerServerType')
            if env_var=='PROD':
                self.HOST = 'production.cgfo05y38ueo.us-east-1.rds.amazonaws.com'
                self.USER = 'rabbit_worker'
                self.PASS = 'ch3ckm@t3w0rk3r'
            else:
                self.HOST = 'development.cgfo05y38ueo.us-east-1.rds.amazonaws.com'
                self.USER = 'devmaster'
                self.PASS = 'As7bwo&d8'
            '''

            # add singleton variables here
            #self.connection = MySQLdb.Connection(self.HOST, self.USER, self.PASS)
            self.connection = MySQLdb.connect(self.HOST, self.USER, self.PASS)

    def __init__(self):
        if DbConnection.connection is None:
            DbConnection.connection = DbConnection.Singleton().connection

    def closeConnection(self):
        DbConnection.connection.close()
        DbConnection.connection = None
