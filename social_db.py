#!/usr/bin/env python

# import MySQLdb
# from db_connection import DbConnection
import json
import redis
from config import CheckMateConfig

class SocialDb:

    def __init__(self):
        self.checkmateconfig = CheckMateConfig

    def updateCachedSocialData(self, venue_uid, data ):
        """    
        Updates the cached social data 

        Parameters
        ----------
        venue_uid : int
        data : dictionary

        Returns
        -------
        out : True

        """
       
        if venue_uid == 202:
            host = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_HOST
            port = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_PORT
            db = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_DB
            password = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_PASSWORD
            r = redis.Redis(host, port, db, password)
            r.publish("social_channel", json.dumps(data))

        return True
