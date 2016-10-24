
from config import CheckMateConfig

class SocialFeedDb:
	
    def __init__(self, db):
        self.db = db
        self.dbc = self.db.cursor()
        self.checkmateconfig = CheckMateConfig()
    def getTodaysHomeEgos(self):
                # get the event with event_date closest to now() ; most recently started event.
        self.dbc.execute('SELECT ego_uid, events.id as event_uid, venue_uid\
                                  FROM setup.events\
                                  JOIN  setup.events_x_egos ON events.id = events_x_egos.event_uid\
                                  WHERE event_date > NOW() AND\
                                    event_date < DATE_ADD(NOW(), INTERVAL 24 HOUR) AND\
                                    setup.events_x_egos.is_home = 1\
                                  ORDER BY event_date DESC')

        return self.dbc.fetchall()

    def getEgosTwitterFeeds(self, ego):
        self.dbc.execute('SELECT consumer_key, consumer_secret, screen_name\
			  	  FROM social.social_feed_twitter\
			          WHERE ego_uid = ' + str(ego) + ' AND is_active = 1');
        return self.dbc.fetchall()
	
    def getVenueSuitemateTablets(self, venue):
        self.dbc.execute('SELECT device_id from tablets.venues_x_tablet\
				 JOIN tablets.system_versions ON venues_x_tablet.current_system_version_uid = system_versions.id\
				 WHERE system_versions.system_uid = 2')
        return [item[0] for item in self.dbc.fetchall()]

    def sendTweets(self, event, venue, tweets):
       
        tablets = self.getVenueSuitemateTablets(venue) 	
			
        data = {}
        data["type"] = "twitter"
        data["message"] = tweets
        data["device_uids"] = tablets
        import json
        import redis
        host = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_HOST
        port = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_PORT
        db = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_DB
        password = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_PASSWORD
        r = redis.Redis(host, port, db, password)
        
        try:
			#lastTwitter = r.get("last_social")
			#if lastTwitter != json.dumps(data):
            last_social_key = "last_social_{0}".format(event)
            social_channel_key = "social_channel_{0}".format(venue)
            
            print "Last social key: " +  last_social_key
            
            r.set(last_social_key, json.dumps(data))
            r.expire(last_social_key, 86400)
            print "Social Channel Key " + social_channel_key
            r.publish(social_channel_key, json.dumps(data))
        except AttributeError:
            r.set(last_social_key, json.dumps(data))
            r.expire(last_social_key, 86400)
            r.publish(social_channel_key, json.dumps(data))
