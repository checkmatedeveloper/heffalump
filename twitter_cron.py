import requests
import json
import pprint
import redis
from config import CheckMateConfig

CONSUMER_KEY = "jYcy6VflImwRfwOKVNOxONM55"
CONSUMER_SECRET = "FW1GnG4SL5oOGIxe0chVO79vjyf3mdbLirHhIcYW5YTpTMLxyW"
TWITTER_TOKEN_URL = "https://api.twitter.com/oauth2/token"
TWITTER_TIMELINE_ENDPOINT = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name=whitesox"


#STEP 1: Obtain an access token
headers = {'Content-Type':"application/x-www-form-urlencoded;charset=UTF-8"}
params = {'grant_type':'client_credentials'}

accessTokenResponse = requests.post(TWITTER_TOKEN_URL, auth=(CONSUMER_KEY, CONSUMER_SECRET), params=params, headers=headers)

#STEP 2: use token to get timeline
accessToken = json.loads(accessTokenResponse.text);

auth = "Bearer " + accessToken['access_token']


headers = {'Content-Type':'application/json', 'Authorization':'Bearer ' + accessToken['access_token']}
params = {'exclude_replies':'true'}
timelineResponse = requests.get(TWITTER_TIMELINE_ENDPOINT, headers=headers, params=params)

#STEP 3: parse out the relevant information
twitterData = json.loads(timelineResponse.text)

tweets = []

for data in twitterData:
    tweet = {}
    #id
    tweet['id'] = data['id']
    #image (if present)
    if 'media' in data['entities']:
        if data['entities']['media'][0]['type'] == "photo":
            tweet['image'] = data['entities']['media'][0]['media_url']
    #tweet text
    tweet['text'] = data['text']
    
    #user_name
    tweet['user_name'] = data['user']['name']

    #user_imate
    tweet['user_image'] = data['user']['profile_image_url']

    #created_at
    import time
    import datetime
    dateString = data['created_at']
    timeStamp  = time.mktime(datetime.datetime.strptime(dateString, '%a %b %d %H:%M:%S +0000 %Y').timetuple())
    tweet['age'] = int(timeStamp)
    
    tweets.append(tweet)


# pprint.pprint(tweets)

#TODO add a comparison here to check if the new data is the same as the old data
#

checkmateconfig = CheckMateConfig()
host = checkmateconfig.REDIS_SOCIAL_PUBSUB_HOST
port = checkmateconfig.REDIS_SOCIAL_PUBSUB_PORT
db = checkmateconfig.REDIS_SOCIAL_PUBSUB_DB
password = checkmateconfig.REDIS_SOCIAL_PUBSUB_PASSWORD
r = redis.Redis(host, port, db, password)

social = {"twitter":tweets}                                                                                     

#STEP 4: publish results to redis and push them through the socket
try:
    lastTwitter = r.get("last_social")
    if lastTwitter != json.dumps(social):
        r.set("last_social", json.dumps(social))
        r.publish("social_channel", json.dumps(social))
except AttributeError:
    r.set("last_social", json.dumps(social))
    r.publish("social_channel", json.dumps(social))
