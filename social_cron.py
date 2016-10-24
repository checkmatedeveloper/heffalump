import requests
import json
import pprint
from social_feed_db import SocialFeedDb
from db_connection import DbConnection

TWITTER_TOKEN_URL = "https://api.twitter.com/oauth2/token"
TWITTER_TIMELINE_ENDPOINT = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="

def getTweets(consumerKey, consumerSecret, screenName):
	#TODO: abstrct this into its own file

	print "Getting tweets for: " + screenName
	#STEP 1: Obtain an access token
	headers = {'Content-Type':"application/x-www-form-urlencoded;charset=UTF-8"}
	params = {'grant_type':'client_credentials'}

	accessTokenResponse = requests.post(TWITTER_TOKEN_URL, auth=(consumerKey, consumerSecret), params=params, headers=headers)

	print accessTokenResponse
	#STEP 2: use token to get timeline
	accessToken = json.loads(accessTokenResponse.text);

	auth = "Bearer " + accessToken['access_token']


	headers = {'Content-Type':'application/json', 'Authorization':'Bearer ' + accessToken['access_token']}
	params = {'exclude_replies':'true'}
	timelineResponse = requests.get(TWITTER_TIMELINE_ENDPOINT + screenName, headers=headers, params=params)

	
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

	return tweets


##################################################

db = DbConnection().connection

socialDb = SocialFeedDb(db)

todaysEgos = socialDb.getTodaysHomeEgos()

print todaysEgos

for ego in  todaysEgos:	
    twitterFeeds = socialDb.getEgosTwitterFeeds(ego[0])
    for feed in twitterFeeds:
        tweets = getTweets(feed[0], feed[1], feed[2])
        print tweets
        socialDb.sendTweets(ego[1], ego[2], tweets)
       
 	
