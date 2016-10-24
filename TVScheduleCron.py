from db_connection import DbConnection
from TVDB import TVDB
import requests

ROVI_API_KEY = "ytagyaun2qzaah4a886etaj6"
ROVI_API_URL = "http://api.rovicorp.com/TVlistings/v9/listings/gridschedule/"
DURATION = 240

conn = DbConnection().connection
dbCore = TVDB(conn)


locationServices = dbCore.getLocationServices()


for row in locationServices:
    venueUid, locationUid = row
    print "LOCATION " + str(venueUid) + " = "  + str(locationUid)
    
    #get venue channels
    channelList = dbCore.getChannelList(venueUid)

    sourceIds = []
    for channel in channelList:
        
        channelUid, channelName, venueChannelNumber, roviSourceId = channel
        if roviSourceId is not None:
            sourceIds.append(roviSourceId)

    sourceIdList = ','.join(str(x) for x in sourceIds)

    rovi_url = ROVI_API_URL + str(locationUid) + "/info?apikey=" + ROVI_API_KEY + "&locale=en-US&duration=" + str(DURATION) + "&sourceid=" + sourceIdList

    print rovi_url
    response = requests.get(rovi_url)

    channelsJSON = response.json() 

    gridChannels = channelsJSON['GridScheduleResult']['GridChannels']
    for gridChannel in gridChannels:
        channelUid = dbCore.getChannelId(venueUid, gridChannel['SourceId'])
        print "ChannelUid: " + str(channelUid)
 
        airings = gridChannel['Airings']

        for airing in airings:
            programId = airing['ProgramId']
            showTitle = airing['Title']
            episodeTitle = airing.get('EpisodeTitle', None)
            airingTime = airing['AiringTime']
            durationInMinutes = airing['Duration']
            closedCaptioned = airing.get('CC', False)
            if closedCaptioned:
                closedCaptioned = 1 
            else: 
                closedCaptioned = 0
            hd = airing.get('HD', False)
            if hd: 
                hd = 1 
            else: 
                hd = 0

            tvRating = airing.get('TVRating', None)
            category = airing.get('Category', None)
            subcategory = airing.get('Subcategory', None)
            sports = airing.get('Sports', None)
            
            if sports: 
                sports = 1 
            else: 
                sports = 0
            try:
                #print "attempting to add airing"
                #print str((channelUid, programId, showTitle, episodeTitle, airingTime, durationInMinutes, closedCaptioned, hd, tvRating, category, subcategory, sports))
                dbCore.addChannelAiring(channelUid, programId, showTitle, episodeTitle, airingTime, durationInMinutes, closedCaptioned, hd, tvRating, category, subcategory, sports)
            except:
                print "There was a problem"
