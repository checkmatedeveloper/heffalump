from config import CheckMateConfig

class TVDB:
    
    def __init__(self, db):
        self.db = db
        

    def getLocationServices(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            venue_uid, \
                            rovi_location_service_id \
                        FROM setup.rovi_location_service_ids")
        return  cursor.fetchall()
    

    def getChannelList(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            channel_name, \
                            venue_channel_number, \
                            rovi_source_id \
                        FROM setup.tv_channels \
                        WHERE venue_uid = %s",
                        (venueUid))
        return cursor.fetchall()


    def addChannelAiring(self, channelUid, programId, showTitle, episodeTitle, airingTime, durationInMinutes, closeCaptioned, hd, tvRating, category, subcategory, sports):
        cursor = self.db.cursor()
        cursor.execute("INSERT IGNORE INTO `setup`.`tv_airings`( \
                            `channel_uid`, \
                            `program_id`, \
                            `show_title`, \
                            `episode_title`, \
                            `airing_time`, \
                            `duration_in_minutes`, \
                            `close_captioned`, \
                            `hd`, \
                            `tv_rating`, \
                            `category`, \
                            `subcategory`, \
                            `sports`, \
                            `created_at` \
                        )VALUES( \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());",
                        (channelUid, programId, showTitle, episodeTitle, airingTime, durationInMinutes, closeCaptioned, hd, tvRating, category, subcategory, sports))
        self.db.commit()


    def getChannelId(self, venueUid, roviSourceId):
        print str(venueUid) + " " + str(roviSourceId)
        cursor = self.db.cursor()
        cursor.execute("SELECT id FROM setup.tv_channels \
                        WHERE venue_uid = %s AND rovi_source_id = %s",
                        (venueUid, roviSourceId))
        return cursor.fetchone()[0]
