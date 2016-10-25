#!/usr/bin/env python

# import MySQLdb
# from db_connection import DbConnection
import json
import redis
from config import CheckMateConfig

class WeatherDb:
    '''A class for abstracting Dinexus mySQL queries'''

    def __init__(self, db):
        self.db = db
        # prepare a cursor object using cursor() method
        self.dbc = self.db.cursor()
        self.checkmateconfig = CheckMateConfig()

    def updateCachedLocationWeatherCondition(self, data ):
        """    
        Updates the cached location weather condition data 

        Parameters
        ----------
        data : dictionary

        Returns
        -------
        out : True

        """
       
        conditions = {}
        conditions['conditions'] = {}
        conditions['conditions']['weather']               = str( data['weather'] )
        conditions['conditions']['temperature_string']    = str( data['temperature_string'] )
        conditions['conditions']['temp_f']                = str( data['temp_f'] )
        conditions['conditions']['wind_string']           = str( data['wind_string'] )
        conditions['conditions']['wind_dir']              = str( data['wind_dir'] )
        conditions['conditions']['wind_mph']              = str( data['wind_mph'] )

        self.dbc.execute("select venue_uid from weather.venues_x_locations where location_uid = %s", data['location_uid'])
        rows = self.dbc.fetchall()

        if rows == None:
            raise Exception("Unable to find venue for location %s" % data['location_uid'])

        host = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_HOST
        port = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_PORT
        db = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_DB
        password = self.checkmateconfig.REDIS_SOCIAL_PUBSUB_PASSWORD
        r = redis.Redis(host, port, db, password)

        for row in rows:
            venue_uid = int( row[0] )

            #if venue_uid == 202:
            key_conditions_last = "conditions_last_{0}".format(venue_uid)
            key_weather_channel = "weather_channel_{0}".format(venue_uid)

            r.set(key_conditions_last, json.dumps(conditions))
            r.publish(key_weather_channel, json.dumps(conditions))

        return True

    def getAllLocations(self):
        """
        Gets zipcode for all locations in weather.locations table

        Parameters
        ----------
        none

        Returns
        -------
        out : array

        """

        self.dbc.execute("select id, location_zip from weather.locations order by id desc");
        rows = self.dbc.fetchall()

        if rows == None:
            results = False
        else:
            results = []
            for row in rows:
                tmp = {}
                tmp['location_uid'] = int( row[0] )    
                tmp['zipcode']      = int( row[1] )
                results.append(tmp)

        return results

    def getLastWeatherConditionByLocation(self, location_uid):
        """
        Gets last row in conditions table for the specified location_uid

        Parameters
        ----------
        location_uid : int

        Returns
        -------
        out : dictionary
            { "weather" : string, "temperature_string" : string, "temp_f" : float, "wind_string" : string, "wind_dir" : string, "wind_mph" : float }

        """

        # condition, temp, wind direction, wind speed
        self.dbc.execute("select weather, temperature_string, temp_f, wind_string, wind_dir, wind_mph from weather.conditions where location_uid = %s order by id desc limit 1", location_uid)
        row = self.dbc.fetchone()

        if row == None:
            result = False
        else:
            result = {}
            result['weather']               = str( row[0] )
            result['temperature_string']    = str( row[1] )
            result['temp_f']                = float( row[2] )
            result['wind_string']           = str( row[3] )
            result['wind_dir']              = str( row[4] )
            result['wind_mph']              = float( row[5] )

        return result

    def addWeatherCondition(self, data):
        """  

        Parameters
        ----------
        data : dictionary

        Returns
        -------
        out : Bool

        """

        location_uid = data['location_uid'] 

        try:
            image_url               = '' if 'image' not in data or 'url' not in data['image'] or data['image']['url'] == None or data['image']['url'] == 'NA' else str( data['image']['url'] )
        except ValueError:
            image_url               = ''
        try:
            image_title             = '' if 'image' not in data or 'title' not in data['image'] or data['image']['title'] == None or data['image']['title'] == 'NA' else str( data['image']['title'] )
        except ValueError:
            image_title             = ''
        try:
            image_link              = '' if 'image' not in data or 'link' not in data['image'] or data['image']['link'] == None or data['image']['link'] == 'NA' else str( data['image']['link'] )
        except ValueError:
            image_link              = ''
        try:
            weather                 = '' if 'weather' not in data or data['weather'] == None or data['weather'] == 'NA' else str( data['weather'] )
        except ValueError:
            weather                 = ''
        try:
            temperature_string      = '' if 'temperature_string' not in data or data['temperature_string'] == None or data['temperature_string'] == 'NA' else str( data['temperature_string'] )
        except ValueError:
            temperature_string      = ''
        try:
            temp_f                  = 0 if 'temp_f' not in data or data['temp_f'] == None or data['temp_f'] == 'NA' else float( data['temp_f'] )
        except ValueError:
            temp_f                  = 0
        try:
            temp_c                  = 0 if 'temp_c' not in data or data['temp_c'] == None or data['temp_c'] == 'NA' else float( data['temp_c'] )
        except ValueError:
            temp_c                  = 0
        try:
            relative_humidity       = '' if 'relative_humidity' not in data or data['relative_humidity'] == None or data['relative_humidity'] == 'NA' else str( data['relative_humidity'] )
        except ValueError:
            relative_humidity       = ''
        try:
            wind_string             = '' if 'wind_string' not in data or data['wind_string'] == None or data['wind_string'] == 'NA' else str( data['wind_string'] )
        except ValueError:
            wind_string             = ''
        try:
            wind_dir                = '' if 'wind_dir' not in data or data['wind_dir'] == None or data['wind_dir'] == 'NA' else str( data['wind_dir'] )
        except ValueError:
            wind_dir                = ''
        try:
            wind_degrees            = 0 if 'wind_degrees' not in data or data['wind_degrees'] == None or data['wind_degrees'] == 'NA' else float( data['wind_degrees'] )
        except ValueError:
            wind_degrees            = 0
        try:
            wind_mph                = 0 if 'wind_mph' not in data or data['wind_mph'] == None or data['wind_mph'] == 'NA' else float( data['wind_mph'] )
        except ValueError:
            wind_mph                = 0
        try:
            wind_gust_mph           = 0 if 'wind_gust_mph' not in data or data['wind_gust_mph'] == None or data['wind_gust_mph'] == 'NA' else float( data['wind_gust_mph'] )
        except ValueError:
            wind_gust_mph           = 0
        try:
            wind_kph                = 0 if 'wind_kph' not in data or data['wind_kph'] == None or data['wind_kph'] == 'NA' else float( data['wind_kph'] )
        except ValueError:
            wind_kph                = 0
        try:
            wind_gust_kph           = 0 if 'wind_gust_kph' not in data or data['wind_gust_kph'] == None or data['wind_gust_kph'] == 'NA' else float( data['wind_gust_kph'] )
        except ValueError:
            wind_gust_kph           = 0
        try:
            pressure_mb             = 0 if 'pressure_mb' not in data or data['pressure_mb'] == None or data['pressure_mb'] == 'NA' else float( data['pressure_mb'] )
        except ValueError:
            pressure_mb             = 0
        try:
            pressure_in             = 0 if 'pressure_in' not in data or data['pressure_in'] == None or data['pressure_in'] == 'NA' else float( data['pressure_in'] )
        except ValueError:
            pressure_in             = 0
        try:
            pressure_trend          = '' if 'pressure_trend' not in data or data['pressure_trend'] == None or data['pressure_trend'] == 'NA' else str( data['pressure_trend'] )
        except ValueError:
            pressure_trend          = ''
        try:
            dewpoint_string         = '' if 'dewpoint_string' not in data or data['dewpoint_string'] == None or data['dewpoint_string'] == 'NA' else str( data['dewpoint_string'] )
        except ValueError:
            dewpoint_string         = ''
        try:
            dewpoint_f              = 0 if 'dewpoint_f' not in data or data['dewpoint_f'] == None or data['dewpoint_f'] == 'NA' else float( data['dewpoint_f'] )
        except ValueError:
            dewpoint_f              = 0
        try:
            dewpoint_c              = 0 if 'dewpoint_c' not in data or data['dewpoint_c'] == None or data['dewpoint_c'] == 'NA' else float( data['dewpoint_c'] )
        except ValueError:
            dewpoint_c              = 0
        try:
            heat_index_string       = '' if 'heat_index_string' not in data or data['heat_index_string'] == None or data['heat_index_string'] == 'NA' else str( data['heat_index_string'] )
        except ValueError:
            heat_index_string       = ''
        try:
            heat_index_f            = 0 if 'heat_index_f' not in data or data['heat_index_f'] == None or data['heat_index_f'] == 'NA' else float( data['heat_index_f'] )
        except ValueError:
            heat_index_f            = 0
        try:
            heat_index_c            = 0 if 'heat_index_c' not in data or data['heat_index_c'] == None or data['heat_index_c'] == 'NA' else float( data['heat_index_c'] )
        except ValueError:
            heat_index_c            = 0
        try:
            windchill_string        = '' if 'windchill_string' not in data or data['windchill_string'] == None or data['windchill_string'] == 'NA' else str( data['windchill_string'] )
        except ValueError:
            windchill_string        = ''
        try:
            windchill_f             = 0 if 'windchill_f' not in data or data['windchill_f'] == None or data['windchill_f'] == 'NA' else float( data['windchill_f'] )
        except ValueError:
            windchill_f             = 0
        try:
            windchill_c             = 0 if 'windchill_c' not in data or data['windchill_c'] == None or data['windchill_c'] == 'NA' else float( data['windchill_c'] )
        except ValueError:
            windchill_c             = 0
        try:
            feelslike_string        = '' if 'feelslike_string' not in data or data['feelslike_string'] == None or data['feelslike_string'] == 'NA' else str( data['feelslike_string'] )
        except ValueError:
            feelslike_string        = ''
        try:
            feelslike_f             = 0 if 'feelslike_f' not in data or data['feelslike_f'] == None or data['feelslike_f'] == 'NA' else float( data['feelslike_f'] )
        except ValueError:
            feelslike_f             = 0
        try:
            feelslike_c             = 0 if 'feelslike_c' not in data or data['feelslike_c'] == None or data['feelslike_c'] == 'NA' else float( data['feelslike_c'] )
        except ValueError:
            feelslike_c             = 0
        try:
            visibility_mi           = 0 if 'visibility_mi' not in data or data['visibility_mi'] == None or data['visibility_mi'] == 'NA' else float( data['visibility_mi'] )
        except ValueError:
            visibility_mi           = 0
        try:
            visibility_km           = 0 if 'visibility_km' not in data or data['visibility_km'] == None or data['visibility_km'] == 'NA' else float( data['visibility_km'] )
        except ValueError:
            visibility_km           = 0
        try:
            precip_1hr_string       = '' if 'precip_1hr_string' not in data or data['precip_1hr_string'] == None or data['precip_1hr_string'] == 'NA' else str( data['precip_1hr_string'] )
        except ValueError:
            precip_1hr_string       = ''
        try:
            precip_1hr_in           = 0 if 'precip_1hr_in' not in data or data['precip_1hr_in'] == None or data['precip_1hr_in'] == 'NA' else float( data['precip_1hr_in'] )
        except ValueError:
            precip_1hr_in           = 0
        try:
            precip_1hr_metric       = 0 if 'precip_1hr_metric' not in data or data['precip_1hr_metric'] == None or data['precip_1hr_metric'] == 'NA' else float( data['precip_1hr_metric'] )
        except ValueError:
            precip_1hr_metric       = 0
        try:
            precip_today_string     = '' if 'precip_today_string' not in data or data['precip_today_string'] == None or data['precip_today_string'] == 'NA' else str( data['precip_today_string'] )
        except ValueError:
            precip_today_string     = ''
        try:
            precip_today_in         = 0 if 'precip_today_in' not in data or data['precip_today_in'] == None or data['precip_today_in'] == 'NA' else float( data['precip_today_in'] )
        except ValueError:
            precip_today_in         = 0
        try:
            precip_today_metric     = 0 if 'precip_today_metric' not in data or data['precip_today_metric'] == None or data['precip_today_metric'] == 'NA' else float( data['precip_today_metric'] )
        except ValueError:
            precip_today_metric     = 0
        try:
            icon                    = '' if 'icon' not in data or data['icon'] == None or data['icon'] == 'NA' else str( data['icon'] )
        except ValueError:
            icon                    = ''
        try:
            icon_url                = '' if 'icon_url' not in data or data['icon_url'] == None or data['icon_url'] == 'NA' else str( data['icon_url'] )
        except ValueError:
            icon_url = ''
        try:
            forecast_url            = '' if 'forecast_url' not in data or data['forecast_url'] == None or data['forecast_url'] == 'NA' else str( data['forecast_url'] )
        except ValueError:
            forecast_url            = ''
        try:
            history_url             = '' if 'history_url' not in data or data['history_url'] == None or data['history_url'] == 'NA' else str( data['history_url'] )
        except ValueError:
            history_url             = ''
        try:
            ob_url                  = '' if 'ob_url' not in data or data['ob_url'] == None or data['ob_url'] == 'NA' else str( data['ob_url'] )
        except ValueError:
            ob_url                  = ''

        self.dbc.execute('INSERT INTO weather.conditions (location_uid, image_url, image_title, image_link, weather, temperature_string, temp_f, temp_c, relative_humidity, wind_string, wind_dir, wind_degrees, wind_mph, wind_gust_mph, wind_kph, wind_gust_kph, pressure_mb, pressure_in, pressure_trend, dewpoint_string, dewpoint_f, dewpoint_c, heat_index_string, heat_index_f, heat_index_c, windchill_string, windchill_f, windchill_c, feelslike_string, feelslike_f, feelslike_c, visibility_mi, visibility_km, precip_1hr_string, precip_1hr_in, precip_1hr_metric, precip_today_string, precip_today_in, precip_today_metric, icon, icon_url, forecast_url, history_url, ob_url, created_at) values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() )', (location_uid, image_url, image_title, image_link, weather, temperature_string, temp_f, temp_c, relative_humidity, wind_string, wind_dir, wind_degrees, wind_mph, wind_gust_mph, wind_kph, wind_gust_kph, pressure_mb, pressure_in, pressure_trend, dewpoint_string, dewpoint_f, dewpoint_c, heat_index_string, heat_index_f, heat_index_c, windchill_string, windchill_f, windchill_c, feelslike_string, feelslike_f, feelslike_c, visibility_mi, visibility_km, precip_1hr_string, precip_1hr_in, precip_1hr_metric, precip_today_string, precip_today_in, precip_today_metric, icon, icon_url, forecast_url, history_url, ob_url))
        condition_uid = self.dbc.lastrowid
        self.db.commit()

        return condition_uid
