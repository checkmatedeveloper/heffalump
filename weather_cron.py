import urllib2
import json
from pprint import pprint
from db_connection import DbConnection
from weather_db import WeatherDb

baseUrl = "http://api.wunderground.com/api"
apiKey = "0f83826b9d9a6512"
conditionsEndpoint = "conditions/q"
format = ".json"
db = DbConnection().connection
weatherDb = WeatherDb(db)

#load in the locations from the database
locations =  weatherDb.getAllLocations()

print locations

for location in locations:

    #print "location"
    #print location

    #collect the previous weather conditions 
    previousWeather = weatherDb.getLastWeatherConditionByLocation(location['location_uid']) 
    

    #get the current weather from weather underground
    

    stringResponse = urllib2.urlopen(baseUrl + "/" + apiKey + "/" + conditionsEndpoint + "/" + str(location['zipcode']).zfill(5) + format).read()
    
    #print stringResponse   
 
    currentWeather = dict()
    try:
        jsonData = json.loads(stringResponse)
    except ValueError:
        #continue
        pass

    weatherData =  jsonData['current_observation']
    
    currentWeather = dict()
    currentWeather["temp_f"] = weatherData["temp_f"]
    currentWeather["weather"] = str(weatherData["weather"])
    currentWeather["wind_dir"] = str(weatherData["wind_dir"])
    currentWeather["wind_string"] = str(weatherData["wind_string"])
    currentWeather["temperature_string"] = str(weatherData["temperature_string"])
    currentWeather["wind_mph"] = weatherData["wind_mph"]
    
    #print "current weather"
    #print currentWeather

    weatherData['location_uid'] = location['location_uid']    
   
    print weatherData

    #currentWeather["temp_f"] = "58.9"
    #print previousWeather
    #print currentWeather

    #if cmp(previousWeather, currentWeather) != 0:
    if True:
        #print "new data, update redis"
        weatherDb.updateCachedLocationWeatherCondition(weatherData)
    else:
        #print "same weather data, don't update redis"
        pass

    id = weatherDb.addWeatherCondition(weatherData)
    #print id

