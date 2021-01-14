import datetime as dt
import mysql.connector
from mysql.connector import errorcode
import urllib.request
import json
from datetime import datetime
import time as t

class controller:
    @staticmethod
    def connect_database():
        global connection
        print("Establishing connection to database...")
        try:
            connection = mysql.connector.connect(
                host="",
                port="",
                user="",
                password="",
                database="", )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        return connection

    # https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html

    @staticmethod
    def connect_api_forecast(postcode):
        # api request and parse
        print(postcode+": Making API request")
        url = 'http://api.weatherapi.com/v1/forecast.json?key=6f612f83195d43da90d115159201412&q='+postcode+'&days=3'
        try:
            request = urllib.request.Request(url)
            r = urllib.request.urlopen(request).read()
            content = json.loads(r.decode('utf-8'))
            print(postcode+": Successful request")
            return content
        except errorcode as e:
            print(e)
            print(postcode+ + urllib.error.HTTPError)
            print(postcode+": Successful request")
            return


    @staticmethod
    def connect_api_historical():
        # api request and parse
        print("HISTORICAL: Making API request")
        date = (datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")

        url = 'http://api.weatherapi.com/v1/history.json?key=6f612f83195d43da90d115159201412&q=NE426BE&dt=' + date
        try:
            request = urllib.request.Request(url)
            r = urllib.request.urlopen(request).read()
            content = json.loads(r.decode('utf-8'))
            print("HISTORICAL: Successful request")
            print('NOTE: Historical data not from actuals')
            return content
        except errorcode as e:
            print(e)
            print("HISTORICAL" + urllib.error.HTTPErro)
            return

    @staticmethod
    def calc_seconds(hours, minutes, seconds):
        h = hours * 3600
        m = minutes * 60
        seconds = h + m + seconds
        return seconds


    @staticmethod
    def parse_json(table_name, content, connection):
        start_time = t.time()
        print("Parsing json, passing to insertion method...")

        for item in content['forecast']['forecastday']:
            for item in item['hour']:
                temp_c = item['temp_c']
                wind_kph = item['wind_kph']
                wind_degree = item['wind_degree']
                wind_dir = item['wind_dir']
                pressure_mb = item['pressure_mb']
                precip_mm = item['precip_mm']
                humidity = item['humidity']
                cloud = item['cloud']
                feelslike_c = item['feelslike_c']
                windchill_c = item['windchill_c']
                heatindex_c = item['heatindex_c']
                dewpoint_c = item['dewpoint_c']
                will_it_rain = item['will_it_rain']
                chance_of_rain = item['chance_of_rain']
                will_it_snow = item['will_it_snow']
                chance_of_snow = item['chance_of_snow']
                vis_miles = item['vis_miles']
                gust_mph = item['gust_mph']
                time = item['time']
                stamp = datetime.strptime(time, "%Y-%m-%d %H:%M")
                time = stamp.time()
                hours = time.hour
                minutes = time.minute
                seconds = time.second
                time = controller.calc_seconds(hours, minutes, seconds)
                date = stamp.date().strftime("%Y%m%d")

                #controller.run_historical_insertion(table_name, connection, date, time, temp_c, wind_kph, wind_degree, wind_dir,
                                                       #pressure_mb,
                                                       #precip_mm, humidity, cloud, feelslike_c, windchill_c,
                                                       #heatindex_c, dewpoint_c,
                                                       #will_it_rain, chance_of_rain, will_it_snow, chance_of_snow,
                                                       #vis_miles,
                                                       #gust_mph)

                controller.run_forecast_insertion(table_name, connection, date, time, temp_c, wind_kph, wind_degree, wind_dir,
                                                     pressure_mb, precip_mm, humidity, cloud, feelslike_c, windchill_c,
                                                     heatindex_c, dewpoint_c, will_it_rain, chance_of_rain,
                                                     will_it_snow, chance_of_snow, vis_miles, gust_mph)
        print("PARSE TIME: --- %s seconds ---" % (t.time() - start_time))



    @staticmethod
    def run_historical_insertion(table_name, connection, date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb,
                                 precip_mm,
                                 humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain,
                                 chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph):



        insert = "INSERT INTO "+table_name+" (date, time, temp_c, wind_kph, wind_degree,  wind_dir, pressure_mb, precip_mm, humidity, cloud, " \
                 "feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, " \
                 "chance_of_snow, vis_miles, gust_mph) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

        val = date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb, precip_mm, humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph
        cursor = connection.cursor()

        try:
            cursor.execute(insert, val)
        except:
            return False

        connection.commit()


    @staticmethod
    def run_forecast_insertion(table_name, connection, date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb, precip_mm,
                               humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain,
                               chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph):

        cursor = connection.cursor()
        delete = 'DELETE FROM Weather.'+table_name+' WHERE date = ' + str(date) + ' AND time = ' + str(time) + ';'


        insert = "INSERT INTO Weather."+table_name+" (date, time, temp_c, wind_kph, wind_degree,  wind_dir, pressure_mb, precip_mm, humidity, cloud, " \
                 "feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, " \
                 "chance_of_snow, vis_miles, gust_mph) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

        val = date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb, precip_mm, humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph
        cursor.execute(delete)
        cursor.execute(insert, val)

        try:
            cursor.execute(delete)
            cursor.execute(insert, val)
        except errorcode as e:
            print(e)
            return False

        connection.commit()

    @staticmethod
    def get_fields(connection):
         cursor = connection.cursor()
         request = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'NE426BE\' ORDER BY ORDINAL_POSITION'
         cursor.execute(request)
         result = cursor.fetchall()
         metrics = []
         for item in result:
             metrics.append(item)
         return metrics

    #def get_all(connection):
        #cursor = connection.cursor()
        #request = 'SELECT * FROM weather_future;'
        #cursor.execute(request)
        #result = cursor.fetchall()
        #return result


