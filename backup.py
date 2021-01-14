# 6f612f83195d43da90d115159201412 #weather api
from datetime import datetime
import datetime as dt
import mysql.connector
from mysql.connector import errorcode
import urllib.request
import json
import matplotlib.pyplot as plt

# connection to sql database
def connect_database():
    print("Establishing connection to db...")
    try:
        connection = mysql.connector.connect(
            host="192.168.1.216",
            port="3307",
            user="davidpc",
            password="Joseph_2020",
            database="Weather", )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    return connection
#https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html


def connect_api_forecast():
    # api request and parse
    print("FORECAST: Making API request")
    url = 'http://api.weatherapi.com/v1/forecast.json?key=6f612f83195d43da90d115159201412&q=NE426BE&days=3'
    try:
        request = urllib.request.Request(url)
        r = urllib.request.urlopen(request).read()
        content = json.loads(r.decode('utf-8'))
        print("FORECAST: Successful request")
        return content
    except:
        print("FORECAST" + urllib.error.HTTPError)
        print("FORECAST: Issue with connection")
        return

def connect_api_historical():
    # api request and parse
    print("HISTORICAL: Making API request")
    date = (datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")

    url = 'http://api.weatherapi.com/v1/history.json?key=6f612f83195d43da90d115159201412&q=NE426BE&dt='+ date
    try:
        request = urllib.request.Request(url)
        r = urllib.request.urlopen(request).read()
        content = json.loads(r.decode('utf-8'))
        print("HISTORICAL: Successful request")
        return content
    except:
        print("HISTORICAL" + urllib.error.HTTPErro)
        return





def calc_seconds(hours, minutes, seconds):
    h = hours * 3600
    m = minutes * 60
    seconds = h + m + seconds
    return seconds


def parse_json_forecast(content, connection):
    print("FORECAST: Parsing json, passing to insertion method...")

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
            time = calc_seconds(hours, minutes, seconds)
            date = stamp.date().strftime("%Y%m%d")

            if run_forecast_insertion(connection, date, time, temp_c, wind_kph, wind_degree, wind_dir,pressure_mb, precip_mm, humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph) == False:
                print("FORECAST: Insertion returned error")
                return



def run_forecast_insertion(connection, date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb,  precip_mm, humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph):

    cursor = connection.cursor()
    delete = 'DELETE FROM Weather.weather_future WHERE date = '+str(date)+' AND time = '+str(time)+';'
    cursor.execute(delete)
    #update = 'If(SELECT W.date, W.time from Weather.weather_future WHERE W.date =' + str(date) + ' AND W.time='+ str(time) + ') UPDATE Weather.weather_future SET W.date ='+ str(date) + ', W.time='+str(time)+' WHERE W.date ='+ str(date)+ ' AND W.time='+ str(time)+' ELSE INSERT INTO weather_future (date, time, temp_c, wind_kph, wind_degree,  wind_dir, pressure_mb, precip_mm, humidity, cloud,feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow,chance_of_snow, vis_miles, gust_mph) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    #update = 'DELETE weather_future WHERE date= '+str(date)+' AND time= ' + str(time)+' INSERT INTO weather_future (date, time, temp_c, wind_kph, wind_degree,  wind_dir, pressure_mb, precip_mm, humidity, cloud,feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow,chance_of_snow, vis_miles, gust_mph) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

    insert = "INSERT INTO weather_future (date, time, temp_c, wind_kph, wind_degree,  wind_dir, pressure_mb, precip_mm, humidity, cloud, " \
             "feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, " \
             "chance_of_snow, vis_miles, gust_mph) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

    val = date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb, precip_mm, humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph

    cursor.execute(insert, val)
    connection.commit()


def parse_json_historical(content, connection):
    print("HISTORICAL: Parsing json, passing to insertion method...")

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
            time = calc_seconds(hours, minutes, seconds)
            date = stamp.date().strftime("%Y%m%d")

            if run_historical_insertion(connection, date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb,
                                      precip_mm, humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c,
                                      will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_miles,
                                      gust_mph) == False:
                print("HISTORICAL: SQL Error Message")
                return


def run_historical_insertion(connection, date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb, precip_mm,
                           humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain,
                           chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph):
    insert = "INSERT INTO weather_historic (date, time, temp_c, wind_kph, wind_degree,  wind_dir, pressure_mb, precip_mm, humidity, cloud, " \
             "feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, " \
             "chance_of_snow, vis_miles, gust_mph) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

    val = date, time, temp_c, wind_kph, wind_degree, wind_dir, pressure_mb, precip_mm, humidity, cloud, feelslike_c, windchill_c, heatindex_c, dewpoint_c, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_miles, gust_mph
    cursor = connection.cursor()

    try:
        cursor.execute(insert, val)
    except:
        return False

    connection.commit()


def run_query(connection, query, x, y):
    print("Running query...")
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    x, y = zip(*data)

    tempx = []
    for item in x:
        tempx.append(item)

    tempy = []
    for item in y:
        tempy.append(item)

    return tempx, tempy

def format_seconds(tempx):
    tempx_formatted = []
    i = 0
    for item in tempx:
        time = int(tempx[i])
        time = int(time / 3600)
        tempx_formatted.append(time)
        i = i + 1
    return tempx_formatted

def plot(j, title, tempx, tempy):
    print("Plotting graph...")




    plt.xticks(format_seconds(tempx))
    plt.gcf().autofmt_xdate()

    plt.style.use('ggplot')

    plt.xlabel("Hour")
    #plt.ylabel("Max temp degrees Celsius")
    plt.title(title)
    #plt.title(str((datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")))
    plt.legend()

    plt.grid(True)

    plt.figure(j)
    plt.plot(format_seconds(tempx), tempy)

    #plt.tight_layout()


def queries(connection):

    new_query = "SELECT A.time, A.wind_kph FROM Weather.weather_future A;"
    title = 'TEST QUERY'
    tempx, tempy = run_query(connection, new_query, x='time', y='wind_kph')
    plot(1, title, tempx, tempy)
    tempx = None
    tempy = None


    # historic query
    history_query = "SELECT A.time, A.temp_c FROM Weather.weather_historic A WHERE A.date = 20201214;"
    title = 'Historic forecast'
    tempx, tempy = run_query(connection, history_query, x='time', y='temp_c')
    plot(2, title, tempx, tempy)
    tempx = None
    tempy = None

    # forecast query
    forecast_query = "SELECT A.time, A.temp_c FROM Weather.weather_future A WHERE A.date = 20201217;"
    title = 'Three day forecast'
    tempx, tempy = run_query(connection, forecast_query, x='time', y='temp_c')
    plot(3, title, tempx, tempy)
    tempx = None
    tempy = None



def main():
    connection = connect_database()
    forecast_content = connect_api_forecast() # forecast api
    parse_json_forecast(forecast_content, connection) #forecast parse
    historical_content = connect_api_historical()
    parse_json_historical(historical_content, connection)
    queries(connection)
    plt.show()

if __name__ == "__main__":
    main()
