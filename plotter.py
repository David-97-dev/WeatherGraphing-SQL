import matplotlib.pyplot as plt

class plotter:
    def calc_seconds(hours, minutes, seconds):
        h = hours * 3600
        m = minutes * 60
        seconds = h + m + seconds
        return seconds

    def run_query(connection, query, x, y):
        # runs quieries, unpacks data and returns lists
        print("Running query...")
        cursor = connection.cursor()

        try :
            cursor.execute(query)
        except:
            print("Invalid")


        data = cursor.fetchall()
        x, y = zip(*data)

        tempx = []
        for item in x:
            tempx.append(item)

        tempy = []
        for item in y:
            tempy.append(item)

        return tempx, tempy
        #eturn x, y

    def format_seconds(tempx):
        tempx_formatted = []
        i = 0
        for item in tempx:
            time = int(tempx[i])
            time = int(time / 3600)
            tempx_formatted.append(time)
            i = i + 1
        return tempx_formatted

    def plot(noq, title, titlex, titley, tempx, tempy):
        # noq = number of queries/figure number
        #plt.ion()
        plt.xticks(plotter.format_seconds(tempx))
        #plt.xticks()
        plt.gcf().autofmt_xdate()
        plt.style.use('ggplot')
        plt.xlabel(titlex)
        plt.ylabel(titley)
        plt.title(title)
        # plt.title(str((datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")))
        plt.legend()
        plt.grid(True)
        plt.plot(plotter.format_seconds(tempx), tempy)
        #plt.plot(tempx, tempy)
        plt.figure(noq)
        plt.savefig('test')
        # plt.tight_layout()

    def build_query(connection, x, y, table, where, title, noq):
        titlex = str(x)
        titley = str(y)
        query = 'SELECT ' + x + ', ' + y + ' FROM ' + table + ' ' + where + ';'
        tempx, tempy = plotter.run_query(connection, query, x, y)
        plotter.plot(noq, title, titlex, titley, tempx, tempy)
        print('Graph: ' + title + ' RAN')

        # forecast query
        # forecast_query = "SELECT A.time, A.temp_c FROM Weather.weather_future A WHERE A.date = 20201217;"
        # title = 'Three day forecast'
        # tempx, tempy = plotter.run_query(connection, forecast_query, x='time', y='temp_c')
        # plotter.plot(2, title, tempx, tempy)
