import matplotlib.pyplot as plt
from controller import controller
import time
from multiprocessing import Process
#from os import system, name

from plotter import plotter


def connect_to_db():
    # Connecting to database
    connection = controller.connect_database()
    return connection


def run_api(connection):
    print("HANDLING DATA ---------")
    # Connecting to API
    forecast_content = controller.connect_api_forecast('NE426BE')  # forecast api
    #historical_content = controller.connect_api_historical('NE426BE')
    # Parsing
    controller.parse_json('NE426BE', forecast_content, connection)  # forecast parse
    #controller.parse_json(historical_content, connection)
    #DADS HOUSE
    forecast_content = controller.connect_api_forecast('NE392DY')  # forecast api
    #historical_content = controller.connect_api_historical('NE392DY')
    # Parsing
    controller.parse_json('NE392DY', forecast_content, connection)  # forecast parse
    #controller.parse_json(historical_content, connection)
    return connection


def user_input_query(connection, x, y, table, where, title):
    noq = 1
    plotter.build_query(connection, x, y, table, where, title, noq)
    p = Process(target=plt.show())
    interface(connection)
    p.start()
    p.join()

def menu():
    print('c - reconnect to database')
    print('a - update database')
    print('q - Enter Query')
    print('s - Database information')
    print('x - close db connection')
    print('l - logout')


def interface(connection):
    entry = True

    while entry:
        if connection.is_connected():
            print('CONNECTED TO: ' + connection.server_host)
        elif not connection.is_connected():
            print('NO CONNECTION TO DATABASE')
        menu()
        value = input("Enter option:\n")

        if value == 'c':
            try:
                connection = connect_to_db()
            except:
                print("Check user details, or internet connection")
        elif value == 'a':
            if connection.is_connected():
                try:
                    run_api(connection)
                except:
                    print("Something went wrong, check connection to database")


        elif value == 'q':
            if connection.is_connected():
                    print('PRINTING METRICS')

                    metrics = []
                    for item in controller.get_fields(connection):
                        metrics.append(item)
                    print(metrics)


                    print("NE426BE or NE392DY")
                    table = input("Enter table name:\n")
                    x = input("Enter metric for X:\n")
                    #if x not in metrics:
                        #print('No such metric called ' + str(x))

                    y = input("Enter metric for Y:\n")
                    where = input("Enter WHERE clause (if none, do not enter value, press enter instead):\n")
                    title = 'X: '+x +' Y: ' + y + ' for ' + table
                    print('YOUR QUERY: ' + 'SELECT ' + x + ', ' + y + ' FROM ' + table + ' ' + where + ';')
                    print('Would you like to run query?')
                    value = input('y/n\n')
                    if value == 'y':

                        start_time = time.time()
                        user_input_query(connection, x, y, table, where, title)
                        print("--- %s seconds ---" % (time.time() - start_time))

                    elif value != 'n':
                        print('Would you like to run query?')
                        value = input('y/n\n')


                    elif value == 'n':
                        return

            else:
                print("You must connect to data services first\n")
        elif value == 's':
            if connection.is_connected():
                print('IP: ' + connection.server_host)
                print('DATABASE: ' + connection.database)
                print('USERNAME: ' + connection.user)
                print(connection.get_server_info())
                print('WARNINGS: ' + str(connection.get_warnings.conjugate()))
                print('---------')
       # elif value == 't':
            #l = controller.get_all(connection)
            #print( "HEADER1       HEADER2    HEADER3")
            #for ele1, ele2, ele3 in l:
                #print
                #"{:<14}{:<11}{}".format(ele1, ele2, ele3)



        elif value == 'x':
            print('Disconnecting from ' + connection.server_host)
            connection.disconnect()

        elif value == 'l':
            print("Exiting program")
            entry = False
        else:
            print("Invalid entry")


def check_username(connection,user):
    try:
        connection.cursor()
    except:
        print('Services down')
        login_menu(connection)

    cursor = connection.cursor()
    query = 'SELECT COUNT(1) FROM users WHERE username = ' + '\'' + user + '\'' + ';'
    cursor.execute(query)
    value = str(cursor.fetchone())

    if value == '(1,)':
        return 1
    elif value == '(0,)':
        return 0




def check_password(connection,user, pword):
    try:
        cursor = connection.cursor()
    except:
        print('Services down')
        login_menu(connection)

    query = 'SELECT COUNT(1) FROM users WHERE username = ' + '\'' + user + '\'' + ' AND password = md5(' + '\'' + pword + '\')' + ';'
    cursor.execute(query)
    value = str(cursor.fetchone())

    if value == '(1,)':
        return 1

    elif value == '(0,)':
        return 0

def check_email(connection,email):
    cursor = connection.cursor()
    query = 'SELECT COUNT(1) FROM users WHERE email_address = ' + '\'' + email + '\'' + ';'
    cursor.execute(query)
    value = str(cursor.fetchone())

    if value == '(1,)':
        return 1

    elif value == '(0,)':
        return 0


def login(connection):
    i = 1
    login = True
    while login:
        if i <= 3:
            user = input("Enter username\n")
            user_value = check_username(connection, user)
            if user_value == 1 or user_value == 0:
                pword = input("Enter password\n")
                password_value = check_password(connection, user, pword)
                if password_value == 1 and user_value == 1 :
                    return 1
                else:
                    print('Details incorrect')
                    i = i + 1

        else:
            login_menu(connection)


def create_account(connection, username, email_address, password):
    cursor = connection.cursor()
    insert = "INSERT INTO users (username, email_address, password) VALUES (%s, %s, md5(%s)) "
    val = username, email_address, password
    try:
        cursor.execute(insert, val)
        #create = "CREATE "
        #cursor.execute(create)
    except:
        return False

    connection.commit()


def signup(connection):
    print('EMAIL ADDRESSES AND PASSWORDS SECURELY STORED')
    email_check = True
    while email_check:
        email = input("Enter email address\n")
        if check_email(connection, email) == 1:
            print('Email already associated with an account')
        else:
            username = input("Enter new username\n")
            username_check = True
            while username_check:
                if check_username(connection, username) == 1:
                    print('Username unavailable')
                else:
                    pass_check = True
                    while pass_check:
                        password = input("Enter password\n")
                        password_two = input("Please re-enter password\n")
                        if password == password_two:
                            create_account(connection, username, email, password)
                            interface(connection)
                            return
                        else:
                            print('Passwords do not match')


def login_menu(connection):
    print("Login with an existing account or sign up")
    print("l - Login")
    print('s - sign up')
    print('i - info')
    value = input()
    if value == 'l':
        if login(connection) == 1:
            interface(connection)
        elif login(connection) == 0:
            print('Unable to login')
    if value == 's':
        signup(connection)
    if value == 'i':
        print('''
        This software allows the user to generate graphs by selecting from a set of metrics for both x and y. 
        The data is stored on a database, and can be manually updated.
        In the future, this software will allow the user to connect to pre-built, custom databases, and generate
        graphs with ease.
        This is currently in development. Any bugs or issues, please report them to davidabell97@gmail.com.
        Passwords are securely stored, and details are not used, or sold to any third party organisations.
        They are solely used for this application only.
        Produced by David Bell in 2020.
        Last updated 19/12/2020.\n''')




if __name__ == "__main__":

    connecting = True
    while connecting:
        try:
            connection = connect_to_db()

            connecting = False
        except:
            connection = None
            print('Unable to connect to services, please contact davidabell97@gmail.com if issue persists')
            time.sleep(60)

    interface(connection)

    login_menu(connection)

    if login_menu(connection) == 1:
        print('Weather Data powered by weatherapi.com')
        interface(connection)
