import json
from pathlib import Path
import pyodbc
import pandas as pd

import requests
from bs4 import BeautifulSoup

def ScrapeSite(url):

    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = response.content
    soup = BeautifulSoup(html, features="html.parser")
    
    #Array of the Data Values taken on the page
    dataValues = []

    table = soup.find('h1', attrs={'class':'CurrentConditions--location--1Ayv3'})
    text = table.text.replace('&nbsp', '')
    dataValues.append(text.replace(' Weather', '')) #Location

    table = soup.find('span', attrs={'data-testid':'TemperatureValue'})
    text = table.text.replace('&nbsp', '')
    dataValues.append(text) #CurrentTemperature

    table = soup.find('div', attrs={'data-testid':'wxPhrase'})
    dataValues.append(table.text.replace('&nbsp', '')) #Coverage

    try:
        table = soup.find('div', attrs={'data-testid':'precipPhrase'})
        text = table.text.replace('&nbsp', '')
        Percent = text.split("%")
        ChanceofPrecipitation = Percent[0] + '%'
    except:
        ChanceofPrecipitation = '0%'
    dataValues.append(ChanceofPrecipitation) #ChanceofPrecipitation

    table = soup.find('div', attrs={'class':'TodayDetailsCard--detailsContainer--1tqay'})
    for x in table.findAll('div'):
        try:
            stuff = x.find('div', attrs={'data-testid':'wxData'})
            text = stuff.text.replace('&nbsp', '')
            dataValues.append(text)
        except:
            pass
    return(dataValues)


def inputSQL(dataValues):
    #SQL Server Inputs
    
    hilo = dataValues[4].split('/')

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};" "Server=ALEX-PC;" "Database=WeatherDB;" "Trusted_Connection=yes;")
    cursor = cnxn.cursor()

    cursor.execute("SELECT Location FROM WeatherDB.dbo.Weather")

    results = cursor.fetchall()
    sql = ''
    for x in results:
        if(dataValues[0] in x):
            sql = '''UPDATE WeatherDB.dbo.Weather SET Location = '{0}', [Current Temperature] = '{1}', Coverage = '{2}', [Chance of Rain] = '{3}', [Today's High] = '{4}', [Today's Low] = '{5}', [Wind Speed] = '{6}', Humidity = '{7}', [Dew Point] = '{8}', Pressure = '{9}', [UV Index] = '{10}', Visibility = '{11}', [Moon Phase] = '{12}', [Time Stamp] = GETDATE() WHERE Location = '{0}' '''.format(dataValues[0], dataValues[1], dataValues[2], dataValues[3], hilo[0], hilo[1], dataValues[5], dataValues[6], dataValues[7], dataValues[8], dataValues[9], dataValues[10], dataValues[11])
            break
        else:
            sql = '''INSERT INTO WeatherDB.dbo.Weather (Location, [Current Temperature], Coverage, [Chance of Rain], [Today's High], [Today's Low], [Wind Speed], Humidity, [Dew Point], Pressure, [UV Index], Visibility, [Moon Phase], [Time Stamp]) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', GETDATE())'''.format(dataValues[0], dataValues[1], dataValues[2], dataValues[3], hilo[0], hilo[1], dataValues[5], dataValues[6], dataValues[7], dataValues[8], dataValues[9], dataValues[10], dataValues[11])

    cursor.execute(sql)

    cnxn.commit()
    #print('Data is placed in DataBase')
    



def startScraping():
       
     for cityName in configData:
         inputSQL(ScrapeSite(configData[cityName]))

if __name__ == "__main__":
    
    configPath = Path("C:\\Users\\Alex\\Documents\\Code\\Web_Scraper")
    configPathName = configPath / "urls.config"
    with open(configPathName, "r") as configFile:
        configData = json.load(configFile)

    startScraping()

    

