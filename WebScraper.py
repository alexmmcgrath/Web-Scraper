from apscheduler.schedulers.blocking import BlockingScheduler

import pyodbc
import pandas as pd

import requests
from bs4 import BeautifulSoup

urlLocations = ['https://weather.com/weather/today/l/7472a7bbd3a7454aadf596f0ba7dc8b08987b1f7581fae69d8817dffffc487c2', 
'https://weather.com/weather/today/l/adb0e8d623e0a374dcf4ad2090ba0822e90f44018fc2e62fe70dacc200aa349c', 
'https://weather.com/weather/today/l/70dc4754d9ba0a6e8e980e9e64d0e0be9545bed75adc7b4f9a0148430ae92901', 
'https://weather.com/weather/today/l/729d869b5da7e1b89835d0de7940cda8b0b2e0dafda4ef6f742b558409755f66',  
'https://weather.com/weather/today/l/ced0de18c1d771856e6012f3abf0a952cfe22952e72e516e6e098d54ca737114', 
'https://weather.com/weather/today/l/f892433d7660da170347398eb8e3d722d8d362fe7dd15af16ce88324e1b96e70',
'https://weather.com/weather/today/l/0e4d3978a19f3f71985bc6a7754c67f35f5fbf4646d75ce84928847f74cce22f',
'https://weather.com/weather/today/l/1f922c331c324da7196d0046d5c87bcf6058372b93e6c8f289e093dfe020a2a9',
'https://weather.com/weather/today/l/d0dcbf29e9198a4a60c03897b069e4043fb5d083a05073e382460668bef0118d',
'https://weather.com/weather/today/l/9754ddb3459918006e10487e257a892ecedcd6c9e4b8e9bc17e95e9369b14490',
'https://weather.com/weather/today/l/b5a52d7f2684049e75cbf7c2ed09133bae18a821a6abef0cd89f67ccf5005d3a',
'https://weather.com/weather/today/l/d1be3e5aec1726d0df2d6c19f21655d886415ee60ff0e8f14afe8ff7f57c9e5d',
'https://weather.com/weather/today/l/675c2b6342b3512ea4f15bc9070663be6e36cc4bf61056076c500098c8eb3bbe',
'https://weather.com/weather/today/l/e8c1ff368fe9d3318e00befeca3270d5715b6ea8c6910f9d00b08e136593bcc7',
'https://weather.com/weather/today/l/90a31c4554cf9417fc99dc7e0d40ac75799377e240640347cd7e217aeaa17cc4',
'https://weather.com/weather/today/l/6abe63425d4afc49f9536405077c86db1e468bdb6916f2934e933fd43f7c45af',
'https://weather.com/weather/today/l/8f5c8f0c5dd7699966caf7dcb4880c3847a86092521a187546b808f3023b9120',
'https://weather.com/weather/today/l/263f49e812cfbd2c5812f4c3f3dec5eb89d625bb80fab3d024ed4d3a10220f32',
'https://weather.com/weather/today/l/f3ee587cce023b135305cd84e12e67e8d6a20831c6487ac41bcaf1b7ed91c5d4',
'https://weather.com/weather/today/l/0b1d231bcc2e2ad1340fa2c988cafaa840c3e4717404c7d19afa8d938dd2aad4',
'https://weather.com/weather/today/l/60726d811b7e36432583ede41c4600b07b8b2e94c237fa8c6c2a9085a511d43a',
'https://weather.com/weather/today/l/9f4008162d433e9dd2584077ace40c9fc72765c052152218f346dc729206e589',
'https://weather.com/weather/today/l/bc640feecc85fc2d5ccf67968ad8cd52262be8813efb6592b1e592e6f6760fce',
'https://weather.com/weather/today/l/63b2dfd6f61b9599003b18c3dc870cf67f11348d5e3ecbfcb1c7491ac92bdf16',
'https://weather.com/weather/today/l/1af7c6ae2faf3c56edd050d40869d392ba49b74e559687d271b67c9c4968e378',
'https://weather.com/weather/today/l/28bbf592e5da462e28b5fc7cf97e802f2a7a4eabc09d45d8d9bcd26ac2f3a600',
'https://weather.com/weather/today/l/362768b764fc93d07642eedc3dadd2a155206a13e36685d22d0c807cc824740f',
'https://weather.com/weather/today/l/bf217d537cc1c8074ec195ce07fb74de3c1593caa6033b7c3be4645ccc5b01de',
'https://weather.com/weather/today/l/a0c7694d6c51e53bcf16e4259855a5db57dbcf729423678e8b7711125045f750',
'https://weather.com/weather/today/l/d7c764530fccd95c3ca8a0ea8a53a4480cfdca652526eb9f1aa78738ed9ab7cf',
'https://weather.com/weather/today/l/1d5c3918279823e2fda20386b4277cfd84684ce11aa88bdaa1c1d754d111f368',
'https://weather.com/weather/today/l/e60256f3426acd3c1b3380921210fcffeab628a120c41d1de03b59a0f0dd32ad',
'https://weather.com/weather/today/l/493e463e9189a4d1b57fa34bbdad1b1bf048f2d4f9c86c26a84675cc2b3b3461',
'https://weather.com/weather/today/l/6611796588aaff32d5068ab12cd10ff73d95e9ba2c67f08308761837113cdc30',
'https://weather.com/weather/today/l/5070c75f6a2b1bc2c2a09ea342a9dff4a333163fca9863e910a244baaa416716',
'https://weather.com/weather/today/l/3b7ae74f5b0cd50daf16c234e487aa76668973a7f3bd06b5b5333d08ed378e39',
'https://weather.com/weather/today/l/b63d264adac46ae5b0a10a6487301ec6a95a594e787021a71b0ed983db8e8733',
'https://weather.com/weather/today/l/8ca319ca9432d13f848a3a77f4f1794679c568e2faeb1f035e767b221c303115',
'https://weather.com/weather/today/l/7bbd7a4856b00f09882de212c28a46d471565d62f3417dd6da1a4a43b7643211',
'https://weather.com/weather/today/l/06ab73392aefd4663eb8ccaf06f9b33b3cf2c206f9497b4d3f9193614fd307b9',
'https://weather.com/weather/today/l/f6438217ba86311e46dfd34580ab9b747fd69448276e6c17f5626a5ca0b6ee7c',
'https://weather.com/weather/today/l/f0e08ec061e8a5ec6017b6338adffb031304e10ed396670da5c5fbe9838cd9f1',
'https://weather.com/weather/today/l/e88077a24ffb004ce75620f70b56ebd5658872eb12b44f971df9985d3762163a',
'https://weather.com/weather/today/l/76c59d344b0b1ca1b0db128c40983e239e6b11a3a3383a293b3bf897a7572d86',
'https://weather.com/weather/today/l/2c16e765c74fc31b89e5a60bc48144005ff015618146ff614be3b77a0625fb55',
'https://weather.com/weather/today/l/8c9269c7e7ff904bab89e390b16e530be0b9ac60a5a338dc66d11a4e35d6c450',
'https://weather.com/weather/today/l/530205dd02fd2306ee5e01ddcbe57695026f2738f020a799c6a52551729716b9',
'https://weather.com/weather/today/l/f0914aae5b7eee388d889d38601b97d2b4e827243a270ca3183bde6ec2d12efc',
'https://weather.com/weather/today/l/c497a8fe783a21075e4be0fe8e3851415b88cb2e30a6fa184550e22a7ae728c6',
'https://weather.com/weather/today/l/b565aa4d4c1111a09ad8c1ede054636671f50d38757a6527b35d96b987ef86a8',
'https://weather.com/weather/today/l/ada030b2de8db495da2d93d5a2ecf30de1ce8b54cb09725d19c803543685646d',
'https://weather.com/weather/today/l/aef658554f18d5483fc20bdacd1397d27a895e91220dc5631280b045c62f8cf4',
'https://weather.com/weather/today/l/5aea1d50a6d6b9e99cf89ba79f463d67dcf21ea5061990aae1ffc1c7fa8911a9',
'https://weather.com/weather/today/l/18f1dacd8054490ab7ec373384b93b70eae92d66cd9bdad0e4fe5ef2b1fb4ff6',
'https://weather.com/weather/today/l/578c23d55e0637e109570d3f6e83ff9ebec6fd69a4e88877fcf6969c27706545',
'https://weather.com/weather/today/l/b49a92f375f5792cf5a9f0a9e90c7b900a2cc46603c11ad2989876fe7e9d130e'
]
"""
Austin
Rochester
Syracuse
Phoenix
Seattle
New York City
Buffalo NY
Killeen TX
Montgomery AL
Juneau AK
Little Rock AR
Sacramento CA
Denver CO
Hartford CT
Dover, DE
Tallahassee, FL
Atlanta, GA
Honolulu, HI
Boise, ID
Springfield, 
Indianapolis
Des Moines
Topeka KS
Frankfort KY
Baton Rouge
STATES IN ALPHA ORDER
"""
def ScrapeSite(url):

    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = response.content
    soup = BeautifulSoup(html, features="html.parser")
    
    #Array of the Data Values taken on the page
    dataValues = []

    table = soup.find('h1', attrs={'class':'_-_-node_modules-@wxu-components-src-organism-CurrentConditions-CurrentConditions--location--1Ayv3'})
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

    table = soup.find('div', attrs={'class':'_-_-node_modules-@wxu-components-src-organism-TodayDetailsCard-TodayDetailsCard--detailsContainer--1tqay'})
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
    print('Data is placed in DataBase')
    



def startScraping():   
    for urls in urlLocations:
        inputSQL(ScrapeSite(urls))

if __name__ == "__main__":
    """
    scheduler = BlockingScheduler()
    scheduler.add_job(startScraping, 'interval', minutes=1)
    
    try:
        scheduler.start()
    except:
        print('In the Exception')
    """
    startScraping()

    

