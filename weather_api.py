import datetime as dt
import requests

def kelvin_to_celsius(kelvin):
    ceslsius= kelvin-273.15
    return ceslsius

def get_data(city):
    BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"  
    API_KEY = open('Weather_api_key.txt', 'r').read().strip() 
    CITY = city
    url = BASE_URL + "appid=" + API_KEY + "&q=" + CITY
    response = requests.get(url)
    if response.status_code == 200:
       data = response.json()  # Parse the JSON response
    else:
       print(f"Error: {response.status_code}")

    temp_celsius=kelvin_to_celsius(data['main']['temp'])
    feels_like_celsius= kelvin_to_celsius(data['main']['feels_like'])
    wind_speed=data['wind']['speed']
    humidity=data['main']['humidity']
    desciption= data['weather'][0]['description']
    sunrise_time=dt.datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time=dt.datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    return {"city_name":CITY,"temp_celsius":f"{temp_celsius:.2f}","feels_like_celsius":f"{feels_like_celsius:.2f}","wind_speed":f"{wind_speed:.2f}","humidity":humidity,"desciption":desciption,"sunrise_time":sunrise_time.strftime("%Y-%m-%d %H:%M:%S"),"sunset_time":sunset_time.strftime("%Y-%m-%d %H:%M:%S")}



# A response from the Api looks like this:
# {'coord': {'lon': -0.1257, 'lat': 51.5085}, 'weather': [{'id': 803, 'main': 'Clouds', 'description': 'broken clouds', 'icon': '04d'}], 'base': 'stations', 'main': {'temp': 291.73, 'feels_like': 291.34, 'temp_min': 290.07, 'temp_max': 293.23, 'pressure': 1024, 'humidity': 65}, 'visibility': 10000, 'wind': {'speed': 6.17, 'deg': 230}, 'clouds': {'all': 75}, 'dt': 1696514822, 'sys': {'type': 2, 'id': 2006068, 'country': 'GB', 'sunrise': 1696486010, 'sunset': 1696527061}, 'timezone': 3600, 'id': 2643743, 'name': 'London', 'cod': 200}

# docker cp kafka_producer.py kafka-container:/producer.py
# docker cp weather_api.py kafka-container:/weather_api.py
# docker cp Weather_api_key.txt kafka-container:/Weather_api_key.txt