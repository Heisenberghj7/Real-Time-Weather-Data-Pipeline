import datetime as dt
import requests

BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"  # Note the corrected endpoint version (2.5)
API_KEY = open('Weather_api_key.txt', 'r').read().strip()  # Make sure to strip any whitespace
CITY = "London"

def kelvin_to_celsius(kelvin):
    ceslsius= kelvin-273.15
    return ceslsius


url = f"{BASE_URL}q={CITY}&appid={API_KEY}"  # Use f-strings for string formatting

response = requests.get(url)

if response.status_code == 200:
    data = response.json()  # Parse the JSON response
    print(data)
else:
    print(f"Error: {response.status_code}")

    
"""
A response from the Api looks like this:
{'coord': {'lon': -0.1257, 'lat': 51.5085}, 'weather': [{'id': 803, 'main': 'Clouds', 'description': 'broken clouds', 'icon': '04d'}], 'base': 'stations', 'main': {'temp': 291.73, 'feels_like': 291.34, 'temp_min': 290.07, 'temp_max': 293.23, 'pressure': 1024, 'humidity': 65}, 'visibility': 10000, 'wind': {'speed': 6.17, 'deg': 230}, 'clouds': {'all': 75}, 'dt': 1696514822, 'sys': {'type': 2, 'id': 2006068, 'country': 'GB', 'sunrise': 1696486010, 'sunset': 1696527061}, 'timezone': 3600, 'id': 2643743, 'name': 'London', 'cod': 200}

"""
temp_celsius=kelvin_to_celsius(response['main']['temp'])
feels_like_celsius= kelvin_to_celsius(response['main']['feels_like'])
wind_speed=response['wind']['speed']
humidity=response['main']['humidity']
desciption= response['weather'][0]['description']
sunrise_time=dt.datetime.utcfromtimestamp(response['sys']['sunrise'] + response['timezone'])
sunset_time=dt.datetime.utcfromtimestamp(response['sys']['sunset'] + response['timezone'])
