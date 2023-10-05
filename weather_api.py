import datetime as dt
import requests

def kelvin_to_celsius(kelvin):
    ceslsius= kelvin-273.15
    return ceslsius

def get_data():
    BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"  
    API_KEY = open('Weather_api_key.txt', 'r').read().strip() 
    CITY = "London"

    url = f"{BASE_URL}q={CITY}&appid={API_KEY}" 
    response = requests.get(url)
    if response.status_code == 200:
       data = response.json()  # Parse the JSON response
       print(data)
    else:
       print(f"Error: {response.status_code}")

    temp_celsius=kelvin_to_celsius(response['main']['temp'])
    feels_like_celsius= kelvin_to_celsius(response['main']['feels_like'])
    wind_speed=response['wind']['speed']
    humidity=response['main']['humidity']
    desciption= response['weather'][0]['description']
    sunrise_time=dt.datetime.utcfromtimestamp(response['sys']['sunrise'] + response['timezone'])
    sunset_time=dt.datetime.utcfromtimestamp(response['sys']['sunset'] + response['timezone'])

    return {"city_name":CITY,"temp_celsius":temp_celsius,"feels_like_celsius":feels_like_celsius,"wind_speed":wind_speed,"humidity":humidity,"desciption":desciption,"sunrise_time":sunrise_time,"sunset_time":sunset_time}
    
# """
# A response from the Api looks like this:
# {'coord': {'lon': -0.1257, 'lat': 51.5085}, 'weather': [{'id': 803, 'main': 'Clouds', 'description': 'broken clouds', 'icon': '04d'}], 'base': 'stations', 'main': {'temp': 291.73, 'feels_like': 291.34, 'temp_min': 290.07, 'temp_max': 293.23, 'pressure': 1024, 'humidity': 65}, 'visibility': 10000, 'wind': {'speed': 6.17, 'deg': 230}, 'clouds': {'all': 75}, 'dt': 1696514822, 'sys': {'type': 2, 'id': 2006068, 'country': 'GB', 'sunrise': 1696486010, 'sunset': 1696527061}, 'timezone': 3600, 'id': 2643743, 'name': 'London', 'cod': 200}

# """
# docker cp kafka_producer.py kafka-container:/producer.py
# docker cp weather_api.py kafka-container:/weather_api.py
# docker cp Weather_api_key.txt kafka-container:/Weather_api_key.txt

# docker cp kafka_consumer.py (41b5cd501b17345d1c2e8c8cf598beaeb919ef4e91310498dcb17838935c8eb4):/opt/spark/consumer.py