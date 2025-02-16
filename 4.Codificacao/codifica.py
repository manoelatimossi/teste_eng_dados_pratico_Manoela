import requests
import mysql.connector
from datetime import datetime

API_KEY = '0f7fb79e6bf48f664e6adc0f01ff0a49'
CITY = 'São Paulo'

config_db = {
    'host': 'xxx.xxx.x.xxx', #Aqui eu utilizei o meu ip para conectar
    'user': 'Manoela',
    'password': '1234',
    'database': 'itau'
}
try:
    conn = mysql.connector.connect(**config_db)
    cursor = conn.cursor()
    print("Conexão com o MySQL estabelecida com sucesso!")
except mysql.connector.Error as err:
    print(f"Erro ao conectar ao MySQL: {err}")
    exit()

def get_weather_data(city: str, api_key: str):
    url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric'
    print(f"Requisitando dados para a cidade: {city} com a URL: {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
       
        return response.json()  
    else:
        raise Exception(f"Erro na requisição: {response.status_code}")

    # como estou usando o mysql e não tem a possibilidade de usar struct, estou fazendo fazendo essa estruturação em formato flat para armazenar os dados. 
def store_weather_data(data):
    if data and 'list' in data:
        for entry in data['list']:
            dt = entry['dt']
            temp = entry['main']['temp']
            feels_like = entry['main']['feels_like']
            temp_min = entry['main']['temp_min']
            temp_max = entry['main']['temp_max']
            pressure = entry['main']['pressure']
            sea_level = entry['main'].get('sea_level', None) 
            grnd_level = entry['main'].get('grnd_level', None)  
            humidity = entry['main']['humidity']
            temp_kf = entry['main'].get('temp_kf', None) 
            weather_id = entry['weather'][0]['id']
            weather_main = entry['weather'][0]['main']
            weather_description = entry['weather'][0]['description']
            weather_icon = entry['weather'][0]['icon']
            clouds_all = entry['clouds']['all']
            wind_speed = entry['wind']['speed']
            wind_deg = entry['wind']['deg']
            wind_gust = entry['wind'].get('gust', None)  
            visibility = entry.get('visibility', None) 
            pop = entry.get('pop', 0)  
            sys_pod = entry['sys']['pod']
            dt_txt = entry['dt_txt']

            cursor.execute('''
            INSERT INTO itau.weather_data (
                dt, temp, feels_like, temp_min, temp_max, pressure, sea_level, grnd_level,
                humidity, temp_kf, weather_id, weather_main, weather_description, weather_icon,
                clouds_all, wind_speed, wind_deg, wind_gust, visibility, pop, sys_pod, dt_txt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                dt, temp, feels_like, temp_min, temp_max, pressure, sea_level, grnd_level,
                humidity, temp_kf, weather_id, weather_main, weather_description, weather_icon,
                clouds_all, wind_speed, wind_deg, wind_gust, visibility, pop, sys_pod, dt_txt
            ))

        conn.commit()
        print(f"Dados armazenados com sucesso para {len(data['list'])} entradas!")
    else:
        print("Nenhum dado para armazenar.")

weather_data = get_weather_data(CITY, API_KEY)
store_weather_data(weather_data)