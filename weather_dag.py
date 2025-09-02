from airflow import DAG
from datetime import timedelta, datetime, timezone
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import json
import pandas as pd

# intermediate fuinction
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

# transform and load data function
def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'], tz=timezone.utc)
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'], tz=timezone.utc)
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'], tz=timezone.utc)

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": "ASIAZ6GS3TQ3KWMV52X6", 
    "secret": "5nRkjDuIX2rc2FA7E7qXCFcu6LFJ7Cz6MWPUYmJ9", 
    "token": "FwoGZXIvYXdzENr//////////wEaDPHKoovUb23SRpGBLCJqSLwnOfEYIsgZ2BE2Re+yKW81NpUq7TXNgj5vgzcDZwo1HT891ITZ9+cNsXk2Qyh/+wz2IktENC09sTjUQrolvrpBoEg3nFweIKmLaYjdzxn67e9y9ZtpeYZim7sfkyg0U9MlAit88MUB2SiRt9zFBjIoRjqigZeEC4GhkMdz3Xf/daWOpzOj7e3fICe2bI+M54vw2jAATKXTmg=="}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"{dt_string}.csv", index=False)
    df_data.to_csv(f"s3://weatherapi-airflow/{dt_string}.csv", index=False, storage_options=aws_credentials)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 2),
    'email': ['12edwinho@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('weather_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False) as dag: 

    # http sensorchecks calls the API endpoint and 
    # checks if the weather API is ready to be used.
    # this will be the first task of the DAG
    is_weather_api_ready = HttpSensor( 
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&appid=3b867a73945ffebf5933f7fa7f9842b3'
    )

    # http operator calls the API endpoint and 
    # extracts the weather data from the API
    # this will be the second task of the DAG
    extract_weather_data = HttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&appid=3b867a73945ffebf5933f7fa7f9842b3',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    # python operator will allow us to use python code 
    # in the transform and load task, this will point to 
    # the function transform_load_data we created all
    # the way at the top
    transform_load_data_task = PythonOperator(
        task_id='transform_load_data',
        python_callable=transform_load_data
    )

    # define direction of workflow
    is_weather_api_ready >> extract_weather_data >> transform_load_data_task