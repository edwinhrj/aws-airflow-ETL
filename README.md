# weatherAPI-airflow

<img width="1752" height="714" alt="image" src="https://github.com/user-attachments/assets/44633f09-84ff-4211-95a2-fbbdac471abd" />

Note:
This github contains all the essential files required to run the end-to-end ETL pipeline. However, the true python files are hosted on an AWS EC2 instance now, which requires SSH access to connect and run the Airflow services remotely.

🌦️ Weather Data ETL Pipeline with Apache Airflow on AWS

- Experimented and built an automated ETL pipeline that extracts live weather data from the OpenWeatherMap API, transforms it, and loads it into an Amazon S3 bucket using Apache Airflow.

The entire end-to-end workflow is orchestrated on an AWS EC2 instance.

🚀 Project Overview
- **Extract:** Collect current weather data from OpenWeatherMap API.  
- **Transform:** Process and clean the raw JSON response into a structured format.  
- **Load:** Store the processed data into an Amazon S3 bucket for further use (analytics, machine learning, reporting, etc.).  
- **Orchestrate:** Use Apache Airflow to sc


🛠️ Tech Stack
- **Apache Airflow** – Workflow orchestration & scheduling  
- **AWS (S3, EC2, IAM)** – Cloud infrastructure & storage  
- **Python** – Data extraction & transformation logic  
- **OpenWeatherMap API** – Data source

