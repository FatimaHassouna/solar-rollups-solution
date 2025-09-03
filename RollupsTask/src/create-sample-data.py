from pymongo import MongoClient
from datetime import datetime

# 1. connect to the DB


client = MongoClient('mongodb://localhost:27017/')

db = client.mydb
collection = db.solar_readings

# 2. Create the sample data for TEST_SYSTEM_3


# Data for August 29th
aug29_data = [
    {
        "systemID": "TEST_SYSTEM_3",
        "total_24h_forecast_kwh": 30.0,
        "result": {
            "todays_solar_summary": {
                "daily_production_kwh": 8.0,
                "daily_consumption_kwh": 85.0
            }
        },
        "createdAt": datetime(2025, 8, 29, 10, 0, 0)
    },
    {
        "systemID": "TEST_SYSTEM_3",
        "total_24h_forecast_kwh": 31.5,
        "result": {
            "todays_solar_summary": {
                "daily_production_kwh": 9.5,
                "daily_consumption_kwh": 88.0
            }
        },
        "createdAt": datetime(2025, 8, 29, 15, 0, 0) 
    },
    {
        "systemID": "TEST_SYSTEM_3",
        "total_24h_forecast_kwh": 32.0, 
        "result": {
            "todays_solar_summary": {
                "daily_production_kwh": 10.0, 
                "daily_consumption_kwh": 90.0  
            }
        },
        "createdAt": datetime(2025, 8, 29, 23, 59, 0) 
    }
]

# Data for August 30th
aug30_data = [
    {
        "systemID": "TEST_SYSTEM_3",
        "total_24h_forecast_kwh": 25.0,
        "result": {
            "todays_solar_summary": {
                "daily_production_kwh": 9.0,
                "daily_consumption_kwh": 95.0
            }
        },
        "createdAt": datetime(2025, 8, 30, 11, 0, 0)
    },
    {
        "systemID": "TEST_SYSTEM_3",
        "total_24h_forecast_kwh": 28.0, 
        "result": {
            "todays_solar_summary": {
                "daily_production_kwh": 12.0, 
                "daily_consumption_kwh": 100.0 
            }
        },
        "createdAt": datetime(2025, 8, 30, 23, 59, 0) 
    }
]

# Data for August 31st
aug31_data = [
    {
        "systemID": "TEST_SYSTEM_3",
        "total_24h_forecast_kwh": 34.0, 
        "result": {
            "todays_solar_summary": {
                "daily_production_kwh": 0.0,  
                "daily_consumption_kwh": 120.0 
            }
        },
        "createdAt": datetime(2025, 8, 31, 23, 59, 0) 
    }
]

# 4. Combine all the data and insert it
all_data = aug29_data + aug30_data + aug31_data
result = collection.insert_many(all_data)

client.close()


