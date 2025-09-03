from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import defaultdict
def calculate_rollups(system_id):

        
    # connect to MongoClient

    client = MongoClient('mongodb://localhost:27017/')

    db = client.mydb
    solar_readings = db.solar_readings
    solar_rollups = db.solar_rollups

    # Aggregation pipline to get the last record for each day

    pipeline = [
        # step 1 : get the documnets for specific systemID

        {"$match" : {"systemID": system_id}},

        #step 2: add new field (date_only) without time

        {"$addFields" : {
            "date_only" :{
                "$dateToString" :{
                    "format": "%Y-%m-%d",
                    "date" : "$createdAt"
                }
            }
        } },

        #step 3: Sort all documents by createdAt descending (newest first)


        {"$sort": {"createdAt" : -1}},

        #step 4 : Group by date-only and take the first documnet in each group 

        {"$group" : {
            "_id": "$date_only",
            "last_document" : {"$first": "$$ROOT"}  #for each group we keep the first doc 
        }},

        #step 5 : Replace the root with the last doc so we have clean doc
        {"$replaceRoot" : {"newRoot" : "$last_document"}},

        #step 6: Sort the final results by date ascending
         {"$sort": {"createdAt": 1}}

    ]

    # Execute the aggregation
    daily_docs = list(solar_readings.aggregate(pipeline))
    print(f"   Found {len(daily_docs)} days of data.")
    
   
   # now we need to prepare the data structure of the rollups

    daily_rollups = []
    weekly_totals = defaultdict(lambda: {'forecast': 0, 'production': 0, 'consumption': 0})
    monthly_totals = defaultdict(lambda: {'forecast': 0, 'production': 0, 'consumption': 0})
    yearly_totals = defaultdict(lambda: {'forecast': 0, 'production': 0, 'consumption': 0})
    for doc in daily_docs:
        date = doc['createdAt']
        date_str = date.strftime('%Y-%m-%d')
        # Extract metrics from the document
        forecast = doc['total_24h_forecast_kwh']
        production = doc['result']['todays_solar_summary']['daily_production_kwh']
        consumption = doc['result']['todays_solar_summary']['daily_consumption_kwh']
        
        print(f"{date_str}: Forecast={forecast}, Production={production}, Consumption={consumption}")
        daily_rollup = {
             "systemID": system_id,
            "granularity": "day",
            "period_start": date_str,
            "period_end": date_str,
            "metrics": {
                "forecast": forecast,
                "production": production,
                "consumption": consumption
            },
            "created_at": datetime.now()
        }
        daily_rollups.append(daily_rollup)

        # add the weekly totals
        year, week_num, _ = date.isocalendar()
        week_key = f"{year}-W{week_num:02d}"
        weekly_totals[week_key]['forecast'] += forecast
        weekly_totals[week_key]['production'] += production
        weekly_totals[week_key]['consumption'] += consumption
       #  Add to Monthly Totals (format: YYYY-MM)
        month_key = date.strftime('%Y-%m')
        monthly_totals[month_key]['forecast'] += forecast
        monthly_totals[month_key]['production'] += production
        monthly_totals[month_key]['consumption'] += consumption
        #  Add to Yearly Totals (format: YYYY)
        year_key = str(date.year)
        yearly_totals[year_key]['forecast'] += forecast
        yearly_totals[year_key]['production'] += production
        yearly_totals[year_key]['consumption'] += consumption

    all_rollups =[]

    all_rollups.extend(daily_rollups)
    
    # Add weekly rollups
    for week_key, totals in weekly_totals.items():
        year, week_num = map(int, week_key.split('-W'))
        # Calculate start and end dates for the week
        week_start = datetime.strptime(f"{year}-W{week_num}-1", "%Y-W%W-%w")
        week_end = week_start + timedelta(days=6)
        
        weekly_rollup = {
            "systemID": system_id,
            "granularity": "week",
            "period_start": week_start.strftime('%Y-%m-%d'),
            "period_end": week_end.strftime('%Y-%m-%d'),
            "metrics": totals,
            "created_at": datetime.now()
        }
        all_rollups.append(weekly_rollup)
        print(f"Week {week_key}: Forecast={totals['forecast']}, Production={totals['production']}, Consumption={totals['consumption']}")
    
    # Add monthly rollups
    for month_key, totals in monthly_totals.items():
        year, month = map(int, month_key.split('-'))
        # Calculate start and end dates for the month
        month_start = datetime(year, month, 1)
        if month == 12:
            month_end = datetime(year, month, 31)
        else:
            month_end = datetime(year, month + 1, 1) - timedelta(days=1)
        
        monthly_rollup = {
            "systemID": system_id,
            "granularity": "month",
            "period_start": month_start.strftime('%Y-%m-%d'),
            "period_end": month_end.strftime('%Y-%m-%d'),
            "metrics": totals,
            "created_at": datetime.now()
        }
        all_rollups.append(monthly_rollup)
        print(f" Month {month_key}: Forecast={totals['forecast']}, Production={totals['production']}, Consumption={totals['consumption']}")
    
    # Add yearly rollups
    for year_key, totals in yearly_totals.items():
        yearly_rollup = {
            "systemID": system_id,
            "granularity": "year",
            "period_start": f"{year_key}-01-01",
            "period_end": f"{year_key}-12-31",
            "metrics": totals,
            "created_at": datetime.now()
        }
        all_rollups.append(yearly_rollup)
        print(f"Year {year_key}: Forecast={totals['forecast']}, Production={totals['production']}, Consumption={totals['consumption']}")
    
    # Save all rollups to the database
    if all_rollups:
        solar_rollups.insert_many(all_rollups)
        print(f"Saved {len(all_rollups)} rollup documents to the database!")
    else:
        print("No rollups to save.")
    
    client.close()
    return all_rollups


if __name__ == "__main__":
    #  testing  our sample system
    system_to_process = "TEST_SYSTEM_3"  
    calculate_rollups(system_to_process)
    print("Rollup calculation complete!")


        



