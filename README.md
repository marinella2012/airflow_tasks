# airflow_tasks

Create DAG tasks:
1. For calculating the arithmetic average ticket price (Fare) for each class (Pclass) and compares the resulting dataframe to the titanic_mean_fares.csv file.
2. Add a step called last_task, which displays a line in STDOUT that informs about the end of the calculation and displays the execution date in the YYYY-MM-DD format. Example line: "Pipeline finished! Execution date is 2020-12-28"
3. Create a DAG file that calls the API every hour, receives all the information on the weather in your city and writes it to the json file (extract_data), then extracts only the values of interest, the observation date, temperature, humidity and writes it to the json file (transform_data) and transfers it to load_data data obtained using XCom, configure the Airflow connection to Postgres. In Postgres, create the weather_worldweatheronline table and write the data received in load_data into it.
