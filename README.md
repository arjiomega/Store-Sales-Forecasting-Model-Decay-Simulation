# Store Sales Forecasting Model Decay Simulation

Welcome to the Store Sales Forecasting Model Decay Simulation Project! 

**Objectives**
1. **Simulate incoming daily data using dagster**
    - Implement schedule that triggers the loading of data partition (daily/weekly/etc.) every other minute or what is specified in the crons schedule.
2. **Detect Data Drift**
    - Use evidently ai to detect data drift.
3. **Model Retraining**
    - Implement sensors that are activated upon the detection of data drift and/or model drift. These sensors trigger the retraining process, ensuring that the forecasting model adapts to the evolving patterns in the data. The automated retraining strategy helps maintain the model's accuracy over time.


## Setup

Install the project including the dependencies:
```bash
pip install -e ".[dev]"
```

Prepare environment variables by creating .env file:
```env
DAGSTER_HOME=<path to project>
SNOWFLAKE_USER=<snowflake user>
SNOWFLAKE_PASSWORD=<snowflake password>
SNOWFLAKE_ACCOUNT=<snowflake account>
```
> For the snowflake credentials, follow the instructions here: [dagster snowflake integration guide](https://docs.dagster.io/integrations/snowflake/using-snowflake-with-dagster)

Start Dagster UI Server
```bash
dagster dev
```
Open http://localhost:3000 with your browser to see the project.


## Data
Data is currently not located anywhere in the repository. The data can be found at:

https://www.kaggle.com/competitions/store-sales-time-series-forecasting/data

**NOTE**:
>Create a `data` folder in the project root directory and create another folder called `raw` inside and place the downloaded data there.