# Store Sales Forecasting Model Decay Simulation

Welcome to the Store Sales Forecasting Model Decay Simulation Project! 

![img](https://i.imgur.com/7RGYPhE.png)

This project's primary objectives are to simulate incoming monthly data, detect data drift, and trigger model retraining. 

To simulate incoming daily data, the project uses a schedule that triggers the loading of data partitioned monthly every other minute or what is specified in the crons schedule. The data is then merged with static data (unpartitioned). After that, the model will be trained if there is no model yet to measure reports. If there is a model, the new data will be used to measure the model performance, data drift, and model drift. Depending on the outcome of the report, the project will trigger the retraining process.


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