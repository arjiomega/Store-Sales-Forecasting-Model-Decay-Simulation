from setuptools import find_packages, setup

setup(
    name="Store_Sales_Forecasting_Model_Decay_Simulation",
    packages=find_packages(
        exclude=["Store_Sales_Forecasting_Model_Decay_Simulation_tests"]
    ),
    install_requires=[
        "dagster==1.6.7",
        "dagster-cloud==1.6.7",
        "pandas==2.2.1",
        "SQLAlchemy==2.0.27",
        "psycopg2-binary==2.9.9",
    ],
    extras_require={
        "dev": ["dagster-webserver==1.6.7", "pytest==8.0.2", "black==24.2.0"]
    },
)
